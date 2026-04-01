/**
 * MODULO DE CLAUDE API - DefiOracle
 * Reemplaza openai-helper.js usando Anthropic Claude API
 *
 * Usa @anthropic-ai/sdk para:
 * - Chatbot con tool_use (equivalente a function calling)
 * - Generacion de mensajes personalizados para tareas
 * - Cache, rate limiting, retry y estadisticas
 */

const Anthropic = require('@anthropic-ai/sdk');

// Configuracion de costos por modelo (USD por 1M tokens)
const MODEL_COSTS = {
    'claude-sonnet-4-20250514': { input: 3.00, output: 15.00 },
    'claude-haiku-4-20250414': { input: 0.80, output: 4.00 }
};

// Modelo por defecto - Haiku es rapido y barato para chatbot
const DEFAULT_MODEL = 'claude-haiku-4-20250414';

// Cache simple para respuestas (evita llamadas duplicadas)
const responseCache = new Map();
const CACHE_TTL = 30 * 60 * 1000; // 30 minutos

// Contador de requests para rate limiting
let requestCount = 0;
let lastResetTime = Date.now();
const MAX_REQUESTS_PER_MINUTE = 50;

// Estadisticas de uso
let stats = {
    totalRequests: 0,
    successfulRequests: 0,
    failedRequests: 0,
    totalInputTokens: 0,
    totalOutputTokens: 0,
    totalCostUSD: 0,
    cacheHits: 0
};

/**
 * Genera un hash simple para cachear queries
 */
function generateCacheKey(model, messages, options = {}) {
    const key = JSON.stringify({ model, messages, ...options });
    return require('crypto').createHash('md5').update(key).digest('hex');
}

/**
 * Verifica rate limiting local
 */
function checkRateLimit() {
    const now = Date.now();
    const timeSinceReset = now - lastResetTime;

    if (timeSinceReset > 60000) {
        requestCount = 0;
        lastResetTime = now;
    }

    if (requestCount >= MAX_REQUESTS_PER_MINUTE) {
        const waitTime = 60000 - timeSinceReset;
        return { allowed: false, waitTime };
    }

    requestCount++;
    return { allowed: true, waitTime: 0 };
}

/**
 * Calcula costo de la llamada
 */
function calculateCost(model, inputTokens, outputTokens) {
    const costs = MODEL_COSTS[model] || MODEL_COSTS[DEFAULT_MODEL];
    const inputCost = (inputTokens / 1000000) * costs.input;
    const outputCost = (outputTokens / 1000000) * costs.output;
    return inputCost + outputCost;
}

/**
 * Inicializa el cliente Anthropic
 */
function getClient() {
    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) {
        throw new Error('ANTHROPIC_API_KEY no configurada');
    }
    return new Anthropic({ apiKey });
}

/**
 * Funcion principal para llamar a Claude API
 * con retry, cache, rate limiting y monitoreo
 */
async function callClaude(options = {}) {
    const {
        model = DEFAULT_MODEL,
        system = '',
        messages,
        maxTokens = 500,
        temperature = 0.8,
        tools = null,
        useCache = true,
        maxRetries = 3,
        retryDelay = 1000
    } = options;

    // Verificar rate limiting
    const rateLimitCheck = checkRateLimit();
    if (!rateLimitCheck.allowed) {
        console.warn(`⚠️ Rate limit alcanzado. Esperando ${rateLimitCheck.waitTime}ms...`);
        await new Promise(resolve => setTimeout(resolve, rateLimitCheck.waitTime));
    }

    // Verificar cache
    const cacheKey = useCache ? generateCacheKey(model, messages, { maxTokens, temperature, system }) : null;
    if (useCache && cacheKey && responseCache.has(cacheKey)) {
        const cached = responseCache.get(cacheKey);
        if (Date.now() - cached.timestamp < CACHE_TTL) {
            stats.cacheHits++;
            console.log('✅ Respuesta obtenida desde cache');
            return cached.data;
        } else {
            responseCache.delete(cacheKey);
        }
    }

    console.log(`📊 Request Claude: ${model}, max ${maxTokens} tokens salida`);

    // Retry logic con exponential backoff
    let lastError;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            stats.totalRequests++;

            const client = getClient();

            const requestParams = {
                model,
                max_tokens: maxTokens,
                temperature,
                messages
            };

            if (system) {
                requestParams.system = system;
            }

            if (tools && tools.length > 0) {
                requestParams.tools = tools;
            }

            const response = await client.messages.create(requestParams);

            // Extraer datos de uso
            const inputTokens = response.usage?.input_tokens || 0;
            const outputTokens = response.usage?.output_tokens || 0;
            const cost = calculateCost(model, inputTokens, outputTokens);

            // Actualizar estadisticas
            stats.successfulRequests++;
            stats.totalInputTokens += inputTokens;
            stats.totalOutputTokens += outputTokens;
            stats.totalCostUSD += cost;

            console.log(`✅ Claude OK: ${inputTokens} in + ${outputTokens} out tokens | Costo: $${cost.toFixed(6)} | Total acumulado: $${stats.totalCostUSD.toFixed(4)}`);

            // Extraer texto y tool_use del response
            const textContent = response.content.find(c => c.type === 'text');
            const toolUseContent = response.content.find(c => c.type === 'tool_use');

            const result = {
                content: response.content,
                text: textContent ? textContent.text : '',
                toolUse: toolUseContent || null,
                stopReason: response.stop_reason,
                usage: { inputTokens, outputTokens, cost },
                model
            };

            // Guardar en cache
            if (useCache && cacheKey) {
                responseCache.set(cacheKey, {
                    data: result,
                    timestamp: Date.now()
                });

                if (responseCache.size > 100) {
                    const oldestKey = responseCache.keys().next().value;
                    responseCache.delete(oldestKey);
                }
            }

            return result;

        } catch (error) {
            lastError = error;
            stats.failedRequests++;

            const statusCode = error.status;
            const errorMessage = error.message;

            console.error(`❌ Error Claude (intento ${attempt}/${maxRetries}):`, {
                status: statusCode,
                message: errorMessage
            });

            // No reintentar en errores no recuperables
            if (statusCode === 401 || statusCode === 403) {
                console.error('🚫 Error de autenticacion, abortando reintentos');
                break;
            }

            // Esperar antes de reintentar (exponential backoff)
            if (attempt < maxRetries) {
                const delay = retryDelay * Math.pow(2, attempt - 1);
                console.log(`⏳ Reintentando en ${delay}ms...`);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
    }

    throw lastError;
}

/**
 * Wrapper para chatbot con tool_use (equivalente a function calling de OpenAI)
 *
 * Convierte agentFunctions de formato OpenAI a formato Claude tools
 */
async function chatbotWithTools(system, messages, tools) {
    try {
        const result = await callClaude({
            model: DEFAULT_MODEL,
            system,
            messages,
            maxTokens: 500,
            temperature: 0.8,
            tools,
            useCache: false
        });

        return {
            success: true,
            content: result.content,
            text: result.text,
            toolUse: result.toolUse,
            stopReason: result.stopReason,
            usage: result.usage
        };

    } catch (error) {
        return {
            success: false,
            error: error.message,
            statusCode: error.status
        };
    }
}

/**
 * Llamada de continuacion despues de ejecutar un tool
 */
async function chatbotContinue(system, messages) {
    try {
        const result = await callClaude({
            model: DEFAULT_MODEL,
            system,
            messages,
            maxTokens: 500,
            temperature: 0.8,
            useCache: false
        });

        return {
            success: true,
            text: result.text,
            usage: result.usage
        };

    } catch (error) {
        return {
            success: false,
            error: error.message
        };
    }
}

/**
 * Wrapper especifico para generacion de mensajes de tareas
 */
async function generateTaskMessage(params) {
    const {
        nombreCliente,
        diasInactivo,
        tasaPromocional,
        tipoEstrategia
    } = params;

    let promptMensaje = '';

    if (tipoEstrategia === 'inactivo_recordatorio') {
        promptMensaje = `Genera un mensaje de WhatsApp para ${nombreCliente} que lleva ${diasInactivo} días sin enviar dinero. INICIA con saludo personalizado (Hola ${nombreCliente}). Tono: amable recordatorio. NO uses placeholders. Emojis apropiados (2-3 max). Maximo 4 lineas. Se CREATIVO y unico. Lenguaje humano y natural.`;

    } else if (tipoEstrategia === 'inactivo_promocion') {
        promptMensaje = `Genera un mensaje de WhatsApp para ${nombreCliente} que lleva ${diasInactivo} días sin enviar dinero a Venezuela. INICIA con saludo personalizado (Hola ${nombreCliente}). Ofrece tasa promocional: ${tasaPromocional.toFixed(3)} VES/CLP, valida SOLO HOY hasta las 21:00. Tono cercano. NO uses placeholders. Emojis (2-3 max). Maximo 5 lineas. CREATIVO y unico. ANTI-SPAM: conversacional, NO promocional agresivo.`;

    } else if (tipoEstrategia === 'critico_reactivacion') {
        promptMensaje = `Genera un mensaje de WhatsApp para ${nombreCliente} que lleva ${diasInactivo} días sin enviar dinero. INICIA con saludo personalizado (Hola ${nombreCliente}). Ofrece tasa ESPECIAL de reactivacion: ${tasaPromocional.toFixed(3)} VES/CLP, valida SOLO HOY hasta las 21:00. Tono urgente pero calido. NO menciones "perdidas" ni "riesgos". NO placeholders. Emojis (max 3). Maximo 6 lineas. MUY CREATIVO. ANTI-SPAM: urgencia SIN agresividad comercial.`;

    } else if (tipoEstrategia === 'reduccion_actividad') {
        promptMensaje = `Genera un mensaje para ${nombreCliente} que antes enviaba dinero con mas frecuencia pero ahora no tanto. INICIA con saludo personalizado (Hola ${nombreCliente}). Tono: preocupacion genuina, pregunta si todo esta bien. Ofrece tasa EXCLUSIVA: ${tasaPromocional.toFixed(3)} VES/CLP, valida SOLO HOY hasta las 21:00. Lenguaje cercano y familiar. NO placeholders. Emojis (2-3 max). Maximo 5 lineas. CREATIVO. Tono empatico y humano.`;
    }

    try {
        const result = await callClaude({
            model: DEFAULT_MODEL,
            system: 'Eres DefiOracle, empresa chilena de remesas que ayuda a enviar dinero desde Chile hacia Venezuela usando USDT como puente. Genera mensajes directos, calidos y profesionales en español para WhatsApp. NUNCA uses placeholders como [Tu Nombre], [Tu Empresa], [Firma] - el mensaje ya es de DefiOracle. Usa emojis con moderacion (2-3 maximo). Enfoque: remesas familiares, no inversiones. ANTI-SPAM: Escribe como humano real, NO como bot. Evita mayusculas excesivas y lenguaje corporativo. PRIVACIDAD: NO menciones situaciones personales/familiares del cliente. Solo usar: "enviar dinero a Venezuela" o "hacer un envio".',
            messages: [
                {
                    role: 'user',
                    content: promptMensaje
                }
            ],
            maxTokens: 200,
            temperature: 0.9,
            useCache: false
        });

        return {
            success: true,
            message: result.text.trim(),
            usage: result.usage
        };

    } catch (error) {
        console.error('Error generando mensaje con Claude:', error.message);

        // Fallback a mensajes predeterminados
        let mensajeFallback = '';

        if (tipoEstrategia === 'inactivo_recordatorio') {
            mensajeFallback = `Hola ${nombreCliente}! 😊\n\nHace tiempo que no te vemos por aquí. ¿Todo bien? 😊\n\nEstamos disponibles 08:00-21:00 todos los días para tus operaciones. 📱\n\n¡Esperamos verte pronto!`;
        } else {
            mensajeFallback = `Hola ${nombreCliente}! 😊\n\nTenemos una tasa especial para ti: ${tasaPromocional ? tasaPromocional.toFixed(3) : ''} VES/CLP 💰\n\n¡Contáctanos! Disponibles 08:00-21:00 📱`;
        }

        return {
            success: false,
            message: mensajeFallback,
            error: error.message
        };
    }
}

/**
 * Convierte agentFunctions de formato OpenAI a formato Claude tools
 */
function convertOpenAIFunctionsToClaude(openAIFunctions) {
    return openAIFunctions.map(fn => ({
        name: fn.name,
        description: fn.description,
        input_schema: {
            type: 'object',
            properties: fn.parameters.properties || {},
            required: fn.parameters.required || []
        }
    }));
}

/**
 * Obtener estadisticas de uso
 */
function getStats() {
    return {
        ...stats,
        provider: 'Claude (Anthropic)',
        model: DEFAULT_MODEL,
        averageCostPerRequest: stats.successfulRequests > 0
            ? stats.totalCostUSD / stats.successfulRequests
            : 0,
        successRate: stats.totalRequests > 0
            ? (stats.successfulRequests / stats.totalRequests * 100).toFixed(2) + '%'
            : '0%',
        cacheHitRate: stats.totalRequests > 0
            ? (stats.cacheHits / stats.totalRequests * 100).toFixed(2) + '%'
            : '0%'
    };
}

/**
 * Resetear estadisticas
 */
function resetStats() {
    stats = {
        totalRequests: 0,
        successfulRequests: 0,
        failedRequests: 0,
        totalInputTokens: 0,
        totalOutputTokens: 0,
        totalCostUSD: 0,
        cacheHits: 0
    };
}

/**
 * Limpiar cache manualmente
 */
function clearCache() {
    const size = responseCache.size;
    responseCache.clear();
    console.log(`🗑️ Cache limpiado: ${size} entradas eliminadas`);
}

module.exports = {
    callClaude,
    chatbotWithTools,
    chatbotContinue,
    generateTaskMessage,
    convertOpenAIFunctionsToClaude,
    getStats,
    resetStats,
    clearCache
};
