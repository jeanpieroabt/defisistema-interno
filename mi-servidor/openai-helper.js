/**
 * MODULO OPTIMIZADO DE OPENAI - DefiOracle
 *
 * PROBLEMAS TECNICOS QUE PUEDE PRESENTAR LA IA:
 *
 * 1. ERRORES DE AUTENTICACION:
 *    - API Key invalida o expirada
 *    - API Key sin creditos/saldo insuficiente
 *    - API Key con permisos restringidos
 *
 * 2. ERRORES DE RATE LIMITING:
 *    - Demasiadas solicitudes por minuto (RPM)
 *    - Demasiados tokens por minuto (TPM)
 *    - Limite de solicitudes diarias excedido
 *
 * 3. ERRORES DE RED:
 *    - Timeout de conexion (red lenta)
 *    - Error 500/502/503 del servidor de OpenAI
 *    - Problemas de DNS o conectividad
 *
 * 4. ERRORES DE CONTENIDO:
 *    - Mensaje rechazado por filtros de contenido
 *    - Tokens exceden el maximo permitido
 *    - Formato invalido en function calling
 *
 * 5. PROBLEMAS DE COSTO:
 *    - Consumo excesivo de tokens sin control
 *    - Requests innecesarias duplicadas
 *    - Uso de modelos muy caros sin necesidad
 *
 * OPTIMIZACIONES IMPLEMENTADAS:
 * - Retry automatico con exponential backoff
 * - Cache de respuestas para queries repetidas
 * - Contador de tokens y costos
 * - Rate limiting local para evitar exceder limites
 * - Fallbacks automaticos cuando falla la IA
 * - Logging detallado de errores
 */

const axios = require('axios');

// Configuracion de costos por modelo (USD por 1M tokens)
const MODEL_COSTS = {
    'gpt-4o-mini': { input: 0.15, output: 0.60 },
    'gpt-3.5-turbo': { input: 0.50, output: 1.50 }
};

// Cache simple para respuestas (evita llamadas duplicadas)
const responseCache = new Map();
const CACHE_TTL = 30 * 60 * 1000; // 30 minutos

// Contador de requests para rate limiting
let requestCount = 0;
let lastResetTime = Date.now();
const MAX_REQUESTS_PER_MINUTE = 50; // Limite conservador

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

    // Reset contador cada minuto
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
 * Calcula tokens aproximados (estimacion simple)
 */
function estimateTokens(text) {
    // Regla aproximada: 1 token ~ 4 caracteres en español
    return Math.ceil(text.length / 4);
}

/**
 * Calcula costo de la llamada
 */
function calculateCost(model, inputTokens, outputTokens) {
    const costs = MODEL_COSTS[model] || MODEL_COSTS['gpt-4o-mini'];
    const inputCost = (inputTokens / 1000000) * costs.input;
    const outputCost = (outputTokens / 1000000) * costs.output;
    return inputCost + outputCost;
}

/**
 * Funcion principal optimizada para llamar a OpenAI
 * con retry, cache, rate limiting y monitoreo
 */
async function callOpenAI(options = {}) {
    const {
        model = 'gpt-4o-mini',
        messages,
        maxTokens = 200,
        temperature = 0.8,
        functions = null,
        functionCall = 'auto',
        useCache = true,
        maxRetries = 3,
        retryDelay = 1000
    } = options;

    // Validar API key
    const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
    if (!OPENAI_API_KEY) {
        throw new Error('OPENAI_API_KEY no configurada');
    }

    // Verificar rate limiting
    const rateLimitCheck = checkRateLimit();
    if (!rateLimitCheck.allowed) {
        console.warn(`⚠️ Rate limit alcanzado. Esperando ${rateLimitCheck.waitTime}ms...`);
        await new Promise(resolve => setTimeout(resolve, rateLimitCheck.waitTime));
    }

    // Verificar cache
    const cacheKey = useCache ? generateCacheKey(model, messages, { maxTokens, temperature }) : null;
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

    // Estimar tokens de entrada
    const inputText = messages.map(m => m.content || '').join(' ');
    const estimatedInputTokens = estimateTokens(inputText);

    console.log(`📊 Request OpenAI: ${model}, ~${estimatedInputTokens} tokens entrada, max ${maxTokens} salida`);

    // Retry logic con exponential backoff
    let lastError;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            stats.totalRequests++;

            const requestBody = {
                model,
                messages,
                max_tokens: maxTokens,
                temperature
            };

            if (functions) {
                requestBody.functions = functions;
                requestBody.function_call = functionCall;
            }

            const response = await axios.post(
                'https://api.openai.com/v1/chat/completions',
                requestBody,
                {
                    headers: {
                        'Authorization': `Bearer ${OPENAI_API_KEY}`,
                        'Content-Type': 'application/json'
                    },
                    timeout: 30000 // 30 segundos timeout
                }
            );

            // Extraer datos de uso
            const usage = response.data.usage || {};
            const inputTokens = usage.prompt_tokens || estimatedInputTokens;
            const outputTokens = usage.completion_tokens || estimateTokens(response.data.choices[0].message.content || '');
            const cost = calculateCost(model, inputTokens, outputTokens);

            // Actualizar estadisticas
            stats.successfulRequests++;
            stats.totalInputTokens += inputTokens;
            stats.totalOutputTokens += outputTokens;
            stats.totalCostUSD += cost;

            console.log(`✅ OpenAI OK: ${inputTokens} in + ${outputTokens} out tokens | Costo: $${cost.toFixed(6)} | Total acumulado: $${stats.totalCostUSD.toFixed(4)}`);

            const result = {
                message: response.data.choices[0].message,
                usage: { inputTokens, outputTokens, cost },
                model
            };

            // Guardar en cache
            if (useCache && cacheKey) {
                responseCache.set(cacheKey, {
                    data: result,
                    timestamp: Date.now()
                });

                // Limpiar cache viejo si crece mucho
                if (responseCache.size > 100) {
                    const oldestKey = responseCache.keys().next().value;
                    responseCache.delete(oldestKey);
                }
            }

            return result;

        } catch (error) {
            lastError = error;
            stats.failedRequests++;

            const errorCode = error.response?.data?.error?.code;
            const errorMessage = error.response?.data?.error?.message || error.message;
            const statusCode = error.response?.status;

            console.error(`❌ Error OpenAI (intento ${attempt}/${maxRetries}):`, {
                code: errorCode,
                status: statusCode,
                message: errorMessage
            });

            // No reintentar en ciertos errores
            if (errorCode === 'invalid_api_key' ||
                errorCode === 'insufficient_quota' ||
                statusCode === 401 ||
                statusCode === 403) {
                console.error('🚫 Error no recuperable, abortando reintentos');
                break;
            }

            // Esperar antes de reintentar (exponential backoff)
            if (attempt < maxRetries) {
                const delay = retryDelay * Math.pow(2, attempt - 1); // 1s, 2s, 4s
                console.log(`⏳ Reintentando en ${delay}ms...`);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
    }

    // Si todos los reintentos fallaron, lanzar el ultimo error
    throw lastError;
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
        promptMensaje = `Eres DefiOracle, empresa de remesas Chile-Venezuela. Genera un mensaje de WhatsApp para ${nombreCliente} que lleva ${diasInactivo} días sin enviar dinero. IMPORTANTE: INICIA el mensaje con un saludo personalizado usando el nombre del cliente (Hola ${nombreCliente}, Hola Juan, etc). Tono: amable recordatorio, menciona que lo extrañamos. NO uses placeholders como [Tu Nombre]. Mensaje directo de DefiOracle. Emojis apropiados (2-3 max). Horario: 08:00-21:00. Maximo 4 lineas. Se CREATIVO - cada mensaje debe ser unico. ANTI-SPAM: Lenguaje humano y natural.`;

    } else if (tipoEstrategia === 'inactivo_promocion') {
        promptMensaje = `Eres DefiOracle, empresa de remesas Chile-Venezuela. Genera un mensaje de WhatsApp para ${nombreCliente} que lleva ${diasInactivo} días sin enviar dinero a Venezuela. IMPORTANTE: INICIA el mensaje con un saludo personalizado usando el nombre del cliente (Hola ${nombreCliente}, Hola Juan, etc). Ofrece tasa promocional: ${tasaPromocional.toFixed(3)} VES/CLP, valida SOLO HOY hasta las 21:00. Tono: cercano, hazle saber que lo extrañamos. NO uses placeholders como [Tu Nombre]. Mensaje directo de DefiOracle. Emojis apropiados (2-3 max). Horario: 08:00-21:00. Maximo 5 lineas. IMPORTANTE: Se CREATIVO - varia el tono, la estructura y las palabras. Cada mensaje debe ser unico. ANTI-SPAM: Lenguaje humano y natural, NO promocional agresivo. Evita: OFERTAS!!!, TODO EN MAYSCULAS, lenguaje de marketing. Se conversacional.`;

    } else if (tipoEstrategia === 'critico_reactivacion') {
        promptMensaje = `Eres DefiOracle, empresa de remesas Chile-Venezuela. Genera un mensaje de WhatsApp para ${nombreCliente} que lleva ${diasInactivo} días sin enviar dinero. IMPORTANTE: INICIA el mensaje con un saludo personalizado usando el nombre del cliente (Hola ${nombreCliente}, Hola Juan, etc). Ofrece tasa ESPECIAL de reactivacion: ${tasaPromocional.toFixed(3)} VES/CLP, valida SOLO HOY hasta las 21:00. Tono: urgente pero calido, transmite que lo extrañamos. NO menciones "perdidas" ni "riesgos". NO incluyas placeholders como [Tu Nombre] o [Tu Empresa]. El mensaje es DIRECTO del equipo DefiOracle. Emojis: (maximo 3). Horario: 08:00-21:00. Maximo 6 lineas. IMPORTANTE: Se MUY CREATIVO - cada mensaje debe tener diferente estructura, estilo y expresiones. Personaliza segun el contexto. ANTI-SPAM: Urgencia SIN agresividad comercial. Evita: !!URGENTE!!, OFERTA LIMITADA!!!, mayusculas excesivas. Preferir: lenguaje directo pero amigable.`;

    } else if (tipoEstrategia === 'reduccion_actividad') {
        promptMensaje = `Eres DefiOracle, empresa de remesas Chile-Venezuela. Genera un mensaje para ${nombreCliente} que antes enviaba dinero con mas frecuencia pero ahora no tanto. IMPORTANTE: INICIA el mensaje con un saludo personalizado usando el nombre del cliente (Hola ${nombreCliente}, Hola Juan, etc). Tono: preocupacion genuina, pregunta si todo esta bien o si podemos mejorar. Ofrece tasa EXCLUSIVA solo para el/ella: ${tasaPromocional.toFixed(3)} VES/CLP, valida SOLO HOY hasta las 21:00. NO uses palabras corporativas como "retencion", "estrategia", "fidelizacion". Lenguaje cercano y familiar. NO placeholders. Emojis moderados (2-3 max). Horario: 08:00-21:00. Maximo 5 lineas. IMPORTANTE: Varia la forma de expresar preocupacion y oferta. Se unico y creativo en cada mensaje. ANTI-SPAM: Tono empatico y humano, NO ventas. Evita: frases genericas de marketing, exclamaciones excesivas. Parecer conversacion real.`;
    }

    try {
        const result = await callOpenAI({
            model: 'gpt-4o-mini',
            messages: [
                {
                    role: 'system',
                    content: 'Eres DefiOracle, empresa chilena de remesas que ayuda a enviar dinero desde Chile hacia Venezuela usando USDT como puente. Genera mensajes directos, calidos y profesionales en español para WhatsApp. NUNCA uses placeholders como [Tu Nombre], [Tu Empresa], [Firma] - el mensaje ya es de DefiOracle. Usa emojis con moderacion (2-3 maximo). Enfoque: remesas familiares, no inversiones ni perdidas financieras. IMPORTANTE ANTI-SPAM: Escribe como humano real, NO como bot. Evita: palabras todo en mayusculas, multiples signos de exclamacion (!!!), lenguaje muy formal o corporativo, frases genericas de marketing. Preferir: conversacion natural, tuteo, preguntas genuinas, tono cercano como si fuera un amigo. PRIVACIDAD: NO menciones situaciones personales/familiares del cliente ("apoyo a casa", "seres queridos", "familia"). Solo usar: "enviar dinero a Venezuela" o "hacer un envio".'
                },
                {
                    role: 'user',
                    content: promptMensaje
                }
            ],
            maxTokens: 200,
            temperature: 0.9,
            useCache: false // No cachear mensajes personalizados
        });

        return {
            success: true,
            message: result.message.content.trim(),
            usage: result.usage
        };

    } catch (error) {
        console.error('Error generando mensaje con IA:', error.message);

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
 * Wrapper para chatbot con function calling
 */
async function chatbotWithFunctions(messages, functions) {
    try {
        const result = await callOpenAI({
            model: 'gpt-4o-mini',
            messages,
            maxTokens: 500,
            temperature: 0.8,
            functions,
            functionCall: 'auto',
            useCache: false // No cachear conversaciones
        });

        return {
            success: true,
            message: result.message,
            usage: result.usage
        };

    } catch (error) {
        return {
            success: false,
            error: error.message,
            errorCode: error.response?.data?.error?.code
        };
    }
}

/**
 * Obtener estadisticas de uso
 */
function getStats() {
    return {
        ...stats,
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
    callOpenAI,
    generateTaskMessage,
    chatbotWithFunctions,
    getStats,
    resetStats,
    clearCache
};
