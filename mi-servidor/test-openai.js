// Script de prueba para verificar la API de OpenAI
require('dotenv').config();
const openaiHelper = require('./openai-helper');

async function testOpenAI() {
    console.log('\n=== PRUEBA DE API OPENAI ===\n');

    // Verificar que la API key está configurada
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
        console.error('❌ ERROR: OPENAI_API_KEY no está configurada');
        process.exit(1);
    }

    console.log('✅ API Key configurada:', apiKey.substring(0, 20) + '...');

    // Prueba 1: Generar un mensaje de tarea
    console.log('\n--- Prueba 1: Generar mensaje de tarea ---');
    try {
        const result = await openaiHelper.generateTaskMessage({
            nombreCliente: 'Juan Perez',
            diasInactivo: 45,
            tasaPromocional: 0.634,
            tipoEstrategia: 'inactivo_promocion'
        });

        if (result.success) {
            console.log('✅ Mensaje generado exitosamente:');
            console.log('---');
            console.log(result.message);
            console.log('---');
            console.log(`📊 Tokens: ${result.usage.inputTokens} in + ${result.usage.outputTokens} out`);
            console.log(`💰 Costo: $${result.usage.cost.toFixed(6)}`);
        } else {
            console.log('⚠️ Fallo la generacion, usando fallback:');
            console.log(result.message);
            console.log('Error:', result.error);
        }
    } catch (error) {
        console.error('❌ Error en prueba 1:', error.message);
    }

    // Prueba 2: Obtener estadísticas
    console.log('\n--- Prueba 2: Estadísticas de uso ---');
    const stats = openaiHelper.getStats();
    console.log('📊 Estadísticas:');
    console.log(`  - Total requests: ${stats.totalRequests}`);
    console.log(`  - Exitosos: ${stats.successfulRequests}`);
    console.log(`  - Fallidos: ${stats.failedRequests}`);
    console.log(`  - Tokens input: ${stats.totalInputTokens}`);
    console.log(`  - Tokens output: ${stats.totalOutputTokens}`);
    console.log(`  - Costo total: $${stats.totalCostUSD.toFixed(6)}`);
    console.log(`  - Tasa de exito: ${stats.successRate}`);
    console.log(`  - Cache hits: ${stats.cacheHits} (${stats.cacheHitRate})`);
    console.log(`  - Costo promedio por request: $${stats.averageCostPerRequest.toFixed(6)}`);

    console.log('\n=== PRUEBA COMPLETADA ===\n');
}

testOpenAI();
