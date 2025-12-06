// Script para ejecutar todas las migraciones en orden
const { execSync } = require('child_process');

console.log('🚀 Ejecutando migraciones...\n');

try {
  console.log('📊 Paso 1/2: Migrando tabla de actividad...');
  execSync('node migracion_actividad.js', { stdio: 'inherit' });
  
  console.log('\n💰 Paso 2/2: Migrando tablas de nómina...');
  execSync('node migracion_nomina.js', { stdio: 'inherit' });
  
  console.log('\n✅ Todas las migraciones completadas exitosamente\n');
} catch (error) {
  console.error('\n❌ Error ejecutando migraciones:', error.message);
  process.exit(1);
}
