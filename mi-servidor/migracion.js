// migracion.js - Script para ejecutar en Render

const sqlite3 = require('sqlite3').verbose();
const path = require('path');

// Render define DATA_DIR como la ruta al disco persistente
const DB_PATH = path.join(process.env.DATA_DIR || '.', 'database.db');

const db = new sqlite3.Database(DB_PATH, (err) => {
  if (err) {
    console.error('Error al conectar con la base de datos en Render:', err.message);
    return;
  }
  console.log('Conectado a la base de datos en el disco persistente de Render.');
});

const sql = `ALTER TABLE operaciones ADD COLUMN numero_recibo TEXT UNIQUE`;

db.run(sql, function(err) {
  if (err) {
    if (err.message.includes('duplicate column name')) {
      console.log('✅ La columna "numero_recibo" ya existe. No se necesita hacer nada.');
    } else {
      console.error('❌ Error al añadir la columna:', err.message);
    }
  } else {
    console.log('✅ ¡Éxito! La columna "numero_recibo" ha sido añadida a la tabla "operaciones".');
  }
  
  db.close((err) => {
    if (err) {
      console.error('❌ Error al cerrar la base de datos:', err.message);
    } else {
      console.log('Conexión a la base de datos cerrada.');
    }
  });
});
