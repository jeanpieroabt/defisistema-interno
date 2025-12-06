// Script para crear tabla de monitoreo de actividad
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const DB_PATH = path.join(process.env.DATA_DIR || '.', 'database.db');
const db = new sqlite3.Database(DB_PATH);

console.log('📊 Creando tabla de monitoreo de actividad...\n');

db.serialize(() => {
    // Crear tabla actividad_operadores
    db.run(`
        CREATE TABLE IF NOT EXISTS actividad_operadores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER NOT NULL,
            tipo_actividad TEXT NOT NULL CHECK(tipo_actividad IN ('login', 'logout', 'heartbeat', 'operacion', 'tarea', 'mensaje')),
            timestamp TEXT NOT NULL,
            fecha TEXT GENERATED ALWAYS AS (DATE(timestamp)) VIRTUAL,
            metadata TEXT,
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
        )
    `, (err) => {
        if (err) {
            console.error('❌ Error creando tabla actividad_operadores:', err.message);
        } else {
            console.log('✅ Tabla actividad_operadores creada exitosamente');
        }
    });

    // Crear índices para optimizar consultas
    db.run(`
        CREATE INDEX IF NOT EXISTS idx_actividad_usuario_timestamp 
        ON actividad_operadores(usuario_id, timestamp)
    `, (err) => {
        if (err) {
            console.error('❌ Error creando índice:', err.message);
        } else {
            console.log('✅ Índice idx_actividad_usuario_timestamp creado');
        }
    });

    db.run(`
        CREATE INDEX IF NOT EXISTS idx_actividad_tipo 
        ON actividad_operadores(tipo_actividad, timestamp)
    `, (err) => {
        if (err) {
            console.error('❌ Error creando índice:', err.message);
        } else {
            console.log('✅ Índice idx_actividad_tipo creado');
        }
    });

    db.run(`
        CREATE INDEX IF NOT EXISTS idx_actividad_fecha 
        ON actividad_operadores(fecha)
    `, (err) => {
        if (err) {
            console.error('❌ Error creando índice:', err.message);
        } else {
            console.log('✅ Índice idx_actividad_fecha creado');
        }
    });

    console.log('\n🎉 Migración completada. Tabla lista para usar.');
});

db.close();
