// migracion.js - Versión 2: Recrea la tabla para añadir la columna UNIQUE

const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const DB_PATH = path.join(process.env.DATA_DIR || '.', 'database.db');

const db = new sqlite3.Database(DB_PATH, (err) => {
  if (err) {
    return console.error('❌ Error al conectar con la base de datos:', err.message);
  }
  console.log('✅ Conectado a la base de datos para la migración.');
});

// Usamos db.serialize para asegurar que los comandos se ejecuten en orden
db.serialize(() => {
  console.log('Iniciando proceso de migración...');

  // 1. Iniciar una transacción para que todo el proceso sea atómico
  db.run('BEGIN TRANSACTION;', function(err) {
    if (err) {
        console.error('❌ Error al iniciar la transacción. Abortando.', err.message);
        db.run('ROLLBACK;');
        return;
    }
    console.log('Paso 1: Transacción iniciada.');
  });

  // 2. Renombrar la tabla vieja
  db.run('ALTER TABLE operaciones RENAME TO operaciones_viejas;', function(err) {
    if (err) {
        // Si la tabla vieja ya existe, puede que el script fallara a la mitad antes.
        if (err.message.includes('already exists')) {
            console.log('⚠️ Parece que un intento de migración anterior falló. Limpiando tabla vieja...');
            db.run('DROP TABLE operaciones_viejas;', (dropErr) => {
                if(dropErr) return console.error('❌ No se pudo limpiar la tabla vieja. Abortando.', dropErr.message);
                db.run('ALTER TABLE operaciones RENAME TO operaciones_viejas;');
            });
        } else {
             console.error('❌ Error en Paso 2 (renombrar tabla). Abortando.', err.message);
             db.run('ROLLBACK;');
             return;
        }
    }
    console.log('Paso 2: Tabla "operaciones" renombrada a "operaciones_viejas".');
  });

  // 3. Crear la nueva tabla con la estructura correcta
  const createTableSql = `
    CREATE TABLE operaciones (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      usuario_id INTEGER NOT NULL,
      cliente_id INTEGER NOT NULL,
      fecha TEXT NOT NULL,
      monto_clp REAL NOT NULL,
      monto_ves REAL NOT NULL,
      tasa REAL NOT NULL,
      observaciones TEXT,
      costo_clp REAL DEFAULT 0,
      comision_ves REAL DEFAULT 0,
      numero_recibo TEXT UNIQUE,
      FOREIGN KEY(usuario_id) REFERENCES usuarios(id),
      FOREIGN KEY(cliente_id) REFERENCES clientes(id)
    );`;
  db.run(createTableSql, function(err) {
    if (err) {
        console.error('❌ Error en Paso 3 (crear nueva tabla). Abortando.', err.message);
        db.run('ROLLBACK;');
        return;
    }
    console.log('Paso 3: Nueva tabla "operaciones" creada con el esquema correcto.');
  });

  // 4. Copiar los datos de la tabla vieja a la nueva
  const copyDataSql = `
    INSERT INTO operaciones (id, usuario_id, cliente_id, fecha, monto_clp, monto_ves, tasa, observaciones, costo_clp, comision_ves)
    SELECT id, usuario_id, cliente_id, fecha, monto_clp, monto_ves, tasa, observaciones, costo_clp, comision_ves
    FROM operaciones_viejas;`;
  db.run(copyDataSql, function(err) {
    if (err) {
        console.error('❌ Error en Paso 4 (copiar datos). Abortando.', err.message);
        db.run('ROLLBACK;');
        return;
    }
    console.log(`Paso 4: ${this.changes} filas de datos copiadas a la nueva tabla.`);
  });

  // 5. Eliminar la tabla vieja
  db.run('DROP TABLE operaciones_viejas;', function(err) {
    if (err) {
        console.error('❌ Error en Paso 5 (eliminar tabla vieja). Abortando.', err.message);
        db.run('ROLLBACK;');
        return;
    }
    console.log('Paso 5: Tabla "operaciones_viejas" eliminada.');
  });

  // 6. Confirmar la transacción
  db.run('COMMIT;', function(err) {
    if (err) {
        console.error('❌ Error al confirmar la transacción. La base de datos puede estar en un estado inconsistente.', err.message);
        return;
    }
    console.log('✅ ¡Migración completada con éxito!');
  });
});

// Cerrar la conexión
db.close((err) => {
  if (err) console.error('❌ Error al cerrar la base de datos:', err.message);
});
