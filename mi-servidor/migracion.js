// migracion.js - Sistema de migraciones de base de datos

const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const DB_PATH = path.join(process.env.DATA_DIR || '.', 'database.db');

function runMigrations() {
  return new Promise((resolve, reject) => {
    const db = new sqlite3.Database(DB_PATH, (err) => {
      if (err) {
        console.error('❌ Error al conectar con la base de datos:', err.message);
        return reject(err);
      }
      console.log('✅ Conectado a la base de datos para migraciones.');
    });

    db.serialize(() => {
      console.log('🔄 Iniciando proceso de migraciones...');

      // Verificar si la tabla clientes necesita nuevas columnas
      db.all("PRAGMA table_info(clientes)", [], (err, columns) => {
        if (err) {
          console.error('❌ Error al verificar estructura de clientes:', err.message);
          db.close();
          return reject(err);
        }

        const columnNames = columns ? columns.map(col => col.name) : [];
        const requiredColumns = ['rut', 'email', 'telefono', 'datos_bancarios'];
        const missingColumns = requiredColumns.filter(col => !columnNames.includes(col));

        if (missingColumns.length > 0) {
          console.log(`📝 Añadiendo columnas faltantes a tabla clientes: ${missingColumns.join(', ')}`);
          
          db.run('BEGIN TRANSACTION;');
          
          const alterTablePromises = [];
          if (missingColumns.includes('rut')) {
            alterTablePromises.push(new Promise((res, rej) => {
              db.run('ALTER TABLE clientes ADD COLUMN rut TEXT', (err) => {
                if (err) rej(err);
                else { console.log('  ✓ Columna "rut" añadida'); res(); }
              });
            }));
          }
          if (missingColumns.includes('email')) {
            alterTablePromises.push(new Promise((res, rej) => {
              db.run('ALTER TABLE clientes ADD COLUMN email TEXT', (err) => {
                if (err) rej(err);
                else { console.log('  ✓ Columna "email" añadida'); res(); }
              });
            }));
          }
          if (missingColumns.includes('telefono')) {
            alterTablePromises.push(new Promise((res, rej) => {
              db.run('ALTER TABLE clientes ADD COLUMN telefono TEXT', (err) => {
                if (err) rej(err);
                else { console.log('  ✓ Columna "telefono" añadida'); res(); }
              });
            }));
          }
          if (missingColumns.includes('datos_bancarios')) {
            alterTablePromises.push(new Promise((res, rej) => {
              db.run('ALTER TABLE clientes ADD COLUMN datos_bancarios TEXT', (err) => {
                if (err) rej(err);
                else { console.log('  ✓ Columna "datos_bancarios" añadida'); res(); }
              });
            }));
          }

          Promise.all(alterTablePromises)
            .then(() => {
              db.run('COMMIT;', (err) => {
                if (err) {
                  console.error('❌ Error al confirmar transacción:', err.message);
                  db.run('ROLLBACK;');
                  db.close();
                  reject(err);
                } else {
                  console.log('✅ Migraciones de tabla clientes completadas con éxito.');
                  db.close();
                  resolve();
                }
              });
            })
            .catch((err) => {
              console.error('❌ Error en migraciones:', err.message);
              db.run('ROLLBACK;');
              db.close();
              reject(err);
            });
        } else {
          console.log('✅ La tabla clientes ya tiene todas las columnas necesarias.');
          db.close();
          resolve();
        }
      });
    });
  });
}

// Si se ejecuta directamente
if (require.main === module) {
  runMigrations()
    .then(() => {
      console.log('✅ Proceso de migración finalizado correctamente.');
      process.exit(0);
    })
    .catch((err) => {
      console.error('❌ Error en el proceso de migración:', err.message);
      process.exit(1);
    });
}

module.exports = { runMigrations };
