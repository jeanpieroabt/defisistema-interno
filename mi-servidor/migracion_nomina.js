const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const dbPath = path.join(process.env.DATA_DIR || '.', 'database.db');
const db = new sqlite3.Database(dbPath);

console.log('📊 Creando tablas del sistema de nómina...\n');

db.serialize(() => {
  // Tabla de periodos de pago (quincenas)
  db.run(`
    CREATE TABLE IF NOT EXISTS periodos_pago (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      anio INTEGER NOT NULL,
      mes INTEGER NOT NULL,
      quincena INTEGER NOT NULL CHECK(quincena IN (1, 2)),
      fecha_inicio TEXT NOT NULL,
      fecha_fin TEXT NOT NULL,
      estado TEXT NOT NULL DEFAULT 'abierto' CHECK(estado IN ('abierto', 'cerrado', 'pagado')),
      fecha_cierre TEXT,
      fecha_pago TEXT,
      creado_en TEXT DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(anio, mes, quincena)
    )
  `, (err) => {
    if (err) {
      console.error('❌ Error creando tabla periodos_pago:', err.message);
    } else {
      console.log('✅ Tabla periodos_pago creada exitosamente');
    }
  });

  // Tabla de nómina (registro de pagos por operador)
  db.run(`
    CREATE TABLE IF NOT EXISTS nomina (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      periodo_id INTEGER NOT NULL,
      usuario_id INTEGER NOT NULL,
      sueldo_base REAL NOT NULL DEFAULT 150.00,
      horas_trabajadas REAL NOT NULL DEFAULT 0,
      bono_asistencia REAL NOT NULL DEFAULT 0,
      bono_atencion_rapida REAL NOT NULL DEFAULT 0,
      comision_ventas REAL NOT NULL DEFAULT 0,
      millones_comisionables REAL NOT NULL DEFAULT 0,
      bono_domingos REAL NOT NULL DEFAULT 0,
      domingos_trabajados INTEGER NOT NULL DEFAULT 0,
      bonos_extra REAL NOT NULL DEFAULT 0,
      nota_bonos TEXT,
      total_pagar REAL NOT NULL DEFAULT 0,
      creado_en TEXT DEFAULT CURRENT_TIMESTAMP,
      actualizado_en TEXT DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (periodo_id) REFERENCES periodos_pago(id),
      FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
      UNIQUE(periodo_id, usuario_id)
    )
  `, (err) => {
    if (err) {
      console.error('❌ Error creando tabla nomina:', err.message);
    } else {
      console.log('✅ Tabla nomina creada exitosamente');
    }
  });

  // Tabla de atención rápida (registro de respuestas en menos de 5 minutos)
  db.run(`
    CREATE TABLE IF NOT EXISTS atencion_rapida (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      usuario_id INTEGER NOT NULL,
      cliente_id INTEGER NOT NULL,
      tipo TEXT NOT NULL CHECK(tipo IN ('operacion', 'mensaje')),
      fecha TEXT NOT NULL,
      tiempo_respuesta_minutos REAL NOT NULL,
      creado_en TEXT DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
      FOREIGN KEY (cliente_id) REFERENCES clientes(id)
    )
  `, (err) => {
    if (err) {
      console.error('❌ Error creando tabla atencion_rapida:', err.message);
    } else {
      console.log('✅ Tabla atencion_rapida creada exitosamente');
    }
  });

  // Índices para mejorar el rendimiento
  db.run(`CREATE INDEX IF NOT EXISTS idx_nomina_periodo ON nomina(periodo_id)`, (err) => {
    if (err) {
      console.error('❌ Error creando índice idx_nomina_periodo:', err.message);
    } else {
      console.log('✅ Índice idx_nomina_periodo creado');
    }
  });

  db.run(`CREATE INDEX IF NOT EXISTS idx_nomina_usuario ON nomina(usuario_id)`, (err) => {
    if (err) {
      console.error('❌ Error creando índice idx_nomina_usuario:', err.message);
    } else {
      console.log('✅ Índice idx_nomina_usuario creado');
    }
  });

  db.run(`CREATE INDEX IF NOT EXISTS idx_atencion_rapida_usuario_fecha ON atencion_rapida(usuario_id, fecha)`, (err) => {
    if (err) {
      console.error('❌ Error creando índice idx_atencion_rapida_usuario_fecha:', err.message);
    } else {
      console.log('✅ Índice idx_atencion_rapida_usuario_fecha creado');
    }
  });

  db.run(`CREATE INDEX IF NOT EXISTS idx_periodos_pago_estado ON periodos_pago(estado)`, (err) => {
    if (err) {
      console.error('❌ Error creando índice idx_periodos_pago_estado:', err.message);
    } else {
      console.log('✅ Índice idx_periodos_pago_estado creado');
    }
  });
});

db.close((err) => {
  if (err) {
    console.error('❌ Error cerrando la base de datos:', err.message);
  } else {
    console.log('\n🎉 Migración de nómina completada exitosamente');
  }
});
