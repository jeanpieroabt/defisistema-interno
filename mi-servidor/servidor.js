// servidor.js
// =======================================================
// Defi Oracle – Backend (Auth, Envíos, Histórico, Tasas, Compras, Operadores)
// =======================================================

const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');
const session = require('express-session');
const path = require('path');
const axios = require('axios');

// ✅ RUTA DE LA BASE DE DATOS AJUSTADA PARA DESPLIEGUE
const DB_PATH = path.join(process.env.DATA_DIR || '.', 'database.db');

const app = express();
// ✅ PUERTO AJUSTADO PARA DESPLIEGUE
const PORT = process.env.PORT || 3000;

// Zona horaria ajustada a Caracas, Venezuela
process.env.TZ = process.env.TZ || 'America/Caracas';

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static('.'));
app.use(
  session({
    secret: 'defi-oracle-sesion-muy-larga-y-robusta',
    resave: false,
    saveUninitialized: false,
    cookie: { secure: false, maxAge: 1000 * 60 * 60 * 8 }, // 8h
  })
);

// -------------------- DB --------------------
// ✅ CONEXIÓN USANDO LA RUTA DINÁMICA
const db = new sqlite3.Database(DB_PATH, (err) => {
  if (err) console.error('Error DB:', err.message);
  else console.log(`SQLite conectado en: ${DB_PATH}`);
});

// Función para ejecutar una Promesa para cada sentencia SQL
const dbRun = (sql, params = []) => {
    return new Promise((resolve, reject) => {
        db.run(sql, params, function(err) {
            if (err) reject(err);
            else resolve(this);
        });
    });
};

const dbGet = (sql, params = []) => {
    return new Promise((resolve, reject) => {
        db.get(sql, params, (err, row) => err ? reject(err) : resolve(row));
    });
};

const dbAll = (sql, params = []) => {
    return new Promise((resolve, reject) => {
        db.all(sql, params, (err, rows) => err ? reject(err) : resolve(rows));
    });
};

// =================================================================
// INICIO: LÓGICA DE CÁLCULO DE COSTO REFINADA
// =================================================================
const getAvgPurchaseRate = (date, callback) => {
    const sql = `SELECT SUM(clp_invertido) as totalClp, SUM(ves_obtenido) as totalVes FROM compras WHERE date(fecha) = date(?)`;
    db.get(sql, [date], (err, row) => {
        if (err) return callback(err, 0);
        if (!row || !row.totalClp || row.totalClp === 0) {
            return callback(null, 0);
        }
        const rate = row.totalVes / row.totalClp;
        return callback(null, rate);
    });
};

const calcularCostoClpPorVes = (fecha, callback) => {
    getAvgPurchaseRate(fecha, (err, rate) => {
        if (err) {
            console.error(`Error obteniendo tasa de compra para el día ${fecha}:`, err.message);
            return callback(err);
        }
        if (rate > 0) {
            return callback(null, 1 / rate);
        }

        db.get(`SELECT tasa_clp_ves FROM compras WHERE date(fecha) <= date(?) ORDER BY fecha DESC, id DESC LIMIT 1`, [fecha], (errLast, lastPurchase) => {
            if (errLast) {
                console.error(`Error obteniendo última tasa histórica para fecha ${fecha}:`, errLast.message);
                return callback(errLast);
            }
            if (lastPurchase && lastPurchase.tasa_clp_ves > 0) {
                return callback(null, 1 / lastPurchase.tasa_clp_ves);
            }

            db.get(`SELECT tasa_clp_ves FROM compras ORDER BY fecha ASC, id ASC LIMIT 1`, [], (errNext, nextPurchase) => {
                if (errNext) {
                    console.error(`Error obteniendo primera tasa histórica disponible:`, errNext.message);
                    return callback(errNext);
                }
                if (nextPurchase && nextPurchase.tasa_clp_ves > 0) {
                    return callback(null, 1 / nextPurchase.tasa_clp_ves);
                }

                readConfigValue('capitalCostoVesPorClp')
                    .then(costoConfig => callback(null, costoConfig))
                    .catch(e => callback(e));
            });
        });
    });
};
// =================================================================
// FIN: LÓGICA DE CÁLCULO DE COSTO REFINADA
// =================================================================

// =================================================================
// INICIO: MIGRACIÓN Y VERIFICACIÓN DE BASE DE DATOS
// =================================================================
const runMigrations = async () => {
    console.log('Iniciando verificación de la estructura de la base de datos...');

    const addColumn = async (tableName, columnDef) => {
        const columnName = columnDef.split(' ')[0];
        try {
            await dbRun(`ALTER TABLE ${tableName} ADD COLUMN ${columnDef}`);
            console.log(`✅ Columna '${columnName}' añadida a la tabla '${tableName}'.`);
        } catch (err) {
            if (!err.message.includes('duplicate column name')) {
                console.error(`❌ Error al añadir columna ${columnName} a ${tableName}:`, err.message);
                throw err;
            }
        }
    };

    await dbRun(`CREATE TABLE IF NOT EXISTS usuarios(id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL, password TEXT NOT NULL, role TEXT NOT NULL CHECK(role IN ('master','operador')))`);
    
    await dbRun(`CREATE TABLE IF NOT EXISTS clientes(id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT UNIQUE NOT NULL, fecha_creacion TEXT NOT NULL)`);
    await addColumn('clientes', 'rut TEXT');
    await addColumn('clientes', 'email TEXT');
    await addColumn('clientes', 'telefono TEXT');
    await addColumn('clientes', 'datos_bancarios TEXT');

    await dbRun(`CREATE TABLE IF NOT EXISTS operaciones(id INTEGER PRIMARY KEY AUTOINCREMENT, usuario_id INTEGER NOT NULL, cliente_id INTEGER NOT NULL, fecha TEXT NOT NULL, monto_clp REAL NOT NULL, monto_ves REAL NOT NULL, tasa REAL NOT NULL, observaciones TEXT, numero_recibo TEXT UNIQUE, FOREIGN KEY(usuario_id) REFERENCES usuarios(id), FOREIGN KEY(cliente_id) REFERENCES clientes(id))`);
    await addColumn('operaciones', 'costo_clp REAL DEFAULT 0');
    await addColumn('operaciones', 'comision_ves REAL DEFAULT 0');

    await dbRun(`CREATE TABLE IF NOT EXISTS compras(id INTEGER PRIMARY KEY AUTOINCREMENT, usuario_id INTEGER NOT NULL, clp_invertido REAL NOT NULL, ves_obtenido REAL NOT NULL, tasa_clp_ves REAL NOT NULL, fecha TEXT NOT NULL, FOREIGN KEY(usuario_id) REFERENCES usuarios(id))`);
    await addColumn('compras', 'tasa_clp_ves REAL DEFAULT 0');
    
    await dbRun(`CREATE TABLE IF NOT EXISTS configuracion(clave TEXT PRIMARY KEY, valor TEXT)`);
    
    // Inicializar valores por defecto en configuracion si no existen
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('saldoVesOnline', '0')`);
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('totalGananciaAcumuladaClp', '0')`);
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('capitalInicialClp', '0')`);
    
    // ✅ NUEVA TABLA PARA METAS
    await dbRun(`CREATE TABLE IF NOT EXISTS metas(id INTEGER PRIMARY KEY AUTOINCREMENT, mes TEXT NOT NULL UNIQUE, meta_clientes_activos INTEGER DEFAULT 0, meta_nuevos_clientes INTEGER DEFAULT 0, meta_volumen_clp REAL DEFAULT 0, meta_operaciones INTEGER DEFAULT 0)`);

    // ✅ TABLA PARA TAREAS
    await dbRun(`CREATE TABLE IF NOT EXISTS tareas(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        titulo TEXT NOT NULL,
        descripcion TEXT,
        tipo TEXT NOT NULL CHECK(tipo IN ('manual','automatica')),
        prioridad TEXT DEFAULT 'normal' CHECK(prioridad IN ('baja','normal','alta','urgente')),
        estado TEXT DEFAULT 'pendiente' CHECK(estado IN ('pendiente','en_progreso','completada','cancelada')),
        asignado_a INTEGER,
        creado_por INTEGER NOT NULL,
        fecha_creacion TEXT NOT NULL,
        fecha_vencimiento TEXT,
        fecha_completada TEXT,
        resultado TEXT,
        observaciones TEXT,
        FOREIGN KEY(asignado_a) REFERENCES usuarios(id),
        FOREIGN KEY(creado_por) REFERENCES usuarios(id)
    )`);

    // ✅ TABLA PARA NOTIFICACIONES
    await dbRun(`CREATE TABLE IF NOT EXISTS notificaciones(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario_id INTEGER NOT NULL,
        tipo TEXT NOT NULL CHECK(tipo IN ('tarea','alerta','info','sistema')),
        titulo TEXT NOT NULL,
        mensaje TEXT NOT NULL,
        leida INTEGER DEFAULT 0,
        fecha_creacion TEXT NOT NULL,
        tarea_id INTEGER,
        FOREIGN KEY(usuario_id) REFERENCES usuarios(id),
        FOREIGN KEY(tarea_id) REFERENCES tareas(id)
    )`);

    // ✅ TABLA PARA ALERTAS DE CLIENTES
    await dbRun(`CREATE TABLE IF NOT EXISTS alertas(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id INTEGER NOT NULL,
        tipo TEXT NOT NULL CHECK(tipo IN ('inactivo','critico','disminucion')),
        severidad TEXT NOT NULL CHECK(severidad IN ('warning','danger')),
        dias_inactivo INTEGER,
        ultima_operacion TEXT,
        tarea_id INTEGER,
        accion_realizada TEXT CHECK(accion_realizada IN ('mensaje_enviado','promocion_enviada',NULL)),
        fecha_accion TEXT,
        fecha_creacion TEXT NOT NULL,
        activa INTEGER DEFAULT 1,
        FOREIGN KEY(cliente_id) REFERENCES clientes(id),
        FOREIGN KEY(tarea_id) REFERENCES tareas(id)
    )`);

    // ✅ TABLA PARA HISTORIAL DE CHATBOT
    await dbRun(`CREATE TABLE IF NOT EXISTS chatbot_history(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario_id INTEGER NOT NULL,
        rol TEXT NOT NULL,
        mensaje TEXT NOT NULL,
        respuesta TEXT NOT NULL,
        contexto_datos TEXT,
        fecha_creacion TEXT NOT NULL,
        FOREIGN KEY(usuario_id) REFERENCES usuarios(id)
    )`);

    // ✅ TABLA PARA MENSAJES PROACTIVOS DEL BOT
    await dbRun(`CREATE TABLE IF NOT EXISTS chatbot_mensajes_proactivos(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario_id INTEGER NOT NULL,
        tipo TEXT NOT NULL CHECK(tipo IN ('celebracion','recordatorio','alerta','sugerencia','informativo')),
        mensaje TEXT NOT NULL,
        contexto TEXT,
        prioridad TEXT DEFAULT 'normal' CHECK(prioridad IN ('baja','normal','alta')),
        mostrado INTEGER DEFAULT 0,
        fecha_creacion TEXT NOT NULL,
        fecha_mostrado TEXT,
        FOREIGN KEY(usuario_id) REFERENCES usuarios(id)
    )`);

    // ✅ TABLA PARA HISTORIAL DE MONITOREO DE TASAS
    await dbRun(`CREATE TABLE IF NOT EXISTS tasas_monitoreo(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tasa_p2p_250k REAL NOT NULL,
        tasa_nivel1 REAL,
        tasa_nivel2 REAL,
        tasa_nivel3 REAL,
        diferencia_porcentaje REAL,
        alerta_generada INTEGER DEFAULT 0,
        fecha_verificacion TEXT NOT NULL
    )`);

    // ✅ TABLA PARA ALERTAS DE TASAS (notificaciones al master)
    await dbRun(`CREATE TABLE IF NOT EXISTS tasas_alertas(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tasa_p2p_250k REAL NOT NULL,
        tasa_nivel3_actual REAL NOT NULL,
        diferencia_porcentaje REAL NOT NULL,
        estado TEXT DEFAULT 'pendiente' CHECK(estado IN ('pendiente','respondida','auto_ajustada')),
        notificacion_id INTEGER,
        respuesta_master TEXT,
        fecha_creacion TEXT NOT NULL,
        fecha_respuesta TEXT,
        fecha_auto_ajuste TEXT,
        tasa_ajustada REAL,
        FOREIGN KEY(notificacion_id) REFERENCES notificaciones(id)
    )`);

    return new Promise(resolve => {
        db.get(`SELECT COUNT(*) c FROM usuarios`, async (err, row) => {
            if (err) return console.error('Error al verificar usuarios semilla:', err.message);
            if (!row || row.c === 0) {
                const hash = await bcrypt.hash('master123', 10);
                await dbRun(`INSERT INTO usuarios(username,password,role) VALUES (?,?,?)`, ['master', hash, 'master']);
                console.log('✅ Usuario semilla creado: master/master123');
            }
            console.log('✅ Verificación de base de datos completada.');
            resolve();
        });
    });
};
// =================================================================
// FIN: MIGRACIÓN Y VERIFICACIÓN DE BASE DE DATOS
// =================================================================


// -------------------- Helpers --------------------
const pageAuth = (req, res, next) => {
  if (!req.session.user) return res.redirect('/login.html');
  next();
};
const apiAuth = (req, res, next) => {
  if (!req.session.user) return res.status(401).json({ message: 'No autorizado' });
  next();
};
const onlyMaster = (req, res, next) => {
  if (req.session.user?.role === 'master') return next();
  return res.status(403).json({ message: 'Acceso denegado' });
};
const hoyLocalYYYYMMDD = () => {
  const now = new Date();
  const local = new Date(now.getTime() - now.getTimezoneOffset() * 60000);
  return local.toISOString().slice(0, 10);
};
const inicioMesLocalYYYYMMDD = () => {
  const now = new Date();
  const local = new Date(now.getTime() - now.getTimezoneOffset() * 60000);
  return local.toISOString().slice(0, 7) + '-01'; 
};

const upsertConfig = (clave, valor, cb) => {
  db.run(
    `INSERT INTO configuracion(clave,valor) VALUES(?,?) ON CONFLICT(clave) DO UPDATE SET valor=excluded.valor`,
    [clave, valor],
    cb
  );
};
const readConfigValue = (clave) => {
  return new Promise((resolve, reject) => {
    db.get(`SELECT valor FROM configuracion WHERE clave=?`, [clave], (e, row) => {
      if (e) return reject(e);
      resolve(row ? Number(row.valor) : 0);
    });
  });
};

// -------------------- Páginas --------------------
app.get('/', (req, res) => res.redirect('/login.html'));
app.get('/app.html', pageAuth, (req, res) => res.sendFile(path.join(__dirname, 'app.html')));
app.get('/admin.html', pageAuth, onlyMaster, (req, res) => res.sendFile(path.join(__dirname, 'admin.html')));
app.get('/historico.html', pageAuth, (req, res) => res.sendFile(path.join(__dirname, 'historico.html')));
app.get('/clientes.html', pageAuth, (req, res) => res.sendFile(path.join(__dirname, 'clientes.html')));
app.get('/analytics.html', pageAuth, onlyMaster, (req, res) => res.sendFile(path.join(__dirname, 'analytics.html')));

// -------------------- Auth --------------------
app.post('/login', (req, res) => {
  const { username, password } = req.body;
  db.get(`SELECT * FROM usuarios WHERE username=?`, [username], (err, u) => {
    if (err || !u) return res.status(400).json({ message: 'Credenciales inválidas' });
    bcrypt.compare(password, u.password, (e, ok) => {
      if (e || !ok) return res.status(400).json({ message: 'Credenciales inválidas' });
      req.session.user = { id: u.id, username: u.username, role: u.role };
      res.json({ message: 'Login OK' });
    });
  });
});
app.get('/logout', (req, res) => {
  req.session.destroy(() => {
    res.clearCookie('connect.sid');
    res.redirect('/login.html');
  });
});
app.get('/api/user-info', apiAuth, (req, res) => res.json(req.session.user));

// --- Endpoint para verificar número de recibo ---
app.get('/api/recibo/check', apiAuth, (req, res) => {
    const { numero, excludeId } = req.query;
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    if (!numero) return res.json({ usado: false });
    let sql = `SELECT id FROM operaciones WHERE numero_recibo = ?`;
    const params = [numero];
    if (excludeId) {
        sql += ' AND id != ?';
        params.push(excludeId);
    }
    db.get(sql, params, (err, row) => {
        if (err) {
            console.error("Error en /api/recibo/check:", err.message);
            return res.status(500).json({ message: 'Error en la base de datos al verificar el recibo.' });
        }
        res.json({ usado: !!row });
    });
});

// -------------------- Rutas de búsqueda añadidas --------------------
app.get('/api/clientes/search', apiAuth, (req, res) => {
    const term = String(req.query.term || '').trim();
    if (!term || term.length < 2) return res.json([]);
    db.all(`SELECT nombre FROM clientes WHERE nombre LIKE ? ORDER BY nombre LIMIT 10`, [`%${term}%`], (err, rows) => {
        if (err) return res.status(500).json({ message: 'Error al buscar clientes.' });
        res.json((rows || []).map(r => r.nombre));
    });
});
app.get('/api/usuarios/search', apiAuth, onlyMaster, (req, res) => {
    const term = String(req.query.term || '').trim();
    if (!term || term.length < 1) return res.json([]);
    db.all(`SELECT username FROM usuarios WHERE username LIKE ? ORDER BY username LIMIT 10`, [`%${term}%`], (err, rows) => {
        if (err) return res.status(500).json({ message: 'Error al buscar usuarios.' });
        res.json((rows || []).map(r => r.username));
    });
});


// =================================================================
// INICIO: ENDPOINTS PARA LA GESTIÓN DE CLIENTES (CRUD)
// =================================================================
// ✅ ENDPOINT DE CLIENTES MODIFICADO PARA PAGINACIÓN Y BÚSQUEDA
app.get('/api/clientes', apiAuth, async (req, res) => {
    try {
        const page = parseInt(req.query.page || '1', 10);
        const limit = parseInt(req.query.limit || '100', 10);
        const search = req.query.search || '';
        const offset = (page - 1) * limit;

        const searchPattern = `%${search}%`;
        
        const countResult = await dbGet(`SELECT COUNT(*) as total FROM clientes WHERE nombre LIKE ?`, [searchPattern]);
        const total = countResult.total;
        
        const clientes = await dbAll(`SELECT * FROM clientes WHERE nombre LIKE ? ORDER BY nombre LIMIT ? OFFSET ?`, [searchPattern, limit, offset]);

        res.json({
            clientes,
            total,
            page,
            limit
        });
    } catch (error) {
        console.error("Error en GET /api/clientes:", error);
        res.status(500).json({ message: 'Error al obtener clientes.' });
    }
});

// 🔍 ENDPOINT PARA BUSCAR POSIBLES DUPLICADOS (debe ir ANTES de /api/clientes/:id)
app.get('/api/clientes/duplicados', apiAuth, onlyMaster, async (req, res) => {
    try {
        const clientes = await dbAll(`SELECT id, nombre, rut, email, telefono FROM clientes ORDER BY LOWER(nombre)`);
        const duplicados = [];
        const procesados = new Set();
        
        // Función para normalizar texto (sin acentos, minúsculas, sin espacios múltiples)
        const normalizar = (texto) => {
            return texto
                .toLowerCase()
                .normalize('NFD')
                .replace(/[\u0300-\u036f]/g, '') // Eliminar acentos
                .trim()
                .replace(/\s+/g, ' '); // Normalizar espacios
        };
        
        // Función para calcular similitud de Levenshtein
        const levenshteinDistance = (str1, str2) => {
            const len1 = str1.length;
            const len2 = str2.length;
            const matrix = [];
            
            for (let i = 0; i <= len1; i++) {
                matrix[i] = [i];
            }
            for (let j = 0; j <= len2; j++) {
                matrix[0][j] = j;
            }
            
            for (let i = 1; i <= len1; i++) {
                for (let j = 1; j <= len2; j++) {
                    const cost = str1[i - 1] === str2[j - 1] ? 0 : 1;
                    matrix[i][j] = Math.min(
                        matrix[i - 1][j] + 1,
                        matrix[i][j - 1] + 1,
                        matrix[i - 1][j - 1] + cost
                    );
                }
            }
            
            return matrix[len1][len2];
        };
        
        // Función para verificar similitud (más flexible)
        const sonSimilares = (nombre1, nombre2) => {
            const n1 = normalizar(nombre1);
            const n2 = normalizar(nombre2);
            
            // Exactos
            if (n1 === n2) return true;
            
            const palabras1 = n1.split(' ').filter(p => p.length > 0);
            const palabras2 = n2.split(' ').filter(p => p.length > 0);
            
            // Si uno contiene al otro completamente
            if (n1.includes(n2) || n2.includes(n1)) {
                const longitudMin = Math.min(n1.length, n2.length);
                // Solo si el más corto tiene al menos 4 caracteres
                if (longitudMin >= 4) return true;
            }
            
            // Calcular similitud con Levenshtein (para nombres cortos similares)
            const longitudMax = Math.max(n1.length, n2.length);
            if (longitudMax <= 15) { // Solo para nombres relativamente cortos
                const distancia = levenshteinDistance(n1, n2);
                const similitud = 1 - (distancia / longitudMax);
                // Si tienen más del 75% de similitud
                if (similitud >= 0.75) return true;
            }
            
            // Verificar si comparten al menos una palabra significativa (>3 chars)
            const palabrasSignificativas1 = palabras1.filter(p => p.length > 3);
            const palabrasSignificativas2 = palabras2.filter(p => p.length > 3);
            
            for (const p1 of palabrasSignificativas1) {
                for (const p2 of palabrasSignificativas2) {
                    // Palabras iguales o muy similares
                    if (p1 === p2) return true;
                    
                    // Similitud entre palabras individuales
                    if (p1.length >= 4 && p2.length >= 4) {
                        const distPalabra = levenshteinDistance(p1, p2);
                        const similitudPalabra = 1 - (distPalabra / Math.max(p1.length, p2.length));
                        if (similitudPalabra >= 0.8) return true; // 80% similitud en palabra
                    }
                    
                    // Una palabra contiene a la otra
                    if ((p1.length > 4 && p2.includes(p1)) || (p2.length > 4 && p1.includes(p2))) {
                        return true;
                    }
                }
            }
            
            // Verificar coincidencia de apellidos (última palabra si hay varias)
            if (palabras1.length >= 2 && palabras2.length >= 2) {
                const apellido1 = palabras1[palabras1.length - 1];
                const apellido2 = palabras2[palabras2.length - 1];
                
                if (apellido1.length > 3 && apellido2.length > 3) {
                    // Apellidos iguales o muy similares
                    if (apellido1 === apellido2) return true;
                    
                    const distApellido = levenshteinDistance(apellido1, apellido2);
                    const similApellido = 1 - (distApellido / Math.max(apellido1.length, apellido2.length));
                    if (similApellido >= 0.85) return true; // 85% similitud en apellido
                }
            }
            
            return false;
        };
        
        for (let i = 0; i < clientes.length; i++) {
            if (procesados.has(clientes[i].id)) continue;
            
            const similares = [];
            
            for (let j = i + 1; j < clientes.length; j++) {
                if (procesados.has(clientes[j].id)) continue;
                
                if (sonSimilares(clientes[i].nombre, clientes[j].nombre)) {
                    similares.push(clientes[j]);
                    procesados.add(clientes[j].id);
                }
            }
            
            if (similares.length > 0) {
                const grupo = [clientes[i], ...similares];
                const opsPromises = grupo.map(c => 
                    dbGet(`SELECT COUNT(*) as total FROM operaciones WHERE cliente_id = ?`, [c.id])
                );
                const opsCounts = await Promise.all(opsPromises);
                
                duplicados.push({
                    nombre_base: clientes[i].nombre,
                    clientes: grupo.map((c, idx) => ({
                        id: c.id,
                        nombre: c.nombre,
                        rut: c.rut || '',
                        email: c.email || '',
                        telefono: c.telefono || '',
                        operaciones: opsCounts[idx].total
                    }))
                });
                
                procesados.add(clientes[i].id);
            }
        }
        
        res.json(duplicados);
    } catch (error) {
        console.error('Error buscando duplicados:', error);
        res.status(500).json({ message: 'Error al buscar duplicados.' });
    }
});

// 🔀 ENDPOINT PARA FUSIONAR CLIENTES DUPLICADOS (debe ir ANTES de /api/clientes/:id)
app.post('/api/clientes/fusionar', apiAuth, onlyMaster, async (req, res) => {
    const { cliente_principal_id, cliente_duplicado_id } = req.body;
    
    if (!cliente_principal_id || !cliente_duplicado_id) {
        return res.status(400).json({ message: 'Se requieren ambos IDs de clientes.' });
    }
    
    if (cliente_principal_id === cliente_duplicado_id) {
        return res.status(400).json({ message: 'No se puede fusionar un cliente consigo mismo.' });
    }
    
    try {
        // Verificar que ambos clientes existen
        const clientePrincipal = await dbGet(`SELECT * FROM clientes WHERE id = ?`, [cliente_principal_id]);
        const clienteDuplicado = await dbGet(`SELECT * FROM clientes WHERE id = ?`, [cliente_duplicado_id]);
        
        if (!clientePrincipal || !clienteDuplicado) {
            return res.status(404).json({ message: 'Uno o ambos clientes no existen.' });
        }
        
        // Contar operaciones a transferir
        const countOps = await dbGet(`SELECT COUNT(*) as total FROM operaciones WHERE cliente_id = ?`, [cliente_duplicado_id]);
        
        await dbRun('BEGIN TRANSACTION');
        
        // Transferir todas las operaciones del duplicado al principal
        await dbRun(`UPDATE operaciones SET cliente_id = ? WHERE cliente_id = ?`, [cliente_principal_id, cliente_duplicado_id]);
        
        // Actualizar datos del cliente principal si el duplicado tiene información adicional
        const updates = [];
        const params = [];
        
        if (!clientePrincipal.rut && clienteDuplicado.rut) { updates.push('rut = ?'); params.push(clienteDuplicado.rut); }
        if (!clientePrincipal.email && clienteDuplicado.email) { updates.push('email = ?'); params.push(clienteDuplicado.email); }
        if (!clientePrincipal.telefono && clienteDuplicado.telefono) { updates.push('telefono = ?'); params.push(clienteDuplicado.telefono); }
        if (!clientePrincipal.datos_bancarios && clienteDuplicado.datos_bancarios) { updates.push('datos_bancarios = ?'); params.push(clienteDuplicado.datos_bancarios); }
        
        if (updates.length > 0) {
            params.push(cliente_principal_id);
            await dbRun(`UPDATE clientes SET ${updates.join(', ')} WHERE id = ?`, params);
        }
        
        // Eliminar el cliente duplicado
        await dbRun(`DELETE FROM clientes WHERE id = ?`, [cliente_duplicado_id]);
        
        await dbRun('COMMIT');
        
        res.json({ 
            message: 'Clientes fusionados con éxito.', 
            operaciones_transferidas: countOps.total,
            cliente_final: clientePrincipal.nombre
        });
        
    } catch (error) {
        await dbRun('ROLLBACK');
        console.error('Error fusionando clientes:', error);
        res.status(500).json({ message: 'Error al fusionar clientes.' });
    }
});

app.get('/api/clientes/:id', apiAuth, (req, res) => {
    db.get('SELECT * FROM clientes WHERE id = ?', [req.params.id], (err, row) => {
        if (err) return res.status(500).json({ message: 'Error al obtener el cliente.' });
        if (!row) return res.status(404).json({ message: 'Cliente no encontrado.' });
        res.json(row);
    });
});
app.post('/api/clientes', apiAuth, (req, res) => {
    let { nombre, rut, email, telefono, datos_bancarios } = req.body;
    if (!nombre) return res.status(400).json({ message: 'El nombre es obligatorio.' });
    
    // Normalizar nombre: Title Case (Primera letra mayúscula de cada palabra)
    nombre = nombre.trim().split(/\s+/).map(palabra => 
        palabra.charAt(0).toUpperCase() + palabra.slice(1).toLowerCase()
    ).join(' ');
    
    // Verificar si ya existe un cliente con el mismo nombre (case-insensitive)
    db.get(`SELECT id, nombre FROM clientes WHERE LOWER(nombre) = LOWER(?)`, [nombre], (err, existente) => {
        if (err) return res.status(500).json({ message: 'Error al verificar cliente.' });
        if (existente) return res.status(400).json({ message: `Ya existe un cliente con ese nombre: "${existente.nombre}"` });
        
        const sql = `INSERT INTO clientes (nombre, rut, email, telefono, datos_bancarios, fecha_creacion) VALUES (?, ?, ?, ?, ?, ?)`;
        db.run(sql, [nombre, rut, email, telefono, datos_bancarios, hoyLocalYYYYMMDD()], function(err) {
            if (err) {
                if (err.message.includes('UNIQUE constraint failed')) return res.status(400).json({ message: 'Ya existe un cliente con ese nombre.' });
                return res.status(500).json({ message: 'Error al crear el cliente.' });
            }
            res.status(201).json({ id: this.lastID, message: 'Cliente creado con éxito.' });
        });
    });
});
app.put('/api/clientes/:id', apiAuth, (req, res) => {
    let { nombre, rut, email, telefono, datos_bancarios } = req.body;
    if (!nombre) return res.status(400).json({ message: 'El nombre es obligatorio.' });
    
    // Normalizar nombre: Title Case
    nombre = nombre.trim().split(/\s+/).map(palabra => 
        palabra.charAt(0).toUpperCase() + palabra.slice(1).toLowerCase()
    ).join(' ');
    
    // Verificar si ya existe otro cliente con el mismo nombre (case-insensitive)
    db.get(`SELECT id, nombre FROM clientes WHERE LOWER(nombre) = LOWER(?) AND id != ?`, [nombre, req.params.id], (err, existente) => {
        if (err) return res.status(500).json({ message: 'Error al verificar cliente.' });
        if (existente) return res.status(400).json({ message: `Ya existe otro cliente con ese nombre: "${existente.nombre}"` });
        
        const sql = `UPDATE clientes SET nombre = ?, rut = ?, email = ?, telefono = ?, datos_bancarios = ? WHERE id = ?`;
        db.run(sql, [nombre, rut, email, telefono, datos_bancarios, req.params.id], function(err) {
            if (err) {
                if (err.message.includes('UNIQUE constraint failed')) return res.status(400).json({ message: 'Ya existe otro cliente con ese nombre.' });
                return res.status(500).json({ message: 'Error al actualizar el cliente.' });
            }
            if (this.changes === 0) return res.status(404).json({ message: 'Cliente no encontrado.' });
            res.json({ message: 'Cliente actualizado con éxito.' });
        });
    });
});
app.delete('/api/clientes/:id', apiAuth, onlyMaster, (req, res) => {
    db.run('DELETE FROM clientes WHERE id = ?', [req.params.id], function(err) {
        if (err) return res.status(500).json({ message: 'Error al eliminar el cliente.' });
        if (this.changes === 0) return res.status(404).json({ message: 'Cliente no encontrado.' });
        res.json({ message: 'Cliente eliminado con éxito.' });
    });
});
// =================================================================
// FIN: ENDPOINTS PARA LA GESTIÓN DE CLIENTES (CRUD)
// =================================================================


// -------------------- APIs de Costos y KPIs --------------------

app.get('/api/dashboard', apiAuth, async (req, res) => {
    try {
        const userRole = req.session.user?.role;
        const hoy = hoyLocalYYYYMMDD();
        const saldoVesOnline = await readConfigValue('saldoVesOnline');
        let masterData = {};

        if (userRole === 'master') {
            const [costo, ventas, tasas, saldos] = await Promise.all([
                new Promise(resolve => calcularCostoClpPorVes(hoy, (e, costo) => resolve({ costoClpPorVes: costo || 0 }))),
                new Promise(resolve => {
                    db.get(`SELECT IFNULL(SUM(monto_clp),0) as totalClpEnviado, IFNULL(SUM(monto_ves),0) as totalVesEnviado, IFNULL(SUM(costo_clp),0) as costoTotalClp FROM operaciones WHERE date(fecha)=date(?)`, [hoy], (e, rowOps) => {
                        if (e) { console.error(e); return resolve({totalClpEnviadoDia: 0, totalVesEnviadoDia: 0, costoTotalClp: 0}); }
                        resolve({ totalClpEnviadoDia: rowOps.totalClpEnviado, totalVesEnviadoDia: rowOps.totalVesEnviado, costoTotalClp: rowOps.costoTotalClp });
                    });
                }),
                new Promise(resolve => {
                    getAvgPurchaseRate(hoy, (err, rateHoy) => {
                        // Si hay tasa hoy, usarla
                        if (!err && rateHoy > 0) return resolve({ tasaCompraPromedio: rateHoy });
                        
                        // Si no hay tasa hoy, buscar la última tasa histórica
                        db.get(`SELECT tasa_clp_ves FROM compras WHERE date(fecha) <= date(?) ORDER BY fecha DESC, id DESC LIMIT 1`, 
                            [hoy], 
                            (errLast, lastPurchase) => {
                                if (errLast || !lastPurchase || !lastPurchase.tasa_clp_ves) {
                                    return resolve({ tasaCompraPromedio: 0 });
                                }
                                // tasa_clp_ves ya está en formato VES/CLP, usar directamente
                                resolve({ tasaCompraPromedio: lastPurchase.tasa_clp_ves });
                            }
                        );
                    });
                }),
                new Promise(resolve => {
                    db.all(`SELECT clave, valor FROM configuracion WHERE clave IN ('capitalInicialClp', 'totalGananciaAcumuladaClp')`, (e, rows) => {
                        const result = {};
                        rows?.forEach(r => result[r.clave] = Number(r.valor) || 0);
                        resolve(result);
                    });
                }),
            ]);

            const { costoClpPorVes } = costo;
            const { totalClpEnviadoDia, totalVesEnviadoDia, costoTotalClp } = ventas;
            const { tasaCompraPromedio } = tasas;
            const { capitalInicialClp = 0, totalGananciaAcumuladaClp = 0 } = saldos;
            const tasaVentaPromedio = totalVesEnviadoDia > 0 ? totalVesEnviadoDia / totalClpEnviadoDia : 0;
            const gananciaBrutaDia = totalClpEnviadoDia - costoTotalClp;
            const comisionDelDia = totalClpEnviadoDia * 0.003;
            const gananciaNetaDelDia = gananciaBrutaDia - comisionDelDia;
            const capitalTotalClp = capitalInicialClp + totalGananciaAcumuladaClp;
            const saldoDisponibleClp = tasaCompraPromedio > 0 ? (saldoVesOnline / tasaCompraPromedio) : 0;

            masterData = {
                totalClpEnviado: totalClpEnviadoDia,
                gananciaBruta: gananciaNetaDelDia,
                margenBruto: totalClpEnviadoDia ? (gananciaNetaDelDia / totalClpEnviadoDia) * 100 : 0,
                tasaCompraPromedio,
                tasaVentaPromedio,
                capitalInicialClp,
                totalGananciaAcumuladaClp,
                capitalTotalClp, 
                saldoDisponibleClp,
            };
        }
        res.json({ saldoVesOnline, ...masterData });
    } catch (e) {
        console.error("Error en GET /api/dashboard:", e);
        res.status(500).json({ message: 'Error al obtener datos del dashboard.' });
    }
});

app.get('/api/ganancia-mensual', apiAuth, onlyMaster, (req, res) => {
    const { mes } = req.query; // Formato: YYYY-MM
    
    if (!mes || !/^\d{4}-\d{2}$/.test(mes)) {
        return res.status(400).json({ message: 'Formato de mes inválido. Use YYYY-MM' });
    }
    
    const inicioMes = `${mes}-01`;
    const [year, month] = mes.split('-');
    const siguienteMes = month === '12' ? `${parseInt(year) + 1}-01-01` : `${year}-${String(parseInt(month) + 1).padStart(2, '0')}-01`;
    
    db.get(
        `SELECT 
            IFNULL(SUM(monto_clp - costo_clp - (monto_clp * 0.003)), 0) as gananciaNeta
        FROM operaciones 
        WHERE date(fecha) >= date(?) AND date(fecha) < date(?)`,
        [inicioMes, siguienteMes],
        (err, row) => {
            if (err) {
                console.error('Error al calcular ganancia mensual:', err);
                return res.status(500).json({ message: 'Error al calcular ganancia mensual' });
            }
            res.json({ 
                mes,
                gananciaMensual: row.gananciaNeta || 0 
            });
        }
    );
});

// Endpoint de análisis de crecimiento día a día
app.get('/api/analisis/crecimiento', apiAuth, (req, res) => {
    const { fecha } = req.query;
    
    if (!fecha || !/^\d{4}-\d{2}-\d{2}$/.test(fecha)) {
        return res.status(400).json({ message: 'Formato de fecha inválido. Use YYYY-MM-DD' });
    }

    // Calcular la fecha del mes anterior (mismo día)
    const fechaObj = new Date(fecha + 'T00:00:00');
    const fechaPreviaObj = new Date(fechaObj);
    fechaPreviaObj.setMonth(fechaPreviaObj.getMonth() - 1);
    
    // Si el día no existe en el mes anterior (ej: 31 de marzo -> 28/29 feb), ajustar
    if (fechaPreviaObj.getDate() !== fechaObj.getDate()) {
        fechaPreviaObj.setDate(0); // Último día del mes anterior
    }
    
    const fechaPrevia = fechaPreviaObj.toISOString().slice(0, 10);

    const obtenerMetricas = (fechaTarget) => {
        return new Promise((resolve, reject) => {
            db.get(`
                SELECT 
                    COUNT(*) as num_operaciones,
                    COUNT(DISTINCT cliente_id) as clientes_unicos,
                    IFNULL(SUM(monto_clp), 0) as ventas_clp,
                    IFNULL(SUM(monto_ves), 0) as ves_enviados,
                    IFNULL(AVG(monto_clp), 0) as ticket_promedio,
                    IFNULL(SUM(costo_clp), 0) as costo_total,
                    IFNULL(SUM(comision_ves), 0) as comision_ves,
                    IFNULL(AVG(tasa), 0) as tasa_promedio,
                    IFNULL(SUM(monto_clp - costo_clp), 0) as ganancia_bruta,
                    IFNULL(SUM(monto_clp) * 0.003, 0) as comision_total
                FROM operaciones 
                WHERE date(fecha) = date(?)
            `, [fechaTarget], (err, row) => {
                if (err) return reject(err);
                
                // Calcular clientes recurrentes (que ya habían operado antes de esta fecha)
                db.get(`
                    SELECT COUNT(DISTINCT o1.cliente_id) as clientes_recurrentes
                    FROM operaciones o1
                    WHERE date(o1.fecha) = date(?)
                    AND EXISTS (
                        SELECT 1 FROM operaciones o2 
                        WHERE o2.cliente_id = o1.cliente_id 
                        AND date(o2.fecha) < date(?)
                    )
                `, [fechaTarget, fechaTarget], (err2, recRow) => {
                    if (err2) return reject(err2);
                    
                    const gananciaBruta = row.ganancia_bruta || 0;
                    const comisionTotal = row.comision_total || 0;
                    const gananciaNeta = gananciaBruta - comisionTotal;
                    
                    const metricas = {
                        num_operaciones: row.num_operaciones || 0,
                        clientes_unicos: row.clientes_unicos || 0,
                        ventas_clp: row.ventas_clp || 0,
                        ves_enviados: row.ves_enviados || 0,
                        ticket_promedio: row.ticket_promedio || 0,
                        costo_total: row.costo_total || 0,
                        comision_ves: row.comision_ves || 0,
                        tasa_promedio: row.tasa_promedio || 0,
                        ganancia_bruta: gananciaNeta,
                        margen_ganancia: row.ventas_clp > 0 ? (gananciaNeta / row.ventas_clp) * 100 : 0,
                        clientes_recurrentes: recRow.clientes_recurrentes || 0
                    };
                    
                    resolve(metricas);
                });
            });
        });
    };

    Promise.all([
        obtenerMetricas(fecha),
        obtenerMetricas(fechaPrevia)
    ])
    .then(([actual, previo]) => {
        res.json({
            fecha_actual: fecha,
            fecha_previa: fechaPrevia,
            actual,
            previo
        });
    })
    .catch(error => {
        console.error('Error en análisis de crecimiento:', error);
        res.status(500).json({ message: 'Error al generar análisis de crecimiento' });
    });
});

app.get('/api/monthly-sales', apiAuth, (req, res) => {
  const user = req.session.user;
  const inicioMes = inicioMesLocalYYYYMMDD();
  let sql = `SELECT IFNULL(SUM(monto_clp), 0) AS totalClpMensual FROM operaciones WHERE date(fecha) >= date(?)`;
  const params = [inicioMes];
  if (user.role !== 'master') { sql += ` AND usuario_id = ?`; params.push(user.id); }
  db.get(sql, params, (err, row) => {
    if (err) return res.status(500).json({ message: 'Error al leer ventas mensuales' });
    res.json({ totalClpMensual: row.totalClpMensual });
  });
});

app.get('/api/operaciones', apiAuth, (req, res) => {
  const user = req.session.user;
  let sql = `SELECT op.*, u.username AS operador, c.nombre AS cliente_nombre FROM operaciones op JOIN usuarios u ON op.usuario_id = u.id JOIN clientes c ON op.cliente_id = c.id`;
  const params = [];
  const where = [];
  if (user.role !== 'master') {
    where.push('op.usuario_id = ?');
    params.push(user.id);
  }
  if (!req.query.historico) {
    where.push("date(op.fecha) = date('now','localtime')");
  }
  if (req.query.startDate) { where.push(`date(op.fecha) >= date(?)`); params.push(req.query.startDate); }
  if (req.query.endDate)   { where.push(`date(op.fecha) <= date(?)`); params.push(req.query.endDate); }
  if (req.query.cliente)   { where.push(`c.nombre LIKE ?`); params.push(`%${req.query.cliente}%`); }
  if (user.role === 'master' && req.query.operador) {
    where.push(`u.username LIKE ?`); params.push(`%${req.query.operador}%`);
  }
  if (where.length) {
    sql += ' WHERE ' + where.join(' AND ');
  }
  sql += ` ORDER BY op.id DESC`;
  db.all(sql, params, (err, rows) => {
    if (err) {
      console.error("SQL Error en GET /api/operaciones:", err.message);
      return res.status(500).json({ message: 'Error al leer operaciones' });
    }
    res.json(rows || []);
  });
});

app.post('/api/operaciones', apiAuth, (req, res) => {
  const { cliente_nombre, monto_clp, monto_ves, tasa, observaciones, fecha, numero_recibo } = req.body;
  if (!numero_recibo || !cliente_nombre || !monto_clp || !tasa) {
    return res.status(400).json({ message: 'Faltan campos obligatorios.' });
  }
  const fechaGuardado = fecha && /^\d{4}-\d{2}-\d{2}$/.test(fecha) ? fecha : hoyLocalYYYYMMDD();
  const montoVesNum = Number(monto_ves || 0);
  const montoClpNum = Number(monto_clp || 0);
  const comisionVes = montoVesNum * 0.003;
  const vesTotalDescontar = montoVesNum + comisionVes; 
  readConfigValue('saldoVesOnline').then(saldoVes => {
      if (vesTotalDescontar > saldoVes) {
        return res.status(400).json({ message: 'Saldo VES online insuficiente.' });
      }
      // Normalizar nombre del cliente: Title Case y sin acentos
      const nombreNormalizado = cliente_nombre.trim()
          .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // Eliminar acentos
          .split(/\s+/)
          .map(palabra => palabra.charAt(0).toUpperCase() + palabra.slice(1).toLowerCase())
          .join(' ');
      
      console.log(`\n📝 Nueva operación - Usuario: ${req.session.user.username}, Cliente: ${nombreNormalizado}, Monto: ${montoClpNum} CLP`);
      
      const findOrCreateCliente = new Promise((resolve, reject) => {
          // Buscar cliente existente comparando sin acentos
          db.all(`SELECT id, nombre FROM clientes`, [], (err, clientes) => {
              if (err) return reject(new Error('Error al buscar cliente.'));
              
              // Buscar coincidencia sin acentos
              const clienteExistente = clientes.find(c => {
                  const nombreSinAcentos = c.nombre.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
                  return nombreSinAcentos.toLowerCase() === nombreNormalizado.toLowerCase();
              });
              
              if (clienteExistente) return resolve(clienteExistente.id);
              
              // NO crear cliente nuevo - retornar error
              return reject(new Error('El cliente no existe. Debe registrarlo primero en la sección de Gestión de Clientes.'));
          });
      });
      
      const getCosto = new Promise((resolve, reject) => {
          calcularCostoClpPorVes(fechaGuardado, (err, costo) => {
              if (err) return reject(new Error('Error al calcular costo.'));
              if (!costo || costo === 0) return reject(new Error('No se pudo determinar el costo de la operación. Registre una compra.'));
              resolve(costo);
          });
      });

      Promise.all([findOrCreateCliente, getCosto])
        .then(([cliente_id, costoClpPorVes]) => {
            const costoVesEnviadoClp = montoVesNum * costoClpPorVes;
            const gananciaBruta = montoClpNum - costoVesEnviadoClp;
            const comisionClp = montoClpNum * 0.003;
            const gananciaNeta = gananciaBruta - comisionClp;
            db.run(`INSERT INTO operaciones(usuario_id,cliente_id,fecha,monto_clp,monto_ves,tasa,observaciones,costo_clp,comision_ves,numero_recibo) VALUES (?,?,?,?,?,?,?,?,?,?)`,
              [req.session.user.id, cliente_id, fechaGuardado, montoClpNum, montoVesNum, Number(tasa || 0), observaciones || '', costoVesEnviadoClp, comisionVes, numero_recibo],
              function (err) {
                if (err) {
                    if (err.message.includes('UNIQUE constraint failed')) return res.status(400).json({ message: 'Error: El número de recibo ya existe.' });
                    return res.status(500).json({ message: 'Error inesperado al guardar la operación.' });
                }
                db.run(`UPDATE configuracion SET valor = CAST(valor AS REAL) - ? WHERE clave = 'saldoVesOnline'`, [vesTotalDescontar]);
                db.run(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'totalGananciaAcumuladaClp'`, [gananciaNeta]);
                
                console.log(`✅ Operación #${numero_recibo} registrada exitosamente`);
                console.log(`   Cliente ID: ${cliente_id}, Monto: ${montoClpNum} CLP → ${montoVesNum} VES`);
                console.log(`   Ganancia Neta: ${gananciaNeta.toFixed(2)} CLP`);
                
                // Verificar si el cliente tiene datos completos y generar alerta si faltan
                db.get(`SELECT nombre, rut, email, telefono FROM clientes WHERE id = ?`, [cliente_id], (errCliente, cliente) => {
                    if (!errCliente && cliente) {
                        const datosFaltantes = [];
                        if (!cliente.rut || cliente.rut.trim() === '') datosFaltantes.push('RUT');
                        if (!cliente.email || cliente.email.trim() === '') datosFaltantes.push('Email');
                        if (!cliente.telefono || cliente.telefono.trim() === '') datosFaltantes.push('Teléfono');
                        
                        if (datosFaltantes.length > 0) {
                            console.log(`\n⚠️  ALERTA: Cliente "${cliente.nombre}" tiene datos incompletos!`);
                            console.log(`   Faltan: ${datosFaltantes.join(', ')}`);
                            console.log(`   Se creará notificación para el operador\n`);
                            
                            const mensaje = `⚠️ Cliente "${cliente.nombre}" realizó una operación pero le faltan datos: ${datosFaltantes.join(', ')}. Por favor actualizar su información.`;
                            const fechaCreacion = new Date().toISOString();
                            
                            // Crear notificación para el operador que registró la operación
                            db.run(
                                `INSERT INTO notificaciones(usuario_id, tipo, titulo, mensaje, fecha_creacion) VALUES (?, ?, ?, ?, ?)`,
                                [req.session.user.id, 'alerta', 'Datos de cliente incompletos', mensaje, fechaCreacion],
                                (errNot) => {
                                    if (errNot) console.error('❌ Error al crear notificación de datos incompletos:', errNot);
                                    else console.log(`✅ Notificación creada para usuario ID ${req.session.user.id}`);
                                }
                            );
                        } else {
                            console.log(`✅ Cliente "${cliente.nombre}" tiene datos completos\n`);
                        }
                    }
                });
                
                res.status(201).json({ id: this.lastID, message: 'Operación registrada con éxito.' });
              }
            );
        })
        .catch(error => {
            console.error("Error en la promesa de operación:", error.message);
            res.status(500).json({ message: error.message || 'Error al procesar la operación.' });
        });
  }).catch(e => {
      console.error("Error validando saldo VES:", e);
      res.status(500).json({ message: 'Error al obtener saldo VES.' });
  });
});

app.put('/api/operaciones/:id', apiAuth, (req, res) => {
    const operacionId = req.params.id;
    const user = req.session.user;
    const { cliente_nombre, monto_clp, tasa, observaciones, fecha, numero_recibo } = req.body;
    if (!numero_recibo) return res.status(400).json({ message: 'El número de recibo es obligatorio.' });
    
    db.get('SELECT id FROM operaciones WHERE numero_recibo = ? AND id != ?', [numero_recibo, operacionId], (err, existing) => {
        if (err) return res.status(500).json({ message: 'Error de base de datos al verificar recibo.' });
        if (existing) return res.status(400).json({ message: 'El número de recibo ya está en uso por otra operación.' });
        
        db.get('SELECT * FROM operaciones WHERE id = ?', [operacionId], (err, opOriginal) => {
            if (err || !opOriginal) return res.status(404).json({ message: 'Operación no encontrada.' });
            
            const esMaster = user.role === 'master';
            const esSuOperacion = opOriginal.usuario_id === user.id;
            const esDeHoy = opOriginal.fecha === hoyLocalYYYYMMDD();
            if (!esMaster && !(esSuOperacion && esDeHoy)) return res.status(403).json({ message: 'No tienes permiso para editar esta operación.' });

            calcularCostoClpPorVes(fecha, (e, costoClpPorVes) => {
                if (e || !costoClpPorVes || costoClpPorVes === 0) return res.status(500).json({ message: 'Error al recalcular el costo. Verifique que existan compras registradas.' });
                
                db.get(`SELECT id FROM clientes WHERE LOWER(nombre) = LOWER(?)`, [cliente_nombre], (err, cliente) => {
                    if (err || !cliente) return res.status(400).json({ message: 'Cliente no encontrado.' });
                    
                    const gananciaNetaOriginal = (opOriginal.monto_clp - opOriginal.costo_clp) - (opOriginal.monto_clp * 0.003);
                    const vesTotalOriginal = opOriginal.monto_ves + opOriginal.comision_ves;
                    const montoClpNuevo = Number(monto_clp || 0);
                    const tasaNueva = Number(tasa || 0);
                    const montoVesNuevo = montoClpNuevo * tasaNueva;
                    const costoClpNuevo = montoVesNuevo * costoClpPorVes;
                    const comisionVesNuevo = montoVesNuevo * 0.003;
                    const gananciaNetaNueva = (montoClpNuevo - costoClpNuevo) - (montoClpNuevo * 0.003);
                    const vesTotalNuevo = montoVesNuevo + comisionVesNuevo;
                    const deltaGanancia = gananciaNetaNueva - gananciaNetaOriginal;
                    const deltaVes = vesTotalOriginal - vesTotalNuevo;
                    
                    db.serialize(() => {
                        db.run('BEGIN TRANSACTION');
                        db.run(`UPDATE operaciones SET cliente_id=?, fecha=?, monto_clp=?, monto_ves=?, tasa=?, observaciones=?, costo_clp=?, comision_ves=?, numero_recibo=? WHERE id = ?`,
                            [cliente.id, fecha, montoClpNuevo, montoVesNuevo, tasaNueva, observaciones || '', costoClpNuevo, comisionVesNuevo, numero_recibo, operacionId]);
                        db.run(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'totalGananciaAcumuladaClp'`, [deltaGanancia]);
                        db.run(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoVesOnline'`, [deltaVes], (err) => {
                            if (err) {
                                db.run('ROLLBACK');
                                return res.status(500).json({ message: 'Error al actualizar saldos, se revirtió la operación.' });
                            }
                            db.run('COMMIT');
                            res.json({ message: 'Operación y saldos actualizados con éxito.' });
                        });
                    });
                });
            });
        });
    });
});

app.delete('/api/operaciones/:id', apiAuth, onlyMaster, (req, res) => {
    const operacionId = req.params.id;
    db.get('SELECT * FROM operaciones WHERE id = ?', [operacionId], (err, op) => {
        if (err || !op) return res.status(404).json({ message: 'Operación no encontrada.' });
        const gananciaBruta = op.monto_clp - op.costo_clp;
        const comisionClp = op.monto_clp * 0.003;
        const gananciaNetaARevertir = gananciaBruta - comisionClp;
        const vesTotalARevertir = op.monto_ves + op.comision_ves;
        db.serialize(() => {
            db.run('BEGIN TRANSACTION');
            db.run('DELETE FROM operaciones WHERE id = ?', [operacionId]);
            db.run(`UPDATE configuracion SET valor = CAST(valor AS REAL) - ? WHERE clave = 'totalGananciaAcumuladaClp'`, [gananciaNetaARevertir]);
            db.run(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoVesOnline'`, [vesTotalARevertir], (err) => {
                if (err) {
                    db.run('ROLLBACK');
                    return res.status(500).json({ message: 'Error al revertir saldos, se canceló el borrado.' });
                }
                db.run('COMMIT');
                res.json({ message: 'Operación borrada y saldos revertidos con éxito.' });
            });
        });
    });
});

// ✅ NUEVO ENDPOINT PARA RECALCULAR COSTOS
app.post('/api/operaciones/recalculate-costs', apiAuth, onlyMaster, async (req, res) => {
    const { ids } = req.body;
    if (!ids || !Array.isArray(ids) || ids.length === 0) {
        return res.status(400).json({ message: 'Se requiere una lista de IDs de operaciones.' });
    }

    try {
        await dbRun('BEGIN TRANSACTION');

        const placeholders = ids.map(() => '?').join(',');
        const opsOriginales = await dbAll(`SELECT * FROM operaciones WHERE id IN (${placeholders})`, ids);
        
        let gananciaAcumuladaDelta = 0;
        let recalculadas = 0;

        for (const op of opsOriginales) {
            const costoClpPorVes = await new Promise((resolve, reject) => {
                calcularCostoClpPorVes(op.fecha, (err, costo) => err ? reject(err) : resolve(costo));
            });

            if (costoClpPorVes > 0) {
                const costoClpOriginal = op.costo_clp;
                const gananciaNetaOriginal = (op.monto_clp - costoClpOriginal) - (op.monto_clp * 0.003);

                const nuevoCostoClp = op.monto_ves * costoClpPorVes;
                
                if (Math.abs(nuevoCostoClp - costoClpOriginal) > 0.01) { // Solo actualizar si hay un cambio significativo
                    await dbRun('UPDATE operaciones SET costo_clp = ? WHERE id = ?', [nuevoCostoClp, op.id]);
                    
                    const nuevaGananciaNeta = (op.monto_clp - nuevoCostoClp) - (op.monto_clp * 0.003);
                    gananciaAcumuladaDelta += (nuevaGananciaNeta - gananciaNetaOriginal);
                    recalculadas++;
                }
            }
        }

        if (recalculadas > 0) {
            await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'totalGananciaAcumuladaClp'`, [gananciaAcumuladaDelta]);
        }

        await dbRun('COMMIT');
        res.json({ message: `Recalculación completada. ${recalculadas} operaciones actualizadas.`, totalAjusteGanancia: gananciaAcumuladaDelta });

    } catch (error) {
        await dbRun('ROLLBACK');
        console.error("Error recalculando costos:", error);
        res.status(500).json({ message: 'Error en el servidor al recalcular los costos.' });
    }
});


app.get('/api/operaciones/export', apiAuth, (req, res) => {
  const user = req.session.user;
  let sql = `SELECT op.fecha, op.numero_recibo, u.username AS operador, c.nombre AS cliente, op.monto_clp, op.tasa, op.monto_ves, op.observaciones, op.costo_clp, op.comision_ves FROM operaciones op JOIN usuarios u ON op.usuario_id = u.id JOIN clientes c ON op.cliente_id = c.id`;
  const params = [];
  const where = [];
  if (req.query.startDate) { where.push(`date(op.fecha) >= date(?)`); params.push(req.query.startDate); }
  if (req.query.endDate)   { where.push(`date(op.fecha) <= date(?)`); params.push(req.query.endDate); }
  if (req.query.cliente)   { where.push(`c.nombre LIKE ?`); params.push(`%${req.query.cliente}%`); }
  if (user.role === 'master' && req.query.operador) { where.push(`u.username LIKE ?`); params.push(`%${req.query.operador}%`); }
  if (where.length) sql += ` WHERE ` + where.join(' AND ');
  sql += ` ORDER BY op.fecha DESC, op.id DESC`;
  db.all(sql, params, (err, rows) => {
    if (err) return res.status(500).json({ message: 'Error export' });
    const header = ['Fecha', 'Recibo', 'Operador', 'Cliente', 'Monto CLP', 'Costo CLP', 'Comision VES', 'Tasa', 'Monto VES', 'Obs.'];
    const csvData = rows.map((r) => [r.fecha, r.numero_recibo, `"${r.operador}"`, `"${r.cliente}"`, r.monto_clp, r.costo_clp, r.comision_ves, r.tasa, r.monto_ves, `"${(r.observaciones || '').replace(/"/g, '""')}"`]);
    const csv = [header.join(',')].concat(csvData.map(row => row.join(','))).join('\n');
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="historico_envios.csv"');
    res.send('\uFEFF' + csv);
  });
});

app.get('/api/historico/resumen', apiAuth, onlyMaster, (req, res) => {
    const { startDate, endDate } = req.query;
    let sql = `SELECT c.nombre AS cliente_nombre, IFNULL(SUM(op.monto_clp), 0) AS total_clp_recibido, IFNULL(SUM(op.costo_clp), 0) AS total_costo_clp FROM operaciones op JOIN clientes c ON op.cliente_id = c.id`;
    const params = [];
    const where = [];
    if (startDate) { where.push(`date(op.fecha) >= date(?)`); params.push(startDate); }
    if (endDate) { where.push(`date(op.fecha) <= date(?)`); params.push(endDate); }
    if (where.length) { sql += ` WHERE ` + where.join(' AND '); }
    sql += ` GROUP BY c.nombre ORDER BY total_clp_recibido DESC`;
    db.all(sql, params, (err, rows) => {
        if (err) return res.status(500).json({ message: 'Error al procesar el resumen.' });
        const resumen = rows.map(row => {
            const gananciaBruta = row.total_clp_recibido - row.total_costo_clp;
            const comisionCliente = row.total_clp_recibido * 0.003;
            const gananciaNeta = gananciaBruta - comisionCliente;
            return {
                cliente_nombre: row.cliente_nombre,
                total_clp_recibido: row.total_clp_recibido,
                total_costo_clp: row.total_costo_clp,
                ganancia_bruta: gananciaBruta,
                ganancia_neta: gananciaNeta
            };
        });
        res.json(resumen);
    });
});

app.get('/api/rendimiento/operadores', apiAuth, onlyMaster, (req, res) => {
    const { startDate, endDate } = req.query;
    let sql = `SELECT u.username AS operador_nombre, IFNULL(COUNT(op.id), 0) AS total_operaciones, IFNULL(COUNT(DISTINCT op.cliente_id), 0) AS clientes_unicos, IFNULL(SUM(op.monto_clp), 0) AS total_clp_enviado FROM operaciones op JOIN usuarios u ON op.usuario_id = u.id`;
    const params = [];
    const where = [];
    if (startDate) { where.push(`date(op.fecha) >= date(?)`); params.push(startDate); }
    if (endDate) { where.push(`date(op.fecha) <= date(?)`); params.push(endDate); }
    if (where.length) { sql += ` WHERE ` + where.join(' AND '); }
    sql += ` GROUP BY u.username ORDER BY total_clp_enviado DESC`;
    db.all(sql, params, (err, rows) => {
        if (err) return res.status(500).json({ message: 'Error al procesar el reporte.' });
        const rendimiento = rows.map(row => {
            const volumenMillones = row.total_clp_enviado / 1000000;
            const bonificacionUsd = Math.floor(volumenMillones) * 2;
            return { ...row, bonificacion_usd: bonificacionUsd, millones_comisionables: Math.floor(volumenMillones) };
        });
        res.json(rendimiento);
    });
});

// Endpoint para operadores: ver su propio rendimiento del mes actual
app.get('/api/mi-rendimiento', apiAuth, (req, res) => {
    const userId = req.session.user.id;
    
    // Obtener primer y último día del mes actual en formato correcto
    const now = new Date();
    const year = now.getFullYear();
    const month = now.getMonth() + 1;
    const primerDia = `${year}-${String(month).padStart(2, '0')}-01`;
    const ultimoDia = `${year}-${String(month).padStart(2, '0')}-31`;
    
    const sql = `
        SELECT 
            COUNT(op.id) AS total_operaciones,
            COUNT(DISTINCT op.cliente_id) AS clientes_unicos,
            IFNULL(SUM(op.monto_clp), 0) AS total_clp_enviado
        FROM operaciones op
        WHERE op.usuario_id = ?
        AND substr(op.fecha, 1, 7) = ?
    `;
    
    const mesActual = `${year}-${String(month).padStart(2, '0')}`;
    
    db.get(sql, [userId, mesActual], (err, row) => {
        if (err) {
            console.error('Error al obtener rendimiento del operador:', err);
            return res.status(500).json({ message: 'Error al procesar el reporte.' });
        }
        
        const volumenMillones = row.total_clp_enviado / 1000000;
        const bonificacionUsd = Math.floor(volumenMillones) * 2;
        
        res.json({
            total_operaciones: row.total_operaciones,
            clientes_unicos: row.clientes_unicos,
            total_clp_enviado: row.total_clp_enviado,
            millones_comisionables: Math.floor(volumenMillones),
            bonificacion_usd: bonificacionUsd,
            mes: mesActual
        });
    });
});

const readConfig = (clave, cb) => db.get(`SELECT valor FROM configuracion WHERE clave=?`, [clave], (e, row) => cb(e, row ? row.valor : null));

app.get('/api/tasas', apiAuth, (req, res) => {
  const result = {};
  readConfig('tasaNivel1', (e1, v1) => {
    result.tasaNivel1 = v1 ? Number(v1) : null;
    readConfig('tasaNivel2', (e2, v2) => {
      result.tasaNivel2 = v2 ? Number(v2) : null;
      readConfig('tasaNivel3', (e3, v3) => {
        result.tasaNivel3 = v3 ? Number(v3) : null;
        res.json(result);
      });
    });
  });
});

app.post('/api/tasas', apiAuth, onlyMaster, (req, res) => {
  const { tasaNivel1, tasaNivel2, tasaNivel3 } = req.body;
  upsertConfig('tasaNivel1', String(tasaNivel1 ?? ''), () => {
    upsertConfig('tasaNivel2', String(tasaNivel2 ?? ''), () => {
      upsertConfig('tasaNivel3', String(tasaNivel3 ?? ''), () => res.json({ ok: true }));
    });
  });
});

app.post('/api/config/capital', apiAuth, onlyMaster, (req, res) => {
    const { capitalInicialClp } = req.body;
    upsertConfig('capitalInicialClp', String(Number(capitalInicialClp) || 0), (err) => {
        if (err) return res.status(500).json({ message: 'Error al actualizar el capital inicial.' });
        res.json({ message: 'Capital inicial actualizado con éxito.' });
    });
});

app.get('/api/config/capital', apiAuth, onlyMaster, (req, res) => {
    Promise.all(['capitalInicialClp', 'saldoInicialVes', 'capitalCostoVesPorClp'].map(readConfigValue))
        .then(([capitalInicialClp, saldoInicialVes, costoVesPorClp]) => res.json({ capitalInicialClp, saldoInicialVes, costoVesPorClp }))
        .catch(e => res.status(500).json({ message: 'Error al leer configuración' }));
});

app.post('/api/config/ajustar-saldo-ves', apiAuth, onlyMaster, (req, res) => {
    const { nuevoSaldoVes } = req.body;
    const saldo = Number(nuevoSaldoVes);
    if (isNaN(saldo) || saldo < 0) {
        return res.status(400).json({ message: 'El valor del saldo debe ser un número positivo.' });
    }
    upsertConfig('saldoVesOnline', String(saldo), (err) => {
        if (err) return res.status(500).json({ message: 'Error al actualizar el saldo.' });
        res.json({ message: 'Saldo VES Online actualizado con éxito.' });
    });
});

app.get('/api/usuarios', apiAuth, onlyMaster, (req, res) => {
    db.all(`SELECT id, username, role FROM usuarios`, [], (err, rows) => {
        if (err) return res.status(500).json({ message: 'Error al listar usuarios' });
        res.json(rows || []);
    });
});

app.put('/api/usuarios/:id', apiAuth, onlyMaster, async (req, res) => {
    const { username, password, role } = req.body;
    let sql = `UPDATE usuarios SET username = ?, role = ? WHERE id = ?`;
    let params = [username, role, req.params.id];
    if (password) {
        const hash = await bcrypt.hash(password, 10);
        sql = `UPDATE usuarios SET username = ?, role = ?, password = ? WHERE id = ?`;
        params = [username, role, hash, req.params.id];
    }
    db.run(sql, params, function (e) {
        if (e) return res.status(500).json({ message: 'Error al actualizar usuario' });
        res.json({ message: 'Usuario actualizado con éxito.' });
    });
});

app.post('/api/create-operator', apiAuth, onlyMaster, async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ message: 'Datos incompletos' });
  const hash = await bcrypt.hash(password, 10);
  db.run(`INSERT INTO usuarios(username,password,role) VALUES (?,?,?)`, [username, hash, 'operador'], (e) => {
      if (e) return res.status(400).json({ message: 'No se pudo crear (¿duplicado?)' });
      res.json({ message: 'Operador creado' });
    }
  );
});

app.get('/api/compras', apiAuth, onlyMaster, (req, res) => {
    db.all(`SELECT id, usuario_id, clp_invertido, ves_obtenido, tasa_clp_ves, fecha FROM compras ORDER BY id DESC`, [], (err, rows) => {
        if (err) return res.status(500).json({ message: 'Error al listar compras' });
        res.json(rows || []);
    });
});

app.post('/api/compras', apiAuth, onlyMaster, (req, res) => {
    const { clp_invertido, ves_obtenido } = req.body;
    const clp = Number(clp_invertido || 0);
    const ves = Number(ves_obtenido || 0);
    if (clp <= 0 || ves <= 0) return res.status(400).json({ message: 'Los montos deben ser mayores a cero.' });
    const tasa = ves / clp;
    db.run(`INSERT INTO compras(usuario_id, clp_invertido, ves_obtenido, tasa_clp_ves, fecha) VALUES (?, ?, ?, ?, ?)`,
        [req.session.user.id, clp, ves, tasa, hoyLocalYYYYMMDD()],
        function(err) {
            if (err) {
                console.error("Error en POST /api/compras:", err.message);
                return res.status(500).json({ message: 'Error al guardar la compra.', error: err.message });
            }
            db.run(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoVesOnline'`, [ves], (updateErr) => {
                if(updateErr) return res.status(500).json({ message: 'Compra guardada, pero falló la actualización del saldo.' });
                res.json({ message: 'Compra registrada con éxito.' });
            });
        }
    );
});

app.put('/api/compras/:id', apiAuth, onlyMaster, (req, res) => {
    const compraId = req.params.id;
    const { clp_invertido, ves_obtenido, fecha } = req.body;
    db.get('SELECT * FROM compras WHERE id = ?', [compraId], (err, compraOriginal) => {
        if (err || !compraOriginal) return res.status(404).json({ message: 'Compra no encontrada.' });
        const vesOriginal = compraOriginal.ves_obtenido;
        const vesNuevo = Number(ves_obtenido || 0);
        const deltaVes = vesNuevo - vesOriginal;
        const clpNuevo = Number(clp_invertido || 0);
        const tasaNueva = clpNuevo > 0 ? vesNuevo / clpNuevo : 0;
        db.serialize(() => {
            db.run('BEGIN TRANSACTION');
            db.run(`UPDATE compras SET clp_invertido = ?, ves_obtenido = ?, tasa_clp_ves = ?, fecha = ? WHERE id = ?`, [clpNuevo, vesNuevo, tasaNueva, fecha, compraId]);
            db.run(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoVesOnline'`, [deltaVes], (err) => {
                if (err) {
                    db.run('ROLLBACK');
                    return res.status(500).json({ message: 'Error al actualizar saldo, se revirtió la operación.' });
                }
                db.run('COMMIT');
                res.json({ message: 'Compra y saldo actualizados con éxito.' });
            });
        });
    });
});

app.delete('/api/compras/:id', apiAuth, onlyMaster, (req, res) => {
    const compraId = req.params.id;
    db.get('SELECT * FROM compras WHERE id = ?', [compraId], (err, compra) => {
        if (err || !compra) return res.status(404).json({ message: 'Compra no encontrada.' });
        const vesARevertir = compra.ves_obtenido;
        db.serialize(() => {
            db.run('BEGIN TRANSACTION');
            db.run('DELETE FROM compras WHERE id = ?', [compraId]);
            db.run(`UPDATE configuracion SET valor = CAST(valor AS REAL) - ? WHERE clave = 'saldoVesOnline'`, [vesARevertir], (err) => {
                if (err) {
                    db.run('ROLLBACK');
                    return res.status(500).json({ message: 'Error al revertir saldo, se canceló el borrado.' });
                }
                db.run('COMMIT');
                res.json({ message: 'Compra borrada y saldo revertido con éxito.' });
            });
        });
    });
});

// =================================================================
// INICIO: ENDPOINTS DE ANALYTICS AVANZADO
// =================================================================

// 📊 ENDPOINT 1: Análisis de comportamiento de clientes
app.get('/api/analytics/clientes/comportamiento', apiAuth, onlyMaster, async (req, res) => {
    try {
        const clientes = await dbAll(`
            SELECT 
                c.id,
                c.nombre,
                c.fecha_creacion,
                COUNT(o.id) as total_operaciones,
                SUM(o.monto_clp) as volumen_total_clp,
                AVG(o.monto_clp) as ticket_promedio,
                MAX(o.fecha) as ultima_operacion,
                MIN(o.fecha) as primera_operacion
            FROM clientes c
            LEFT JOIN operaciones o ON c.id = o.cliente_id
            GROUP BY c.id
            ORDER BY volumen_total_clp DESC
        `);

        const hoy = new Date();
        const analisis = clientes.map(c => {
            const ultimaOp = c.ultima_operacion ? new Date(c.ultima_operacion) : null;
            const primeraOp = c.primera_operacion ? new Date(c.primera_operacion) : null;
            const diasDesdeUltimo = ultimaOp ? Math.floor((hoy - ultimaOp) / (1000 * 60 * 60 * 24)) : null;
            
            let frecuencia = 'Sin actividad';
            let tendencia = 'estable';
            
            if (c.total_operaciones > 0 && primeraOp && ultimaOp) {
                // Cliente con una sola operación
                if (c.total_operaciones === 1) {
                    frecuencia = 'Única operación';
                } else {
                    const diasActivo = Math.max(1, Math.floor((ultimaOp - primeraOp) / (1000 * 60 * 60 * 24)));
                    const promedioDias = diasActivo / Math.max(1, c.total_operaciones - 1);
                    
                    // Diario requiere al menos 5 operaciones y promedio muy bajo
                    if (c.total_operaciones >= 5 && promedioDias <= 2) frecuencia = 'Diario';
                    else if (promedioDias <= 7) frecuencia = 'Semanal';
                    else if (promedioDias <= 30) frecuencia = 'Mensual';
                    else frecuencia = 'Esporádico';
                }
                
                // Análisis de tendencia: comparar últimos 30 días vs 30-60 días atrás
                const hace30 = new Date(hoy.getTime() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
                const hace60 = new Date(hoy.getTime() - 60 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
                
                db.get(`SELECT COUNT(*) as recientes FROM operaciones WHERE cliente_id = ? AND fecha >= ?`, [c.id, hace30], (e1, r1) => {
                    db.get(`SELECT COUNT(*) as anteriores FROM operaciones WHERE cliente_id = ? AND fecha >= ? AND fecha < ?`, [c.id, hace60, hace30], (e2, r2) => {
                        if (r1 && r2) {
                            if (r1.recientes > r2.anteriores * 1.2) tendencia = 'creciente';
                            else if (r1.recientes < r2.anteriores * 0.8) tendencia = 'decreciente';
                        }
                    });
                });
            }
            
            return {
                id: c.id,
                nombre: c.nombre,
                fecha_registro: c.fecha_creacion,
                total_operaciones: c.total_operaciones || 0,
                volumen_total_clp: c.volumen_total_clp || 0,
                ticket_promedio: c.ticket_promedio || 0,
                frecuencia,
                tendencia,
                dias_desde_ultimo: diasDesdeUltimo,
                ultima_operacion: c.ultima_operacion
            };
        });

        res.json(analisis);
    } catch (error) {
        console.error('Error en análisis de comportamiento:', error);
        res.status(500).json({ message: 'Error al analizar comportamiento de clientes' });
    }
});

// 🚨 ENDPOINT 2: Alertas y clientes en riesgo
app.get('/api/analytics/clientes/alertas', apiAuth, onlyMaster, async (req, res) => {
    try {
        const alertas = [];
        const hoy = new Date();
        const fechaHoy = hoyLocalYYYYMMDD();
        
        // Clientes inactivos (30-60 días)
        const inactivos = await dbAll(`
            SELECT c.id, c.nombre, MAX(o.fecha) as ultima_operacion, COUNT(o.id) as total_ops
            FROM clientes c
            JOIN operaciones o ON c.id = o.cliente_id
            GROUP BY c.id
            HAVING julianday('now') - julianday(MAX(o.fecha)) BETWEEN 30 AND 60
        `);
        
        for (const c of inactivos) {
            const dias = Math.floor((hoy - new Date(c.ultima_operacion)) / (1000 * 60 * 60 * 24));
            
            // Verificar si ya existe alerta activa
            const alertaExistente = await dbGet(`
                SELECT * FROM alertas 
                WHERE cliente_id = ? AND tipo = 'inactivo' AND activa = 1
            `, [c.id]);
            
            if (!alertaExistente) {
                // Crear nueva alerta
                const result = await dbRun(`
                    INSERT INTO alertas(cliente_id, tipo, severidad, dias_inactivo, ultima_operacion, fecha_creacion)
                    VALUES (?, 'inactivo', 'warning', ?, ?, ?)
                `, [c.id, dias, c.ultima_operacion, fechaHoy]);
                
                alertas.push({
                    id: result.lastID,
                    tipo: 'inactivo',
                    severidad: 'warning',
                    cliente_id: c.id,
                    cliente_nombre: c.nombre,
                    mensaje: `Cliente inactivo por ${dias} días`,
                    dias_inactivo: dias,
                    ultima_operacion: c.ultima_operacion,
                    accion_realizada: null
                });
            } else {
                // Retornar alerta existente con acción si existe
                alertas.push({
                    id: alertaExistente.id,
                    tipo: alertaExistente.tipo,
                    severidad: alertaExistente.severidad,
                    cliente_id: c.id,
                    cliente_nombre: c.nombre,
                    mensaje: `Cliente inactivo por ${dias} días`,
                    dias_inactivo: dias,
                    ultima_operacion: c.ultima_operacion,
                    accion_realizada: alertaExistente.accion_realizada,
                    fecha_accion: alertaExistente.fecha_accion
                });
            }
        }
        
        // Clientes críticos (+60 días)
        const criticos = await dbAll(`
            SELECT c.id, c.nombre, MAX(o.fecha) as ultima_operacion
            FROM clientes c
            JOIN operaciones o ON c.id = o.cliente_id
            GROUP BY c.id
            HAVING julianday('now') - julianday(MAX(o.fecha)) > 60
        `);
        
        for (const c of criticos) {
            const dias = Math.floor((hoy - new Date(c.ultima_operacion)) / (1000 * 60 * 60 * 24));
            
            const alertaExistente = await dbGet(`
                SELECT * FROM alertas 
                WHERE cliente_id = ? AND tipo = 'critico' AND activa = 1
            `, [c.id]);
            
            if (!alertaExistente) {
                const result = await dbRun(`
                    INSERT INTO alertas(cliente_id, tipo, severidad, dias_inactivo, ultima_operacion, fecha_creacion)
                    VALUES (?, 'critico', 'danger', ?, ?, ?)
                `, [c.id, dias, c.ultima_operacion, fechaHoy]);
                
                alertas.push({
                    id: result.lastID,
                    tipo: 'critico',
                    severidad: 'danger',
                    cliente_id: c.id,
                    cliente_nombre: c.nombre,
                    mensaje: `Cliente sin actividad por ${dias} días - RIESGO ALTO`,
                    dias_inactivo: dias,
                    ultima_operacion: c.ultima_operacion,
                    accion_realizada: null
                });
            } else {
                alertas.push({
                    id: alertaExistente.id,
                    tipo: alertaExistente.tipo,
                    severidad: alertaExistente.severidad,
                    cliente_id: c.id,
                    cliente_nombre: c.nombre,
                    mensaje: `Cliente sin actividad por ${dias} días - RIESGO ALTO`,
                    dias_inactivo: dias,
                    ultima_operacion: c.ultima_operacion,
                    accion_realizada: alertaExistente.accion_realizada,
                    fecha_accion: alertaExistente.fecha_accion
                });
            }
        }
        
        // Disminución de frecuencia
        const hace30 = new Date(hoy.getTime() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
        const hace60 = new Date(hoy.getTime() - 60 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
        
        const clientesActivos = await dbAll(`
            SELECT DISTINCT o.cliente_id 
            FROM operaciones o
            JOIN clientes c ON o.cliente_id = c.id
            WHERE o.fecha >= ?
            AND julianday('now') - julianday(c.fecha_creacion) > 7
        `, [hace60]);
        
        for (const c of clientesActivos) {
            const recientes = await dbGet(`SELECT COUNT(*) as cnt FROM operaciones WHERE cliente_id = ? AND fecha >= ?`, [c.cliente_id, hace30]);
            const anteriores = await dbGet(`SELECT COUNT(*) as cnt FROM operaciones WHERE cliente_id = ? AND fecha >= ? AND fecha < ?`, [c.cliente_id, hace60, hace30]);
            
            if (anteriores.cnt >= 3 && recientes.cnt < anteriores.cnt * 0.5) {
                const cliente = await dbGet(`SELECT nombre, id FROM clientes WHERE id = ?`, [c.cliente_id]);
                const ultimaOp = await dbGet(`SELECT MAX(fecha) as fecha FROM operaciones WHERE cliente_id = ?`, [c.cliente_id]);
                
                const alertaExistente = await dbGet(`
                    SELECT * FROM alertas 
                    WHERE cliente_id = ? AND tipo = 'disminucion' AND activa = 1
                `, [c.cliente_id]);
                
                if (!alertaExistente) {
                    const result = await dbRun(`
                        INSERT INTO alertas(cliente_id, tipo, severidad, ultima_operacion, fecha_creacion)
                        VALUES (?, 'disminucion', 'warning', ?, ?)
                    `, [c.cliente_id, ultimaOp.fecha, fechaHoy]);
                    
                    alertas.push({
                        id: result.lastID,
                        tipo: 'disminucion',
                        severidad: 'warning',
                        cliente_id: c.cliente_id,
                        cliente_nombre: cliente.nombre,
                        mensaje: `Reducción de actividad: ${anteriores.cnt} ops → ${recientes.cnt} ops`,
                        ops_anterior: anteriores.cnt,
                        ops_reciente: recientes.cnt,
                        accion_realizada: null
                    });
                } else {
                    alertas.push({
                        id: alertaExistente.id,
                        tipo: alertaExistente.tipo,
                        severidad: alertaExistente.severidad,
                        cliente_id: c.cliente_id,
                        cliente_nombre: cliente.nombre,
                        mensaje: `Reducción de actividad: ${anteriores.cnt} ops → ${recientes.cnt} ops`,
                        ops_anterior: anteriores.cnt,
                        ops_reciente: recientes.cnt,
                        accion_realizada: alertaExistente.accion_realizada,
                        fecha_accion: alertaExistente.fecha_accion
                    });
                }
            }
        }
        
        res.json(alertas);
    } catch (error) {
        console.error('Error en alertas:', error);
        res.status(500).json({ message: 'Error al generar alertas' });
    }
});

// 👥 ENDPOINT 3: Análisis de clientes nuevos
app.get('/api/analytics/clientes/nuevos', apiAuth, onlyMaster, async (req, res) => {
    try {
        const { mes } = req.query; // formato: 2025-11
        const mesActual = mes || new Date().toISOString().slice(0, 7);
        
        const nuevos = await dbAll(`
            SELECT 
                c.id,
                c.nombre,
                c.fecha_creacion,
                COUNT(o.id) as total_operaciones,
                SUM(o.monto_clp) as volumen_total
            FROM clientes c
            LEFT JOIN operaciones o ON c.id = o.cliente_id
            WHERE substr(c.fecha_creacion, 1, 7) = ?
            GROUP BY c.id
            ORDER BY c.fecha_creacion DESC
        `, [mesActual]);
        
        const hoy = new Date();
        const analisis = nuevos.map(c => {
            const diasDesdeRegistro = Math.floor((hoy - new Date(c.fecha_creacion)) / (1000 * 60 * 60 * 24));
            const convirtio = c.total_operaciones > 1;
            
            return {
                id: c.id,
                nombre: c.nombre,
                fecha_registro: c.fecha_creacion,
                dias_desde_registro: diasDesdeRegistro,
                total_operaciones: c.total_operaciones || 0,
                volumen_total: c.volumen_total || 0,
                convirtio,
                estado: c.total_operaciones === 0 ? 'sin_actividad' : convirtio ? 'activo' : 'primera_compra'
            };
        });
        
        const resumen = {
            mes: mesActual,
            total_nuevos: nuevos.length,
            con_operaciones: nuevos.filter(c => c.total_operaciones > 0).length,
            tasa_conversion: nuevos.length > 0 ? (nuevos.filter(c => c.total_operaciones > 1).length / nuevos.length * 100).toFixed(1) : 0,
            volumen_total: nuevos.reduce((sum, c) => sum + (c.volumen_total || 0), 0)
        };
        
        res.json({ resumen, clientes: analisis });
    } catch (error) {
        console.error('Error en clientes nuevos:', error);
        res.status(500).json({ message: 'Error al analizar clientes nuevos' });
    }
});

// 🔍 ENDPOINT 4: Detalle profundo de un cliente
app.get('/api/analytics/clientes/detalle/:id', apiAuth, onlyMaster, async (req, res) => {
    try {
        const clienteId = req.params.id;
        
        const cliente = await dbGet(`SELECT * FROM clientes WHERE id = ?`, [clienteId]);
        if (!cliente) return res.status(404).json({ message: 'Cliente no encontrado' });
        
        const operaciones = await dbAll(`
            SELECT fecha, monto_clp, monto_ves, tasa 
            FROM operaciones 
            WHERE cliente_id = ? 
            ORDER BY fecha DESC
        `, [clienteId]);
        
        const stats = await dbGet(`
            SELECT 
                COUNT(*) as total_ops,
                SUM(monto_clp) as volumen_total,
                AVG(monto_clp) as ticket_promedio,
                MIN(fecha) as primera_op,
                MAX(fecha) as ultima_op
            FROM operaciones
            WHERE cliente_id = ?
        `, [clienteId]);
        
        // Historial por mes
        const porMes = await dbAll(`
            SELECT 
                substr(fecha, 1, 7) as mes,
                COUNT(*) as operaciones,
                SUM(monto_clp) as volumen
            FROM operaciones
            WHERE cliente_id = ?
            GROUP BY mes
            ORDER BY mes DESC
        `, [clienteId]);
        
        res.json({
            cliente,
            estadisticas: stats,
            historial_mensual: porMes,
            ultimas_operaciones: operaciones.slice(0, 10)
        });
    } catch (error) {
        console.error('Error en detalle de cliente:', error);
        res.status(500).json({ message: 'Error al obtener detalle del cliente' });
    }
});

// 🎯 ENDPOINT 5: Dashboard de metas
app.get('/api/analytics/metas/dashboard', apiAuth, onlyMaster, async (req, res) => {
    try {
        const mesActual = new Date().toISOString().slice(0, 7);
        
        // Obtener o crear meta del mes
        let meta = await dbGet(`SELECT * FROM metas WHERE mes = ?`, [mesActual]);
        if (!meta) {
            await dbRun(`INSERT INTO metas (mes) VALUES (?)`, [mesActual]);
            meta = { mes: mesActual, meta_clientes_activos: 0, meta_nuevos_clientes: 0, meta_volumen_clp: 0, meta_operaciones: 0 };
        }
        
        // Calcular valores actuales
        const stats = await dbGet(`
            SELECT 
                COUNT(DISTINCT cliente_id) as clientes_activos,
                COUNT(*) as total_operaciones,
                SUM(monto_clp) as volumen_total
            FROM operaciones
            WHERE substr(fecha, 1, 7) = ?
        `, [mesActual]);
        
        const nuevosClientes = await dbGet(`
            SELECT COUNT(*) as cnt FROM clientes WHERE substr(fecha_creacion, 1, 7) = ?
        `, [mesActual]);
        
        // Calcular porcentajes
        const calcularProgreso = (actual, meta) => meta > 0 ? Math.round((actual / meta) * 100) : 0;
        
        const dashboard = {
            mes: mesActual,
            metas: {
                clientes_activos: meta.meta_clientes_activos,
                nuevos_clientes: meta.meta_nuevos_clientes,
                volumen_clp: meta.meta_volumen_clp,
                operaciones: meta.meta_operaciones
            },
            actuales: {
                clientes_activos: stats.clientes_activos || 0,
                nuevos_clientes: nuevosClientes.cnt || 0,
                volumen_clp: stats.volumen_total || 0,
                operaciones: stats.total_operaciones || 0
            },
            progreso: {
                clientes_activos: calcularProgreso(stats.clientes_activos, meta.meta_clientes_activos),
                nuevos_clientes: calcularProgreso(nuevosClientes.cnt, meta.meta_nuevos_clientes),
                volumen_clp: calcularProgreso(stats.volumen_total, meta.meta_volumen_clp),
                operaciones: calcularProgreso(stats.total_operaciones, meta.meta_operaciones)
            }
        };
        
        res.json(dashboard);
    } catch (error) {
        console.error('Error en dashboard de metas:', error);
        res.status(500).json({ message: 'Error al obtener dashboard de metas' });
    }
});

// 🎯 ENDPOINT 6: Configurar metas
app.post('/api/analytics/metas/configurar', apiAuth, onlyMaster, async (req, res) => {
    try {
        const { mes, meta_clientes_activos, meta_nuevos_clientes, meta_volumen_clp, meta_operaciones } = req.body;
        const mesConfig = mes || new Date().toISOString().slice(0, 7);
        
        await dbRun(`
            INSERT INTO metas (mes, meta_clientes_activos, meta_nuevos_clientes, meta_volumen_clp, meta_operaciones)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(mes) DO UPDATE SET
                meta_clientes_activos = excluded.meta_clientes_activos,
                meta_nuevos_clientes = excluded.meta_nuevos_clientes,
                meta_volumen_clp = excluded.meta_volumen_clp,
                meta_operaciones = excluded.meta_operaciones
        `, [mesConfig, meta_clientes_activos, meta_nuevos_clientes, meta_volumen_clp, meta_operaciones]);
        
        res.json({ message: 'Metas configuradas correctamente' });
    } catch (error) {
        console.error('Error al configurar metas:', error);
        res.status(500).json({ message: 'Error al configurar metas' });
    }
});

// =================================================================
// FIN: ENDPOINTS DE ANALYTICS AVANZADO
// =================================================================

// =================================================================
// SERVICIO P2P - BINANCE P2P API
// =================================================================

/**
 * Consulta anuncios P2P de Binance
 * @param {string} fiat - Moneda fiat (VES, CLP, etc)
 * @param {string} tradeType - BUY (comprar USDT) o SELL (vender USDT)
 * @param {string[]} payTypes - Métodos de pago específicos (ej: ["Bancamiga"])
 * @param {number} transAmount - Monto mínimo de transacción
 * @returns {Promise<Array>} - Lista de anuncios ordenados por precio
 */
async function consultarBinanceP2P(fiat, tradeType, payTypes = [], transAmount = null) {
    try {
        const payload = {
            page: 1,
            rows: 10,
            fiat: fiat,
            asset: 'USDT',
            tradeType: tradeType,
            publisherType: null
        };

        // Agregar filtro de métodos de pago si se especifican
        if (payTypes && payTypes.length > 0) {
            payload.payTypes = payTypes;
        }

        // Agregar filtro de monto si se especifica
        if (transAmount) {
            payload.transAmount = transAmount;
        }

        const response = await axios.post(
            'https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search',
            payload,
            {
                headers: {
                    'Content-Type': 'application/json',
                    'User-Agent': 'Mozilla/5.0'
                },
                timeout: 10000
            }
        );

        if (response.data && response.data.data) {
            return response.data.data;
        }

        return [];
    } catch (error) {
        console.error(`Error consultando Binance P2P (${fiat} ${tradeType}):`, error.message);
        throw new Error(`No se pudo consultar la API de Binance P2P: ${error.message}`);
    }
}

/**
 * Obtiene la mejor tasa de venta de USDT por VES (Bancamiga)
 * @returns {Promise<number>} - Precio en VES por 1 USDT
 */
async function obtenerTasaVentaVES() {
    try {
        const anuncios = await consultarBinanceP2P('VES', 'SELL', ['Bancamiga'], 50000);
        
        if (!anuncios || anuncios.length === 0) {
            throw new Error('No se encontraron ofertas de venta USDT por VES con Bancamiga');
        }

        // Filtrar por monto mínimo y ordenar por precio (más alto primero = mejor para vender)
        const ofertasValidas = anuncios
            .filter(ad => {
                const minLimit = parseFloat(ad.adv?.minSingleTransAmount || 0);
                return minLimit <= 50000;
            })
            .sort((a, b) => parseFloat(b.adv?.price || 0) - parseFloat(a.adv?.price || 0));

        if (ofertasValidas.length === 0) {
            throw new Error('No hay ofertas válidas con monto mínimo <= 50,000 VES');
        }

        const mejorOferta = ofertasValidas[0];
        const precio = parseFloat(mejorOferta.adv?.price || 0);

        console.log(`✅ Mejor oferta VES: ${precio} VES/USDT (Bancamiga)`);
        return precio;
    } catch (error) {
        console.error('Error obteniendo tasa VES:', error.message);
        throw error;
    }
}

/**
 * Obtiene la mejor tasa de venta de USDT por COP (Bancolombia/Nequi)
 * @returns {Promise<number>} - Precio en COP por 1 USDT
 */
async function obtenerTasaVentaCOP() {
    try {
        const anuncios = await consultarBinanceP2P('COP', 'SELL', ['Bancolombia', 'Nequi'], 40000);
        
        if (!anuncios || anuncios.length === 0) {
            throw new Error('No se encontraron ofertas de venta USDT por COP con Bancolombia/Nequi');
        }

        // Filtrar por monto mínimo y ordenar por precio (más alto primero = mejor para vender)
        const ofertasValidas = anuncios
            .filter(ad => {
                const minLimit = parseFloat(ad.adv?.minSingleTransAmount || 0);
                return minLimit <= 40000;
            })
            .sort((a, b) => parseFloat(b.adv?.price || 0) - parseFloat(a.adv?.price || 0));

        if (ofertasValidas.length === 0) {
            throw new Error('No hay ofertas válidas con monto mínimo <= 40,000 COP');
        }

        const mejorOferta = ofertasValidas[0];
        const precio = parseFloat(mejorOferta.adv?.price || 0);

        console.log(`✅ Mejor oferta COP: ${precio} COP/USDT (Bancolombia/Nequi)`);
        return precio;
    } catch (error) {
        console.error('Error obteniendo tasa COP:', error.message);
        throw error;
    }
}

/**
 * Obtiene la mejor tasa de venta de USDT por PEN (BCP/Yape)
 * @returns {Promise<number>} - Precio en PEN por 1 USDT
 */
async function obtenerTasaVentaPEN() {
    try {
        const anuncios = await consultarBinanceP2P('PEN', 'SELL', ['BCP', 'Yape'], 30);
        
        if (!anuncios || anuncios.length === 0) {
            throw new Error('No se encontraron ofertas de venta USDT por PEN con BCP/Yape');
        }

        // Filtrar por monto mínimo y ordenar por precio (más alto primero = mejor para vender)
        const ofertasValidas = anuncios
            .filter(ad => {
                const minLimit = parseFloat(ad.adv?.minSingleTransAmount || 0);
                return minLimit <= 30;
            })
            .sort((a, b) => parseFloat(b.adv?.price || 0) - parseFloat(a.adv?.price || 0));

        if (ofertasValidas.length === 0) {
            throw new Error('No hay ofertas válidas con monto mínimo <= 30 PEN');
        }

        const mejorOferta = ofertasValidas[0];
        const precio = parseFloat(mejorOferta.adv?.price || 0);

        console.log(`✅ Mejor oferta PEN: ${precio} PEN/USDT (BCP/Yape)`);
        return precio;
    } catch (error) {
        console.error('Error obteniendo tasa PEN:', error.message);
        throw error;
    }
}

/**
 * Obtiene la mejor tasa de venta de USDT por BOB (Banco Ganadero/Economico)
 * @returns {Promise<number>} - Precio en BOB por 1 USDT
 */
async function obtenerTasaVentaBOB() {
    try {
        // Nombres exactos como aparecen en Binance P2P (sin espacios)
        const anuncios = await consultarBinanceP2P('BOB', 'SELL', ['BancoGanadero', 'BancoEconomico'], 100);
        
        if (!anuncios || anuncios.length === 0) {
            throw new Error('No se encontraron ofertas de venta USDT por BOB con Banco Ganadero/Economico');
        }

        // Filtrar por monto mínimo y ordenar por precio (más alto primero = mejor para vender)
        const ofertasValidas = anuncios
            .filter(ad => {
                const minLimit = parseFloat(ad.adv?.minSingleTransAmount || 0);
                return minLimit <= 100;
            })
            .sort((a, b) => parseFloat(b.adv?.price || 0) - parseFloat(a.adv?.price || 0));

        if (ofertasValidas.length === 0) {
            throw new Error('No hay ofertas válidas con monto mínimo <= 100 BOB');
        }

        const mejorOferta = ofertasValidas[0];
        const precio = parseFloat(mejorOferta.adv?.price || 0);

        console.log(`✅ Mejor oferta BOB: ${precio} BOB/USDT (BancoGanadero/BancoEconomico)`);
        return precio;
    } catch (error) {
        console.error('Error obteniendo tasa BOB:', error.message);
        throw error;
    }
}

/**
 * Obtiene la mejor tasa de venta de USDT por ARS (MercadoPago/Brubank/LemonCash)
 * @returns {Promise<number>} - Precio en ARS por 1 USDT
 */
async function obtenerTasaVentaARS() {
    try {
        const anuncios = await consultarBinanceP2P('ARS', 'SELL', ['MercadoPagoNew', 'BancoBrubankNew', 'LemonCash'], 15000);
        
        if (!anuncios || anuncios.length === 0) {
            throw new Error('No se encontraron ofertas de venta USDT por ARS con MercadoPago/Brubank/LemonCash');
        }

        // Filtrar por monto mínimo y ordenar por precio (más alto primero = mejor para vender)
        const ofertasValidas = anuncios
            .filter(ad => {
                const minLimit = parseFloat(ad.adv?.minSingleTransAmount || 0);
                return minLimit <= 15000;
            })
            .sort((a, b) => parseFloat(b.adv?.price || 0) - parseFloat(a.adv?.price || 0));

        if (ofertasValidas.length === 0) {
            throw new Error('No hay ofertas válidas con monto mínimo <= 15000 ARS');
        }

        const mejorOferta = ofertasValidas[0];
        const precio = parseFloat(mejorOferta.adv?.price || 0);

        console.log(`✅ Mejor oferta ARS: ${precio} ARS/USDT (MercadoPago/Brubank/LemonCash)`);
        return precio;
    } catch (error) {
        console.error('Error obteniendo tasa ARS:', error.message);
        throw error;
    }
}

/**
 * Obtiene la mejor tasa de compra de USDT con CLP
 * @returns {Promise<number>} - Precio en CLP por 1 USDT
 */
async function obtenerTasaCompraCLP() {
    try {
        const anuncios = await consultarBinanceP2P('CLP', 'BUY', [], null);
        
        if (!anuncios || anuncios.length === 0) {
            throw new Error('No se encontraron ofertas de compra USDT con CLP');
        }

        // Filtrar por disponibilidad mínima de 500 USDT y ordenar por precio (más bajo primero = mejor para comprar)
        const ofertasValidas = anuncios
            .filter(ad => {
                if (!ad.adv || !ad.adv.price) return false;
                const disponible = parseFloat(ad.adv.surplusAmount || 0);
                return disponible >= 500;
            })
            .sort((a, b) => parseFloat(a.adv.price) - parseFloat(b.adv.price));

        if (ofertasValidas.length === 0) {
            throw new Error('No hay ofertas válidas con disponibilidad >= 500 USDT');
        }

        const mejorOferta = ofertasValidas[0];
        const precio = parseFloat(mejorOferta.adv.price);

        console.log(`✅ Mejor oferta CLP: ${precio} CLP/USDT`);
        return precio;
    } catch (error) {
        console.error('Error obteniendo tasa CLP:', error.message);
        throw error;
    }
}

// =================================================================
// ENDPOINT: TASAS P2P VES/CLP
// =================================================================

app.get('/api/p2p/tasas-ves-clp', apiAuth, async (req, res) => {
    try {
        console.log('🔄 Consultando tasas P2P VES/CLP...');

        // 1. Obtener tasas P2P
        const [tasa_ves_p2p, tasa_clp_p2p] = await Promise.all([
            obtenerTasaVentaVES(),
            obtenerTasaCompraCLP()
        ]);

        // 2. Calcular tasa base CLP → VES
        const tasa_base_clp_ves = tasa_ves_p2p / tasa_clp_p2p;

        // 3. Calcular tasas ajustadas
        const tasa_menos_5 = tasa_base_clp_ves * (1 - 0.05);
        const tasa_menos_4_5 = tasa_base_clp_ves * (1 - 0.045);
        const tasa_menos_4 = tasa_base_clp_ves * (1 - 0.04);

        // 4. Redondear a 4 decimales
        const redondear = (num) => Math.round(num * 10000) / 10000;

        const response = {
            tasa_ves_p2p: redondear(tasa_ves_p2p),
            tasa_clp_p2p: redondear(tasa_clp_p2p),
            tasa_base_clp_ves: redondear(tasa_base_clp_ves),
            tasas_ajustadas: {
                tasa_menos_5: redondear(tasa_menos_5),
                tasa_menos_4_5: redondear(tasa_menos_4_5),
                tasa_menos_4: redondear(tasa_menos_4)
            },
            metadata: {
                fuente: 'Binance P2P',
                banco_ves: 'Bancamiga',
                min_ves: 50000,
                timestamp: new Date().toISOString()
            }
        };

        console.log('✅ Tasas P2P calculadas exitosamente');
        res.json(response);
    } catch (error) {
        console.error('❌ Error en endpoint /api/p2p/tasas-ves-clp:', error.message);
        res.status(500).json({ 
            message: 'Error al consultar tasas P2P',
            error: error.message 
        });
    }
});

// =================================================================
// ENDPOINT: TASAS P2P COP/CLP
// =================================================================

app.get('/api/p2p/tasas-cop-clp', apiAuth, async (req, res) => {
    try {
        console.log('🔄 Consultando tasas P2P COP/CLP...');

        // 1. Obtener tasas P2P
        const [tasa_cop_p2p, tasa_clp_p2p] = await Promise.all([
            obtenerTasaVentaCOP(),
            obtenerTasaCompraCLP()
        ]);

        // 2. Calcular tasa base CLP → COP
        const tasa_base_clp_cop = tasa_cop_p2p / tasa_clp_p2p;

        // 3. Calcular tasas ajustadas
        const tasa_menos_5 = tasa_base_clp_cop * (1 - 0.05);
        const tasa_menos_4_5 = tasa_base_clp_cop * (1 - 0.045);
        const tasa_menos_4 = tasa_base_clp_cop * (1 - 0.04);

        // 4. Redondear a 4 decimales
        const redondear = (num) => Math.round(num * 10000) / 10000;

        const response = {
            tasa_cop_p2p: redondear(tasa_cop_p2p),
            tasa_clp_p2p: redondear(tasa_clp_p2p),
            tasa_base_clp_cop: redondear(tasa_base_clp_cop),
            tasas_ajustadas: {
                tasa_menos_5: redondear(tasa_menos_5),
                tasa_menos_4_5: redondear(tasa_menos_4_5),
                tasa_menos_4: redondear(tasa_menos_4)
            },
            metadata: {
                fuente: 'Binance P2P',
                bancos_cop: 'Bancolombia, Nequi',
                min_cop: 40000,
                timestamp: new Date().toISOString()
            }
        };

        console.log('✅ Tasas P2P COP/CLP calculadas exitosamente');
        res.json(response);
    } catch (error) {
        console.error('❌ Error en endpoint /api/p2p/tasas-cop-clp:', error.message);
        res.status(500).json({ 
            message: 'Error al consultar tasas P2P',
            error: error.message 
        });
    }
});

// =================================================================
// ENDPOINT: TASAS P2P PEN/CLP
// =================================================================

app.get('/api/p2p/tasas-pen-clp', apiAuth, async (req, res) => {
    try {
        console.log('🔄 Consultando tasas P2P PEN/CLP...');

        // 1. Obtener tasas P2P
        const [tasa_pen_p2p, tasa_clp_p2p] = await Promise.all([
            obtenerTasaVentaPEN(),
            obtenerTasaCompraCLP()
        ]);

        // 2. Calcular tasa base CLP → PEN
        const tasa_base_clp_pen = tasa_pen_p2p / tasa_clp_p2p;

        // 3. Calcular tasas ajustadas
        const tasa_menos_5 = tasa_base_clp_pen * (1 - 0.05);
        const tasa_menos_4_5 = tasa_base_clp_pen * (1 - 0.045);
        const tasa_menos_4 = tasa_base_clp_pen * (1 - 0.04);

        // 4. Redondear a 6 decimales para PEN
        const redondear = (num) => Math.round(num * 1000000) / 1000000;

        const response = {
            tasa_pen_p2p: redondear(tasa_pen_p2p),
            tasa_clp_p2p: redondear(tasa_clp_p2p),
            tasa_base_clp_pen: redondear(tasa_base_clp_pen),
            tasas_ajustadas: {
                tasa_menos_5: redondear(tasa_menos_5),
                tasa_menos_4_5: redondear(tasa_menos_4_5),
                tasa_menos_4: redondear(tasa_menos_4)
            },
            metadata: {
                fuente: 'Binance P2P',
                bancos_pen: 'BCP, Yape',
                min_pen: 30,
                timestamp: new Date().toISOString()
            }
        };

        console.log('✅ Tasas P2P PEN/CLP calculadas exitosamente');
        res.json(response);
    } catch (error) {
        console.error('❌ Error en endpoint /api/p2p/tasas-pen-clp:', error.message);
        res.status(500).json({ 
            message: 'Error al consultar tasas P2P',
            error: error.message 
        });
    }
});

// =================================================================
// ENDPOINT: TASAS P2P BOB/CLP
// =================================================================

app.get('/api/p2p/tasas-bob-clp', apiAuth, async (req, res) => {
    try {
        console.log('🔄 Consultando tasas P2P BOB/CLP...');

        // 1. Obtener tasas P2P
        const [tasa_bob_p2p, tasa_clp_p2p] = await Promise.all([
            obtenerTasaVentaBOB(),
            obtenerTasaCompraCLP()
        ]);

        // 2. Calcular tasa base CLP → BOB
        const tasa_base_clp_bob = tasa_bob_p2p / tasa_clp_p2p;

        // 3. Calcular tasas ajustadas
        const tasa_menos_5 = tasa_base_clp_bob * (1 - 0.05);
        const tasa_menos_4_5 = tasa_base_clp_bob * (1 - 0.045);
        const tasa_menos_4 = tasa_base_clp_bob * (1 - 0.04);

        // 4. Redondear a 4 decimales para BOB
        const redondear = (num) => Math.round(num * 10000) / 10000;

        const response = {
            tasa_bob_p2p: redondear(tasa_bob_p2p),
            tasa_clp_p2p: redondear(tasa_clp_p2p),
            tasa_base_clp_bob: redondear(tasa_base_clp_bob),
            tasas_ajustadas: {
                tasa_menos_5: redondear(tasa_menos_5),
                tasa_menos_4_5: redondear(tasa_menos_4_5),
                tasa_menos_4: redondear(tasa_menos_4)
            },
            metadata: {
                fuente: 'Binance P2P',
                bancos_bob: 'Banco Ganadero, Banco Economico',
                min_bob: 100,
                timestamp: new Date().toISOString()
            }
        };

        console.log('✅ Tasas P2P BOB/CLP calculadas exitosamente');
        res.json(response);
    } catch (error) {
        console.error('❌ Error en endpoint /api/p2p/tasas-bob-clp:', error.message);
        res.status(500).json({ 
            message: 'Error al consultar tasas P2P',
            error: error.message 
        });
    }
});

// =================================================================
// ENDPOINT: TASAS P2P ARS/CLP
// =================================================================

app.get('/api/p2p/tasas-ars-clp', apiAuth, async (req, res) => {
    try {
        console.log('🔄 Consultando tasas P2P ARS/CLP...');

        // 1. Obtener tasas P2P
        const [tasa_ars_p2p, tasa_clp_p2p] = await Promise.all([
            obtenerTasaVentaARS(),
            obtenerTasaCompraCLP()
        ]);

        // 2. Calcular tasa base CLP → ARS
        const tasa_base_clp_ars = tasa_ars_p2p / tasa_clp_p2p;

        // 3. Calcular tasas ajustadas
        const tasa_menos_5 = tasa_base_clp_ars * (1 - 0.05);
        const tasa_menos_4_5 = tasa_base_clp_ars * (1 - 0.045);
        const tasa_menos_4 = tasa_base_clp_ars * (1 - 0.04);

        // 4. Redondear a 4 decimales para ARS
        const redondear = (num) => Math.round(num * 10000) / 10000;

        const response = {
            tasa_ars_p2p: redondear(tasa_ars_p2p),
            tasa_clp_p2p: redondear(tasa_clp_p2p),
            tasa_base_clp_ars: redondear(tasa_base_clp_ars),
            tasas_ajustadas: {
                tasa_menos_5: redondear(tasa_menos_5),
                tasa_menos_4_5: redondear(tasa_menos_4_5),
                tasa_menos_4: redondear(tasa_menos_4)
            },
            metadata: {
                fuente: 'Binance P2P',
                bancos_ars: 'MercadoPago, Brubank, LemonCash',
                min_ars: 15000,
                timestamp: new Date().toISOString()
            }
        };

        console.log('✅ Tasas P2P ARS/CLP calculadas exitosamente');
        res.json(response);
    } catch (error) {
        console.error('❌ Error en endpoint /api/p2p/tasas-ars-clp:', error.message);
        res.status(500).json({ 
            message: 'Error al consultar tasas P2P',
            error: error.message 
        });
    }
});

// =================================================================
// FIN: SERVICIO P2P
// =================================================================

// =================================================================
// SISTEMA DE MONITOREO AUTOMÁTICO DE TASAS
// =================================================================

/**
 * Función de monitoreo automático de tasas
 * - Compara tasa P2P (250k) con tasaNivel3
 * - Si P2P es más baja, genera alerta
 * - Espera 15 minutos por respuesta del master
 * - Si no hay respuesta, ajusta automáticamente
 */
const monitorearTasas = async () => {
    try {
        console.log('🔍 [MONITOREO] Iniciando verificación de tasas...');

        // 1. Obtener tasa P2P para 250,000 CLP
        const [tasa_ves_p2p, tasa_clp_p2p] = await Promise.all([
            obtenerTasaVentaVES(),
            obtenerTasaCompraCLP()
        ]);

        const tasa_base_clp_ves = tasa_ves_p2p / tasa_clp_p2p;
        const tasa_p2p_250k = tasa_base_clp_ves * (1 - 0.04); // Con margen -4% para 250k

        // 2. Obtener tasas guardadas (niveles)
        const tasas = await new Promise((resolve) => {
            readConfig('tasaNivel1', (e1, v1) => {
                readConfig('tasaNivel2', (e2, v2) => {
                    readConfig('tasaNivel3', (e3, v3) => {
                        resolve({
                            nivel1: v1 ? Number(v1) : null,
                            nivel2: v2 ? Number(v2) : null,
                            nivel3: v3 ? Number(v3) : null
                        });
                    });
                });
            });
        });

        const { nivel1, nivel2, nivel3 } = tasas;

        // 3. Guardar registro de monitoreo
        const diferencia = nivel3 ? ((nivel3 - tasa_p2p_250k) / tasa_p2p_250k) * 100 : 0;
        const alertaGenerada = nivel3 && tasa_p2p_250k < nivel3 ? 1 : 0;

        await dbRun(
            `INSERT INTO tasas_monitoreo(tasa_p2p_250k, tasa_nivel1, tasa_nivel2, tasa_nivel3, diferencia_porcentaje, alerta_generada, fecha_verificacion) 
             VALUES (?, ?, ?, ?, ?, ?, ?)`,
            [tasa_p2p_250k, nivel1, nivel2, nivel3, diferencia, alertaGenerada, new Date().toISOString()]
        );

        console.log(`📊 [MONITOREO] Tasa P2P 250k: ${tasa_p2p_250k.toFixed(4)} | Nivel3: ${nivel3 || 'N/A'} | Diferencia: ${diferencia.toFixed(2)}%`);

        // 4. Generar alerta si la tasa P2P es más baja (mejor para nosotros)
        if (alertaGenerada) {
            console.log('⚠️ [MONITOREO] ¡Alerta! La tasa P2P es más baja que nuestras tasas guardadas');
            
            // Verificar si ya existe una alerta pendiente reciente (últimos 30 minutos)
            const alertaReciente = await new Promise((resolve) => {
                db.get(
                    `SELECT id FROM tasas_alertas 
                     WHERE estado = 'pendiente' 
                     AND datetime(fecha_creacion) > datetime('now', '-30 minutes')
                     ORDER BY fecha_creacion DESC LIMIT 1`,
                    [],
                    (err, row) => resolve(row)
                );
            });

            if (!alertaReciente) {
                await generarAlertaTasas(tasa_p2p_250k, nivel3, diferencia);
            } else {
                console.log('⏳ [MONITOREO] Ya existe una alerta pendiente reciente, esperando respuesta...');
            }
        } else {
            console.log('✅ [MONITOREO] Tasas normales, no se requiere alerta');
        }

    } catch (error) {
        console.error('❌ [MONITOREO] Error en monitoreo de tasas:', error.message);
    }
};

/**
 * Generar alerta al master sobre tasas altas
 */
const generarAlertaTasas = async (tasaP2P, tasaNivel3, diferencia) => {
    try {
        // 1. Obtener usuario master
        const master = await new Promise((resolve) => {
            db.get(`SELECT id FROM usuarios WHERE role = 'master' LIMIT 1`, [], (err, row) => {
                resolve(row);
            });
        });

        if (!master) {
            console.error('❌ [ALERTA] No se encontró usuario master');
            return;
        }

        // 2. Crear notificación al master
        const mensaje = `🚨 ALERTA DE TASAS VENEZUELA\n\n` +
            `La tasa P2P (250k CLP) está en ${tasaP2P.toFixed(4)} VES/CLP, ` +
            `que es ${diferencia.toFixed(2)}% más BAJA que nuestra tasa actual (${tasaNivel3.toFixed(4)} VES/CLP).\n\n` +
            `Esto significa que NUESTRAS TASAS ESTÁN MUY ALTAS para Venezuela.\n\n` +
            `Si no respondes en 15 minutos, el sistema ajustará automáticamente la tasa a ${tasaP2P.toFixed(4)} VES/CLP.`;

        const notificacionId = await new Promise((resolve) => {
            db.run(
                `INSERT INTO notificaciones(usuario_id, tipo, titulo, mensaje, fecha_creacion) 
                 VALUES (?, ?, ?, ?, ?)`,
                [master.id, 'alerta', '🚨 Tasas Venezuela muy altas', mensaje, new Date().toISOString()],
                function() {
                    resolve(this.lastID);
                }
            );
        });

        // 3. Crear registro de alerta de tasas
        const alertaId = await new Promise((resolve) => {
            db.run(
                `INSERT INTO tasas_alertas(tasa_p2p_250k, tasa_nivel3_actual, diferencia_porcentaje, notificacion_id, fecha_creacion) 
                 VALUES (?, ?, ?, ?, ?)`,
                [tasaP2P, tasaNivel3, diferencia, notificacionId, new Date().toISOString()],
                function() {
                    resolve(this.lastID);
                }
            );
        });

        console.log(`📬 [ALERTA] Notificación creada para master (ID: ${notificacionId})`);
        console.log(`⏰ [ALERTA] Esperando 15 minutos para auto-ajuste...`);

        // 4. Programar auto-ajuste después de 15 minutos
        setTimeout(() => verificarYAjustarTasa(alertaId, tasaP2P), 15 * 60 * 1000);

    } catch (error) {
        console.error('❌ [ALERTA] Error generando alerta:', error.message);
    }
};

/**
 * Verificar si el master respondió, si no, ajustar automáticamente
 */
const verificarYAjustarTasa = async (alertaId, nuevaTasa) => {
    try {
        // 1. Verificar estado de la alerta
        const alerta = await new Promise((resolve) => {
            db.get(
                `SELECT estado, respuesta_master FROM tasas_alertas WHERE id = ?`,
                [alertaId],
                (err, row) => resolve(row)
            );
        });

        if (!alerta) {
            console.error('❌ [AUTO-AJUSTE] Alerta no encontrada');
            return;
        }

        if (alerta.estado === 'respondida') {
            console.log('✅ [AUTO-AJUSTE] Master ya respondió, cancelando auto-ajuste');
            return;
        }

        // 2. Master no respondió, ajustar automáticamente
        console.log(`🤖 [AUTO-AJUSTE] Master no respondió, ajustando tasaNivel3 a ${nuevaTasa.toFixed(4)}`);

        // Actualizar configuración
        await new Promise((resolve) => {
            upsertConfig('tasaNivel3', String(nuevaTasa), () => resolve());
        });

        // Actualizar estado de alerta
        await dbRun(
            `UPDATE tasas_alertas 
             SET estado = 'auto_ajustada', fecha_auto_ajuste = ?, tasa_ajustada = ? 
             WHERE id = ?`,
            [new Date().toISOString(), nuevaTasa, alertaId]
        );

        console.log(`✅ [AUTO-AJUSTE] Tasa ajustada exitosamente a ${nuevaTasa.toFixed(4)} VES/CLP`);

        // 3. Crear mensaje proactivo para el chatbot
        const master = await new Promise((resolve) => {
            db.get(`SELECT id FROM usuarios WHERE role = 'master' LIMIT 1`, [], (err, row) => {
                resolve(row);
            });
        });

        if (master) {
            await dbRun(
                `INSERT INTO chatbot_mensajes_proactivos(usuario_id, tipo, mensaje, prioridad, fecha_creacion) 
                 VALUES (?, ?, ?, ?, ?)`,
                [
                    master.id,
                    'alerta',
                    `🤖 Auto-ajuste de tasa Venezuela: Se ajustó automáticamente tasaNivel3 de a ${nuevaTasa.toFixed(4)} VES/CLP basado en el monitoreo P2P. La tasa anterior estaba muy alta.`,
                    'alta',
                    new Date().toISOString()
                ]
            );
        }

    } catch (error) {
        console.error('❌ [AUTO-AJUSTE] Error en auto-ajuste:', error.message);
    }
};

// Endpoint para que el master responda a una alerta de tasas
app.post('/api/tasas/responder-alerta', apiAuth, onlyMaster, async (req, res) => {
    try {
        const { alerta_id, accion, respuesta } = req.body;
        
        if (!alerta_id || !accion) {
            return res.status(400).json({ message: 'Faltan parámetros requeridos' });
        }

        // Actualizar alerta
        await dbRun(
            `UPDATE tasas_alertas 
             SET estado = 'respondida', respuesta_master = ?, fecha_respuesta = ? 
             WHERE id = ?`,
            [respuesta || accion, new Date().toISOString(), alerta_id]
        );

        console.log(`✅ [RESPUESTA] Master respondió alerta ${alerta_id}: ${accion}`);
        res.json({ ok: true, message: 'Respuesta registrada' });

    } catch (error) {
        console.error('❌ Error respondiendo alerta:', error.message);
        res.status(500).json({ message: 'Error al responder alerta' });
    }
});

// Endpoint para obtener alertas de tasas activas
app.get('/api/tasas/alertas', apiAuth, (req, res) => {
    db.all(
        `SELECT ta.*, n.mensaje as mensaje_notificacion 
         FROM tasas_alertas ta
         LEFT JOIN notificaciones n ON ta.notificacion_id = n.id
         WHERE ta.estado = 'pendiente'
         ORDER BY ta.fecha_creacion DESC`,
        [],
        (err, rows) => {
            if (err) {
                console.error('Error obteniendo alertas de tasas:', err);
                return res.status(500).json({ message: 'Error al obtener alertas' });
            }
            res.json(rows);
        }
    );
});

// Endpoint para obtener historial de monitoreo
app.get('/api/tasas/historial-monitoreo', apiAuth, (req, res) => {
    const limit = req.query.limit || 50;
    db.all(
        `SELECT * FROM tasas_monitoreo 
         ORDER BY fecha_verificacion DESC 
         LIMIT ?`,
        [limit],
        (err, rows) => {
            if (err) {
                console.error('Error obteniendo historial de monitoreo:', err);
                return res.status(500).json({ message: 'Error al obtener historial' });
            }
            res.json(rows);
        }
    );
});

// =================================================================
// FIN: SISTEMA DE MONITOREO AUTOMÁTICO DE TASAS
// =================================================================

// =================================================================
// SISTEMA DE TAREAS Y NOTIFICACIONES
// =================================================================

// Crear tarea
app.post('/api/tareas', apiAuth, (req, res) => {
    const { titulo, descripcion, tipo, prioridad, asignado_a, fecha_vencimiento } = req.body;
    const creado_por = req.session.user.id;
    const fecha_creacion = hoyLocalYYYYMMDD();
    
    if (!titulo) return res.status(400).json({ message: 'El título es obligatorio' });
    
    const sql = `INSERT INTO tareas(titulo, descripcion, tipo, prioridad, asignado_a, creado_por, fecha_creacion, fecha_vencimiento) 
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
    
    db.run(sql, [titulo, descripcion || '', tipo || 'manual', prioridad || 'normal', asignado_a, creado_por, fecha_creacion, fecha_vencimiento], function(err) {
        if (err) {
            console.error('Error creando tarea:', err);
            return res.status(500).json({ message: 'Error al crear tarea' });
        }
        
        // Crear notificación para el asignado
        if (asignado_a) {
            const sqlNot = `INSERT INTO notificaciones(usuario_id, tipo, titulo, mensaje, fecha_creacion, tarea_id) 
                           VALUES (?, 'tarea', ?, ?, ?, ?)`;
            db.run(sqlNot, [asignado_a, 'Nueva tarea asignada', titulo, fecha_creacion, this.lastID]);
        }
        
        res.json({ message: 'Tarea creada exitosamente', id: this.lastID });
    });
});

// Listar tareas
app.get('/api/tareas', apiAuth, (req, res) => {
    const userId = req.session.user.id;
    const userRole = req.session.user.role;
    const { estado, asignado_a } = req.query;
    
    let sql = `SELECT t.*, 
               u1.username as asignado_nombre,
               u2.username as creado_por_nombre
               FROM tareas t
               LEFT JOIN usuarios u1 ON t.asignado_a = u1.id
               LEFT JOIN usuarios u2 ON t.creado_por = u2.id
               WHERE 1=1`;
    const params = [];
    
    // Si es operador, solo ve sus tareas
    if (userRole !== 'master') {
        sql += ` AND t.asignado_a = ?`;
        params.push(userId);
    } else if (asignado_a) {
        sql += ` AND t.asignado_a = ?`;
        params.push(asignado_a);
    }
    
    if (estado) {
        sql += ` AND t.estado = ?`;
        params.push(estado);
    }
    
    sql += ` ORDER BY 
             CASE t.prioridad 
                WHEN 'urgente' THEN 1 
                WHEN 'alta' THEN 2 
                WHEN 'normal' THEN 3 
                WHEN 'baja' THEN 4 
             END,
             t.fecha_creacion DESC`;
    
    db.all(sql, params, (err, rows) => {
        if (err) {
            console.error('Error obteniendo tareas:', err);
            return res.status(500).json({ message: 'Error al obtener tareas' });
        }
        res.json(rows);
    });
});

// Actualizar estado de tarea
app.put('/api/tareas/:id', apiAuth, async (req, res) => {
    const tareaId = req.params.id;
    const { estado, resultado, observaciones, accion } = req.body;
    const userId = req.session.user.id;
    
    try {
        let sql = `UPDATE tareas SET estado = ?, resultado = ?, observaciones = ?`;
        const params = [estado, resultado, observaciones];
        
        if (estado === 'completada') {
            sql += `, fecha_completada = ?`;
            params.push(hoyLocalYYYYMMDD());
        }
        
        sql += ` WHERE id = ?`;
        params.push(tareaId);
        
        await dbRun(sql, params);
        
        // Marcar como leída la notificación de esta tarea para el usuario actual (operador)
        // Esto sucede cuando el operador toma acción (en_progreso, completada, etc.)
        await dbRun(`
            UPDATE notificaciones 
            SET leida = 1 
            WHERE tarea_id = ? AND usuario_id = ? AND leida = 0
        `, [tareaId, userId]);
        
        // Si la tarea se completó con una acción, actualizar la alerta relacionada
        if (estado === 'completada' && accion) {
            const fechaHoy = hoyLocalYYYYMMDD();
            await dbRun(`
                UPDATE alertas 
                SET accion_realizada = ?, fecha_accion = ? 
                WHERE tarea_id = ?
            `, [accion, fechaHoy, tareaId]);
        }
        
        // Notificar al creador si la tarea fue completada
        if (estado === 'completada') {
            const tarea = await dbGet(`SELECT creado_por, titulo FROM tareas WHERE id = ?`, [tareaId]);
            if (tarea) {
                await dbRun(`
                    INSERT INTO notificaciones(usuario_id, tipo, titulo, mensaje, fecha_creacion, tarea_id) 
                    VALUES (?, 'info', ?, ?, ?, ?)
                `, [tarea.creado_por, 'Tarea completada', `La tarea "${tarea.titulo}" ha sido completada`, hoyLocalYYYYMMDD(), tareaId]);
            }
        }
        
        res.json({ message: 'Tarea actualizada exitosamente' });
    } catch (error) {
        console.error('Error actualizando tarea:', error);
        res.status(500).json({ message: 'Error al actualizar tarea' });
    }
});

// Eliminar tarea
app.delete('/api/tareas/:id', apiAuth, onlyMaster, (req, res) => {
    const tareaId = req.params.id;
    
    db.run(`DELETE FROM tareas WHERE id = ?`, [tareaId], function(err) {
        if (err) {
            console.error('Error eliminando tarea:', err);
            return res.status(500).json({ message: 'Error al eliminar tarea' });
        }
        
        // Eliminar notificaciones relacionadas
        db.run(`DELETE FROM notificaciones WHERE tarea_id = ?`, [tareaId]);
        
        res.json({ message: 'Tarea eliminada exitosamente' });
    });
});

// Listar notificaciones
app.get('/api/notificaciones', apiAuth, (req, res) => {
    const userId = req.session.user.id;
    const { leida } = req.query;
    
    let sql = `SELECT * FROM notificaciones WHERE usuario_id = ?`;
    const params = [userId];
    
    if (leida !== undefined) {
        sql += ` AND leida = ?`;
        params.push(leida === 'true' ? 1 : 0);
    }
    
    sql += ` ORDER BY fecha_creacion DESC LIMIT 50`;
    
    db.all(sql, params, (err, rows) => {
        if (err) {
            console.error('Error obteniendo notificaciones:', err);
            return res.status(500).json({ message: 'Error al obtener notificaciones' });
        }
        res.json(rows);
    });
});

// Marcar notificación como leída
app.put('/api/notificaciones/:id/leer', apiAuth, (req, res) => {
    const notifId = req.params.id;
    const userId = req.session.user.id;
    
    db.run(`UPDATE notificaciones SET leida = 1 WHERE id = ? AND usuario_id = ?`, [notifId, userId], function(err) {
        if (err) {
            console.error('Error marcando notificación:', err);
            return res.status(500).json({ message: 'Error al marcar notificación' });
        }
        res.json({ message: 'Notificación marcada como leída' });
    });
});

// Marcar todas las notificaciones como leídas
app.put('/api/notificaciones/leer-todas', apiAuth, (req, res) => {
    const userId = req.session.user.id;
    
    db.run(`UPDATE notificaciones SET leida = 1 WHERE usuario_id = ?`, [userId], function(err) {
        if (err) {
            console.error('Error marcando todas las notificaciones:', err);
            return res.status(500).json({ message: 'Error al marcar notificaciones' });
        }
        res.json({ message: 'Todas las notificaciones marcadas como leídas' });
    });
});

// Contador de notificaciones no leídas
app.get('/api/notificaciones/contador', apiAuth, (req, res) => {
    const userId = req.session.user.id;
    
    db.get(`SELECT COUNT(*) as count FROM notificaciones WHERE usuario_id = ? AND leida = 0`, [userId], (err, row) => {
        if (err) {
            console.error('Error contando notificaciones:', err);
            return res.status(500).json({ count: 0, noLeidas: 0 });
        }
        res.json({ count: row.count, noLeidas: row.count });
    });
});

// Obtener notificaciones no leídas (para chatbot)
app.get('/api/notificaciones/no-leidas', apiAuth, (req, res) => {
    const userId = req.session.user.id;
    
    db.all(`SELECT * FROM notificaciones WHERE usuario_id = ? AND leida = 0 ORDER BY fecha_creacion DESC`, [userId], (err, rows) => {
        if (err) {
            console.error('Error obteniendo notificaciones no leídas:', err);
            return res.json([]);
        }
        res.json(rows || []);
    });
});

// Generar tareas automáticas desde alertas
app.post('/api/tareas/generar-desde-alertas', apiAuth, onlyMaster, async (req, res) => {
    try {
        const fechaHoy = hoyLocalYYYYMMDD();
        
        // Obtener alertas activas SIN acción realizada (sin mensaje_enviado ni promocion_enviada)
        // Permitir reasignar si la última tarea fue cancelada o es de días anteriores
        const alertasSinResolver = await dbAll(`
            SELECT a.* 
            FROM alertas a
            WHERE a.activa = 1 
            AND (a.accion_realizada IS NULL OR a.accion_realizada = '')
            AND (
                a.tarea_id IS NULL 
                OR EXISTS (
                    SELECT 1 FROM tareas t 
                    WHERE t.id = a.tarea_id 
                    AND (t.estado = 'cancelada' OR t.fecha_creacion < ?)
                )
            )
        `, [fechaHoy]);
        
        if (alertasSinResolver.length === 0) {
            return res.json({ message: 'No hay alertas pendientes sin resolver', tareas_creadas: 0 });
        }
        
        // Obtener todos los operadores (excluir master)
        const operadores = await dbAll(`
            SELECT id, username FROM usuarios WHERE role != 'master' ORDER BY id
        `);
        
        if (operadores.length === 0) {
            return res.status(400).json({ message: 'No hay operadores disponibles' });
        }
        
        let indiceOperador = 0;
        let tareasCreadas = 0;
        
        // Distribuir alertas equitativamente
        for (const alerta of alertasSinResolver) {
            // Seleccionar operador por rotación
            const operador = operadores[indiceOperador];
            indiceOperador = (indiceOperador + 1) % operadores.length;
            
            // Obtener datos del cliente
            const cliente = await dbGet(`SELECT nombre FROM clientes WHERE id = ?`, [alerta.cliente_id]);
            
            // Determinar prioridad según días de inactividad
            let prioridad = 'normal';
            if (alerta.dias_inactivo > 60) prioridad = 'urgente';
            else if (alerta.dias_inactivo >= 45) prioridad = 'alta';
            
            // Crear tarea
            const titulo = `Reactivar cliente: ${cliente ? cliente.nombre : 'Desconocido'}`;
            const descripcion = `${alerta.tipo === 'inactivo' ? 'Cliente inactivo' : alerta.tipo === 'critico' ? 'Cliente crítico' : 'Disminución de frecuencia'} - ${alerta.dias_inactivo ? `${alerta.dias_inactivo} días sin actividad` : 'Reducción de operaciones'}. Última operación: ${alerta.ultima_operacion || 'N/A'}`;
            
            const resultTarea = await dbRun(`
                INSERT INTO tareas(titulo, descripcion, tipo, prioridad, asignado_a, creado_por, fecha_creacion)
                VALUES (?, ?, 'automatica', ?, ?, 1, ?)
            `, [titulo, descripcion, prioridad, operador.id, fechaHoy]);
            
            // Vincular tarea con alerta (actualizar el tarea_id)
            await dbRun(`
                UPDATE alertas SET tarea_id = ? WHERE id = ?
            `, [resultTarea.lastID, alerta.id]);
            
            // Crear notificación para el operador
            await dbRun(`
                INSERT INTO notificaciones(usuario_id, tipo, titulo, mensaje, fecha_creacion, tarea_id)
                VALUES (?, 'tarea', 'Nueva tarea asignada', ?, ?, ?)
            `, [operador.id, titulo, fechaHoy, resultTarea.lastID]);
            
            tareasCreadas++;
        }
        
        res.json({ 
            message: `${tareasCreadas} tareas creadas exitosamente`,
            tareas_creadas: tareasCreadas,
            operadores_asignados: operadores.length
        });
    } catch (error) {
        console.error('Error generando tareas desde alertas:', error);
        res.status(500).json({ message: 'Error al generar tareas' });
    }
});

// Rendimiento de operadores (Master)
app.get('/api/operadores/rendimiento', apiAuth, onlyMaster, async (req, res) => {
    try {
        const operadores = await dbAll(`
            SELECT id, username FROM usuarios WHERE role != 'master'
        `);
        
        const rendimientos = [];
        
        for (const operador of operadores) {
            const tareas = await dbGet(`
                SELECT 
                    COUNT(*) as total_asignadas,
                    SUM(CASE WHEN estado = 'completada' THEN 1 ELSE 0 END) as completadas,
                    SUM(CASE WHEN estado = 'pendiente' THEN 1 ELSE 0 END) as pendientes,
                    SUM(CASE WHEN estado = 'en_progreso' THEN 1 ELSE 0 END) as en_progreso
                FROM tareas 
                WHERE asignado_a = ?
            `, [operador.id]);
            
            const porcentaje = tareas.total_asignadas > 0 
                ? Math.round((tareas.completadas / tareas.total_asignadas) * 100) 
                : 0;
            
            let clasificacion = 'bajo';
            if (porcentaje >= 90) clasificacion = 'excelente';
            else if (porcentaje >= 60) clasificacion = 'bueno';
            else if (porcentaje >= 40) clasificacion = 'regular';
            
            rendimientos.push({
                operador_id: operador.id,
                operador_nombre: operador.username,
                total_asignadas: tareas.total_asignadas,
                completadas: tareas.completadas,
                pendientes: tareas.pendientes,
                en_progreso: tareas.en_progreso,
                porcentaje: porcentaje,
                clasificacion: clasificacion
            });
        }
        
        res.json(rendimientos);
    } catch (error) {
        console.error('Error obteniendo rendimiento:', error);
        res.status(500).json({ message: 'Error al obtener rendimiento' });
    }
});

// Rendimiento propio (Operador)
app.get('/api/operadores/mi-rendimiento', apiAuth, async (req, res) => {
    try {
        const userId = req.session.user.id;
        
        const tareas = await dbGet(`
            SELECT 
                COUNT(*) as total_asignadas,
                SUM(CASE WHEN estado = 'completada' THEN 1 ELSE 0 END) as completadas,
                SUM(CASE WHEN estado = 'pendiente' THEN 1 ELSE 0 END) as pendientes,
                SUM(CASE WHEN estado = 'en_progreso' THEN 1 ELSE 0 END) as en_progreso
            FROM tareas 
            WHERE asignado_a = ?
        `, [userId]);
        
        const porcentaje = tareas.total_asignadas > 0 
            ? Math.round((tareas.completadas / tareas.total_asignadas) * 100) 
            : 0;
        
        let clasificacion = 'bajo';
        if (porcentaje >= 90) clasificacion = 'excelente';
        else if (porcentaje >= 60) clasificacion = 'bueno';
        else if (porcentaje >= 40) clasificacion = 'regular';
        
        res.json({
            total_asignadas: tareas.total_asignadas,
            completadas: tareas.completadas,
            pendientes: tareas.pendientes,
            en_progreso: tareas.en_progreso,
            porcentaje: porcentaje,
            clasificacion: clasificacion
        });
    } catch (error) {
        console.error('Error obteniendo mi rendimiento:', error);
        res.status(500).json({ message: 'Error al obtener rendimiento' });
    }
});

// =================================================================
// FIN: SISTEMA DE TAREAS Y NOTIFICACIONES
// =================================================================

// ==================== CHATBOT ENDPOINT ====================
app.post('/api/chatbot', apiAuth, async (req, res) => {
    try {
        const { message } = req.body;
        const messageLower = message.toLowerCase();
        const userRole = req.session.user?.role || 'operador';
        const username = req.session.user?.username || 'Usuario';
        const userId = req.session.user?.id;
        
        if (!message || message.trim().length === 0) {
            return res.status(400).json({ reply: 'Por favor escribe un mensaje.' });
        }

        // Obtener datos del sistema para contexto adicional
        let contextData = await obtenerContextoSistema(userId, userRole);
        
        // Verificar notificaciones no leídas
        const notificacionesNoLeidasPromise = new Promise((resolve) => {
            db.all(`
                SELECT * FROM notificaciones 
                WHERE usuario_id = ? AND leida = 0 
                ORDER BY fecha_creacion DESC 
                LIMIT 5
            `, [userId], (err, rows) => {
                if (err || !rows) return resolve([]);
                resolve(rows);
            });
        });

        // Obtener mensajes proactivos no mostrados (IMPORTANTE: Estos contienen contexto específico de alertas)
        const mensajesProactivosPromise = new Promise((resolve) => {
            db.all(`
                SELECT id, tipo, mensaje, prioridad, fecha_creacion
                FROM chatbot_mensajes_proactivos
                WHERE usuario_id = ? AND mostrado = 0
                ORDER BY prioridad DESC, fecha_creacion DESC
                LIMIT 3
            `, [userId], (err, rows) => {
                if (err || !rows) return resolve([]);
                resolve(rows);
            });
        });

        // Obtener tareas pendientes del usuario
        const tareasPendientesPromise = new Promise((resolve) => {
            db.all(`
                SELECT id, titulo, descripcion, prioridad, estado, fecha_vencimiento, fecha_creacion
                FROM tareas
                WHERE asignado_a = ? AND estado NOT IN ('completada', 'cancelada')
                ORDER BY 
                    CASE prioridad 
                        WHEN 'alta' THEN 1 
                        WHEN 'media' THEN 2 
                        WHEN 'baja' THEN 3 
                        ELSE 4 
                    END,
                    fecha_vencimiento ASC
                LIMIT 10
            `, [userId], (err, rows) => {
                if (err || !rows) return resolve([]);
                resolve(rows);
            });
        });

        // Obtener historial reciente de conversación (últimos 10 mensajes, últimas 24 horas)
        const historialPromise = new Promise((resolve) => {
            const hace24h = new Date();
            hace24h.setHours(hace24h.getHours() - 24);
            const fechaLimite = hace24h.toISOString();
            
            db.all(`
                SELECT mensaje, respuesta, fecha_creacion
                FROM chatbot_history
                WHERE usuario_id = ?
                AND fecha_creacion >= ?
                ORDER BY fecha_creacion DESC
                LIMIT 10
            `, [userId, fechaLimite], (err, rows) => {
                if (err || !rows) return resolve([]);
                resolve(rows.reverse()); // Ordenar cronológicamente
            });
        });

        const [historial, notificaciones, mensajesProactivos, tareasPendientes] = await Promise.all([
            historialPromise, 
            notificacionesNoLeidasPromise, 
            mensajesProactivosPromise,
            tareasPendientesPromise
        ]);
        
        // Si hay notificaciones sin leer, validarlas antes de incluirlas
        if (notificaciones.length > 0) {
            // Filtrar notificaciones que ya están resueltas (por ejemplo, cliente ya completado)
            const notificacionesValidas = [];
            
            for (const notif of notificaciones) {
                let esValida = true;
                
                // Si es notificación de "datos incompletos", verificar si el cliente YA fue actualizado
                if (notif.tipo === 'datos_incompletos' && notif.mensaje) {
                    // Extraer nombre del cliente de la notificación
                    const matchNombre = notif.mensaje.match(/Cliente "([^"]+)"/);
                    if (matchNombre) {
                        const nombreCliente = matchNombre[1];
                        
                        // Verificar si el cliente ahora tiene datos completos
                        const clienteActual = await new Promise((resolve) => {
                            db.get(
                                `SELECT rut, email, telefono FROM clientes WHERE nombre = ? LIMIT 1`,
                                [nombreCliente],
                                (err, cliente) => {
                                    if (err || !cliente) return resolve(null);
                                    resolve(cliente);
                                }
                            );
                        });
                        
                        // Si el cliente ahora tiene todos los datos, NO incluir la notificación
                        if (clienteActual && clienteActual.rut && clienteActual.email && clienteActual.telefono) {
                            esValida = false;
                            // Marcar como leída automáticamente ya que está resuelta
                            db.run(`UPDATE notificaciones SET leida = 1 WHERE id = ?`, [notif.id]);
                            console.log(`✅ Notificación #${notif.id} marcada como leída automáticamente (cliente "${nombreCliente}" ya completo)`);
                        }
                    }
                }
                
                if (esValida) {
                    notificacionesValidas.push(notif);
                }
            }
            
            // Solo agregar notificaciones que siguen siendo relevantes
            if (notificacionesValidas.length > 0) {
                contextData.notificaciones_pendientes = notificacionesValidas;
            }
        }
        
        // Agregar mensajes proactivos al contexto (IMPORTANTE: contienen detalles específicos de alertas)
        if (mensajesProactivos.length > 0) {
            contextData.mensajes_proactivos = mensajesProactivos.map(mp => ({
                tipo: mp.tipo,
                mensaje: mp.mensaje,
                prioridad: mp.prioridad
            }));
        }
        
        // Agregar tareas pendientes al contexto (IMPORTANTE: el usuario puede preguntar sobre sus tareas)
        if (tareasPendientes.length > 0) {
            const hoy = new Date();
            contextData.tareas_pendientes = tareasPendientes.map(t => {
                const fechaVenc = t.fecha_vencimiento ? new Date(t.fecha_vencimiento) : null;
                const diasRestantes = fechaVenc ? Math.ceil((fechaVenc - hoy) / (1000 * 60 * 60 * 24)) : null;
                const vencida = fechaVenc ? fechaVenc < hoy : false;
                
                return {
                    id: t.id,
                    titulo: t.titulo,
                    descripcion: t.descripcion,
                    prioridad: t.prioridad,
                    estado: t.estado,
                    fecha_vencimiento: t.fecha_vencimiento,
                    vencida: vencida,
                    dias_restantes: diasRestantes
                };
            });
            contextData.total_tareas_pendientes = tareasPendientes.length;
        } else {
            contextData.tareas_pendientes = [];
            contextData.total_tareas_pendientes = 0;
        }
        
        // Las consultas de clientes ahora se manejan automáticamente por OpenAI Function Calling
        // Ya no necesitamos regex para detectar búsquedas - OpenAI decide cuándo llamar buscar_cliente()

        // Las consultas ahora se manejan automáticamente por OpenAI Function Calling
        // Ya no necesitamos regex para detectar consultas - OpenAI decide qué función llamar

        // Contexto del sistema para el chatbot - ASISTENTE INTERNO DE OPERACIONES
        const systemContext = `🧠 PROMPT SISTEMA – ASISTENTE INTERNO DE OPERACIONES Y SUPERVISOR SUAVE (DEFIORACLE.CL)

Eres el Asistente Interno de Operaciones y Supervisor Suave de la empresa de remesas DefiOracle.cl.

👉 Solo hablas con operadores y usuarios master del sistema.
Nunca conversas directamente con el cliente final.

Tu trabajo es ayudar, supervisar suavemente y mejorar el rendimiento de los operadores.

USUARIO ACTUAL: "${username}" con rol de "${userRole}".

1. INFORMACIÓN DE LA EMPRESA

Nombre comercial: DefiOracle.cl
Razón social: DEFI ORACLE SPA
Rubro: Empresa de remesas y cambio de divisas, usando cripto (USDT) como puente.
Ubicación: Santiago de Chile, comuna de Las Condes.
Ámbito: Envía dinero desde Chile (CLP) hacia varios países (principalmente Venezuela, pero también Colombia, Perú, Argentina, República Dominicana, Europa y EE.UU.).

DATOS BANCARIOS OFICIALES (cuenta CLP):
Banco: BancoEstado – Chequera Electrónica
Nombre: DEFI ORACLE SPA
N.º de cuenta: 316-7-032793-3
RUT: 77.354.262-7

Horario de atención: 08:00–21:00 hrs, todos los días.

Canales de atención:
- Canal principal: WhatsApp (chat directo con clientes, envío de comprobantes, seguimiento)
- Canal soporte/marketing: Instagram @DefiOracle.cl

2. SERVICIOS Y DESTINOS

Envío desde CLP (Chile) hacia:
- Venezuela (VES): Provincial, Banesco, Banco de Venezuela, Tesoro, BNC, Mercantil, Bancamiga, Pago Móvil
- Colombia (COP): Bancolombia, Davivienda, Daviplata, Nequi
- Perú (PEN): BCP, Interbank
- Bolivia (BOB): Bancos disponibles
- Argentina (ARS): Bancos disponibles
- Otros: República Dominicana, Europa, EE.UU.

3. USO DE TASAS Y CONVERSIONES

IMPORTANTE: Las tasas que debes usar son las TASAS DE VENTA (las que ofrecemos a los clientes):

VENEZUELA (VES) - TASAS DE VENTA:
- ≥ 5.000 CLP: ${contextData.tasas_actuales.VES_nivel1} VES por 1 CLP
- ≥ 100.000 CLP: ${contextData.tasas_actuales.VES_nivel2} VES por 1 CLP
- ≥ 250.000 CLP: ${contextData.tasas_actuales.VES_nivel3} VES por 1 CLP

Estas son las tasas que los operadores ofrecen a los clientes finales.

OTROS PAÍSES (COP, PEN, BOB, ARS):
- Usa tasas basadas en Binance P2P ajustadas con margen

PROMOCIONES POR BAJA ACTIVIDAD (solo Venezuela):
- Cuando el sistema genere alerta de cliente inactivo/reducción de envíos
- Tasa promo = Tasa base CLP→VES de Binance P2P – 3%
- Genera mensaje personalizado con nombre del cliente y tasa promocional

4. ASISTENTE DE CONVERSACIÓN

Cuando el operador te escriba:
- Entiende la intención (conversión, proceso, datos bancarios, tiempos, promo)
- Genera respuesta clara, amigable, semiformal
- Lista para copiar y pegar en WhatsApp

DATOS BANCARIOS - Cuando se pidan, envía SIEMPRE:
"Te dejo los datos de nuestra cuenta en Chile:
Banco: BancoEstado – Chequera Electrónica
Nombre: DEFI ORACLE SPA
N.º de cuenta: 316-7-032793-3
RUT: 77.354.262-7

Después de hacer el pago, que el cliente envíe el comprobante por WhatsApp para procesar su envío 😉."

5. SUPERVISIÓN DE DATOS DE CLIENTES Y ACCIONES COMO AGENTE

Cliente "completo" = nombre, rut, email, telefono

⚠️ IMPORTANTE: NO guardamos ni solicitamos datos bancarios de clientes. Solo validamos: RUT, email, teléfono.

Si falta información, informa al operador de forma conversacional y sugiere actualizar los datos.

🤖 MODO AGENTE AUTÓNOMO CON FUNCTION CALLING:

Tienes acceso REAL a funciones para consultar la base de datos. OpenAI decide AUTOMÁTICAMENTE cuándo llamarlas según el contexto de la pregunta.

FUNCIONES DISPONIBLES (llamadas automáticamente por ti):

1️⃣ **buscar_cliente(nombre)**
   - Cuándo usarla: Cuando pregunten sobre un cliente específico, si actualizaron datos, verificar información, etc.
   - Ejemplos de preguntas:
     * "¿ya actualizaron a Cris?"
     * "datos de María"
     * "tiene el cliente Juan todos los datos?"
     * "verificar si Cris tiene email"
   - Retorna: {encontrado, nombre, rut, email, telefono, datos_completos, faltan: [array con 'RUT', 'Email', 'Teléfono']}
   - Razonamiento: Si preguntan "¿ya actualizaron a Cris?", TÚ decides llamar buscar_cliente("Cris"), recibes los datos actuales, y respondes si están completos o no
   - CRÍTICO: Si el cliente tiene datos_completos=true, NO menciones que faltan datos. Si datos_completos=false, menciona SOLO lo que está en faltan[]

2️⃣ **listar_operaciones_dia(limite)**
   - Cuándo usarla: Cuando pregunten sobre operaciones de hoy, envíos realizados, última operación
   - Ejemplos:
     * "¿cuántas operaciones llevamos hoy?"
     * "muéstrame las últimas operaciones"
     * "qué envíos se hicieron hoy"
   - Retorna: {total, operaciones: [{numero_recibo, cliente, monto_clp, monto_ves, tasa, operador, hora}]}

3️⃣ **consultar_rendimiento()**
   - Cuándo usarla: Cuando el operador pregunte sobre su desempeño, estadísticas, productividad
   - Ejemplos:
     * "cómo voy este mes?"
     * "mi rendimiento"
     * "cuántas operaciones he hecho?"
   - Retorna: {total_operaciones, total_procesado_clp, ganancia_total_clp, ganancia_promedio_clp}

4️⃣ **listar_clientes_incompletos(limite)**
   - Cuándo usarla: Cuando pregunten sobre clientes pendientes de actualizar
   - Ejemplos:
     * "¿qué clientes faltan actualizar?"
     * "clientes incompletos"
     * "quién necesita completar datos?"
   - Retorna: {total, clientes: [{nombre, faltan: [array]}]}

5️⃣ **buscar_operaciones_cliente(nombre_cliente)**
   - Cuándo usarla: Cuando pregunten sobre el historial de un cliente específico
   - Ejemplos:
     * "cuántas operaciones tiene Cris?"
     * "historial de envíos de María"
     * "ha enviado Juan anteriormente?"
   - Retorna: {total, operaciones: [{numero_recibo, monto_clp, monto_ves, fecha}]}

6️⃣ **calcular_conversion_moneda(monto, moneda_origen, moneda_destino)**
   - Cuándo usarla: Cuando pregunten sobre conversiones entre monedas, cuánto transferir, tasas de cambio
   - Ejemplos:
     * "¿cuánto debo transferir en CLP para que lleguen 40.000 COP?"
     * "convertir 100.000 CLP a VES"
     * "cuántos dólares son 500.000 pesos chilenos?"
     * "equivalencia entre pesos chilenos y colombianos"
     * "cuál es la tasa CLP a COP"

7️⃣ **consultar_tareas(incluir_completadas)**
   - Cuándo usarla: Cuando pregunten sobre tareas pendientes, trabajo asignado, qué hacer
   - Ejemplos:
     * "¿tengo tareas pendientes?"
     * "qué tareas tengo hoy?"
     * "mis asignaciones"
     * "qué debo hacer?"
     * "tareas"
   - Retorna: {total, tareas: [{titulo, descripcion, prioridad, estado, fecha_vencimiento, vencida, dias_restantes}]}
   - IMPORTANTE: Si el mensaje proactivo mencionó tareas, SIEMPRE llama esta función

8️⃣ **obtener_estadisticas_clientes()**
   - Cuándo usarla: Cuando pregunten por el total de clientes, estadísticas generales
   - Ejemplos:
     * "¿cuántos clientes tenemos?"
     * "total de clientes registrados"
     * "estadísticas de clientes"
   - Retorna: {total_clientes, clientes_completos, clientes_incompletos, porcentaje_completos}

9️⃣ **analizar_tarea_cliente_inactivo(nombre_cliente, descripcion_tarea)**
   - Cuándo usarla: Cuando el operador pida ayuda con una tarea de cliente inactivo o reducción de actividad
   - Ejemplos:
     * "¿qué hago con esta tarea de [cliente]?"
     * "ayúdame con el cliente inactivo [nombre]"
     * "¿qué mensaje envío a [cliente]?"
     * Operador menciona tarea de: "cliente inactivo por X días", "reducción de actividad", "riesgo alto"
   - Función INTELIGENTE que:
     ✅ Analiza los días de inactividad
     ✅ Determina si debe enviar recordatorio (30-44 días) o promoción (45+ días)
     ✅ Calcula tasa promocional automáticamente (0.33% descuento sobre última tasa VES)
     ✅ Genera mensaje personalizado listo para copiar y enviar
   - Aplica a: "Cliente inactivo", "Reducción de actividad", "Riesgo alto"
   - Retorna: {tipo_accion, dias_inactivo, tasa_original, tasa_promocional, mensaje_sugerido}

🔟 **consultar_monitoreo_tasas()**
   - Cuándo usarla: Para verificar el estado del monitoreo automático de tasas VES
   - Ejemplos:
     * "¿cómo están las tasas?"
     * "¿hay alertas de tasas?"
     * "muestra el monitoreo de tasas"
     * "¿están altas las tasas de Venezuela?"
   - Función de MONITOREO AUTOMÁTICO que:
     ✅ Consulta última verificación de tasas P2P vs tasas guardadas
     ✅ Muestra alertas activas de tasas muy altas
     ✅ Indica si hubo auto-ajuste automático
     ✅ Compara tasa P2P (250k) con tasaNivel3
   - Retorna: {ultima_verificacion, tasa_p2p_actual, tasa_nivel3_actual, diferencia, alerta_activa, estado_alerta, historial_reciente}

   - Monedas soportadas: CLP (Chile), COP (Colombia), VES (Venezuela), USD (Dólares), ARS (Argentina), PEN (Perú), BRL (Brasil), MXN (México), EUR (Euro), UYU (Uruguay)
   - Retorna: {monto_origen, moneda_origen, nombre_moneda_origen, monto_convertido, moneda_destino, nombre_moneda_destino, tasa_cambio, formula}

   
   📐 FÓRMULAS DE CONVERSIÓN (IMPORTANTE):
   
   Para convertir DESDE moneda A HACIA moneda B:
   Monto en B = Monto en A × Tasa(A→B)
   
   Ejemplos prácticos:
   
   ✅ "¿Cuántos COP son 100.000 CLP?"
   → Llamas: calcular_conversion_moneda(100000, "CLP", "COP")
   → Tasa CLP→COP = 4 (porque 1 CLP = 4 COP)
   → Resultado: 100.000 × 4 = 400.000 COP
   
   ✅ "¿Cuántos CLP necesito transferir para que lleguen 40.000 COP?"
   → Usuario pregunta: Cuántos CLP → 40.000 COP (quiere saber el origen)
   → Llamas: calcular_conversion_moneda(40000, "COP", "CLP")
   → Tasa COP→CLP = 0.25 (porque 1 COP = 0.25 CLP)
   → Resultado: 40.000 × 0.25 = 10.000 CLP
   → Respondes: "Para que lleguen 40.000 COP, debes transferir 10.000 CLP"
   
   ✅ "¿Cuántos VES recibe el cliente por 50.000 CLP?"
   → Llamas: calcular_conversion_moneda(50000, "CLP", "VES")
   → Resultado basado en tasa actual
   
   ⚠️ IMPORTANTE - INTERPRETACIÓN DE PREGUNTAS:
   
   Cuando pregunten "¿cuánto debo transferir para que lleguen X [moneda destino]?":
   - El usuario TIENE moneda destino conocida (X unidades)
   - El usuario NECESITA saber cuánta moneda origen enviar
   - Llamas: calcular_conversion_moneda(X, "moneda_destino", "moneda_origen")
   
   Cuando pregunten "¿cuánto llega si envío X [moneda origen]?":
   - El usuario TIENE moneda origen conocida (X unidades)
   - El usuario NECESITA saber cuánto llega en moneda destino
   - Llamas: calcular_conversion_moneda(X, "moneda_origen", "moneda_destino")

RAZONAMIENTO AUTÓNOMO:

✅ TÚ DECIDES qué función llamar según el contexto de la pregunta
✅ OpenAI analiza la pregunta y elige la función apropiada automáticamente
✅ NO necesitas que el usuario use palabras exactas
✅ Entiendes intención: "¿ya está listo Cris?" → buscar_cliente("Cris") → revisar datos_completos

EJEMPLOS DE RAZONAMIENTO:

Pregunta: "¿ya actualizaron a ese cliente Cris?"
→ TÚ razonas: "Necesito buscar si Cris existe y si sus datos están completos"
→ Llamas: buscar_cliente("Cris")
→ Recibes: {encontrado: true, nombre: "Cris", rut: "12345", email: "cris@mail.com", telefono: "987654", datos_completos: true, faltan: []}
→ Respondes: "Sí, Cris ya está completo ✅. Tiene RUT, email y teléfono registrados."

Pregunta: "tiene datos el cliente que se llama María?"
→ Llamas: buscar_cliente("María")
→ Respondes según lo que encuentres

Pregunta: "cuánto he trabajado este mes?"
→ Llamas: consultar_rendimiento()
→ Respondes con las estadísticas

IMPORTANTE:

✅ Llamas funciones AUTOMÁTICAMENTE cuando detectas la necesidad
✅ NO pidas permiso para consultar - simplemente hazlo
✅ Presenta los resultados de forma conversacional y amigable
✅ Si no encuentras datos, dilo claramente: "No encontré cliente con ese nombre"
✅ NO inventes información - usa SOLO lo que las funciones retornan

6. GESTIÓN DE TAREAS Y RENDIMIENTO

Revisa tareas pendientes o vencidas
Como supervisor suave, pregunta sin regañar
Sugiere actualización de tareas según respuesta del operador

Rendimiento: Usa /api/mi-rendimiento para explicar métricas del mes

7. CONTEXTO CONVERSACIONAL Y NOTIFICACIONES

Mantén el contexto de la conversación. Si el operador te preguntó algo anteriormente, recuérdalo.

🔔 IMPORTANTE - NOTIFICACIONES PROACTIVAS (OBLIGATORIO):

⚠️ REGLA ABSOLUTA - VERIFICA PRIMERO:
ANTES de responder cualquier cosa, REVISA si contextData.notificaciones_pendientes tiene contenido.

Si contextData.notificaciones_pendientes existe y NO está vacío:
- 🚨 DEBES mencionarlas INMEDIATAMENTE en tu respuesta
- ❌ NO respondas nada más sin mencionarlas primero
- ✅ Menciónalas ANTES de responder cualquier otra cosa

CUÁNDO MENCIONAR NOTIFICACIONES:
- ✅ SIEMPRE que contextData.notificaciones_pendientes tenga datos
- ✅ Especialmente cuando el usuario te salude ("hola", "buenos días", "qué hay", etc.)
- ✅ Cuando pregunten "tengo notificaciones?", "qué hay pendiente", "tareas", "alertas"
- ❌ NUNCA digas "no hay notificaciones" si contextData.notificaciones_pendientes tiene elementos

EJEMPLO VERIFICACIÓN:
Usuario: "hola"
TÚ piensas: ¿Hay algo en contextData.notificaciones_pendientes?
- SI HAY: Mencionar PRIMERO las notificaciones
- NO HAY: Saludo normal

CÓMO MENCIONAR NOTIFICACIONES:
- Ejemplo BUENO: "¡Hola! 👋 Mira, hay un tema: el cliente Craus hizo un envío pero le faltan RUT, email y teléfono. ¿Lo revisamos?"
- Ejemplo MALO: "Notificación #1: Cliente Craus tiene datos incompletos..."
- Si hay varias (2-3), menciónalas: "Hay un par de cosas: 1) Craus necesita datos, 2) María también..."
- Si hay muchas (>3): "Tienes 5 notificaciones. Las más importantes: Craus y María necesitan actualizar datos"

FORMATO DE RESPUESTA CON NOTIFICACIONES:
1. Saludo breve
2. ⭐ MENCIONA LAS NOTIFICACIONES (palabra clave: "pendiente", "falta", "incompleto", etc.)
3. Pregunta si quiere más detalles

Ejemplo completo cuando preguntan "tengo notificaciones?":
"Sí! Tienes 1 notificación pendiente: el cliente Craus hizo una operación pero le faltan datos (RUT, email, teléfono). ¿Quieres que busque más info?"

❌ NUNCA digas "no hay notificaciones" si contextData.notificaciones_pendientes tiene contenido

CRÍTICO - SOBRE CONSULTAS DE DATOS ESPECÍFICOS:
- SI te piden datos de un cliente específico, revisa si hay información en contextData.cliente_consultado
- Si contextData.cliente_consultado existe, muestra esos datos de forma conversacional y clara
- Si NO existe cliente_consultado pero te piden datos, sugiere verificar el nombre del cliente
- NUNCA inventes datos como RUT, email, teléfono
- Solo usa la información real que viene en contextData

CUANDO MUESTRES DATOS DE UN CLIENTE:
- Formato conversacional, NO listados robóticos
- Ejemplo BUENO: "Cris está registrado desde [fecha]. Tiene RUT: xxx, email: xxx, teléfono: xxx. Todo completo ✅"
- Ejemplo MALO: "Datos del cliente: - Nombre: Cris - RUT: xxx..."
- Si faltan datos, menciónalos de forma natural: "A Cris le falta el email y el teléfono, el RUT sí lo tiene"

8. ESTILO Y TONO

- CONVERSACIONAL, cercano, como un compañero de trabajo que ayuda
- Respuestas CORTAS y directas (evita textos largos)
- Usa emojis con moderación (1-2 por mensaje máximo)
- Nunca regañes, siempre sugiere con frases tipo "Ojo con este detalle..." o "Te sugiero..."
- Mismo idioma del operador (por defecto español chileno)
- Si falta información clave, pide aclaración de forma natural
- NO inventes información que no tienes

TU ROL: Eres como un supervisor amigable que ayuda - explicas, corriges, sugieres y acompañas. Nunca atacas ni regañas.

⚠️ IMPORTANTE - LEE SIEMPRE ESTOS CONTEXTOS PRIMERO:

📋 **1. MENSAJES PROACTIVOS** (contextData.mensajes_proactivos):
- Estos mensajes contienen información ESPECÍFICA ya detectada por el sistema
- Nombres exactos de clientes, detalles precisos de alertas
- Cuando el usuario responda a un mensaje proactivo, USA LA INFORMACIÓN DEL MENSAJE
- NO llames funciones genéricas si el mensaje proactivo ya tiene los detalles
- Ejemplo: Si dice "Cristia Jose, Craus y 1 más", menciona ESOS nombres exactos

🔔 **2. NOTIFICACIONES PENDIENTES** (contextData.notificaciones_pendientes):
- Alertas del sistema de notificaciones normales
- Menciónalas cuando existan, especialmente al saludar
- Palabra clave para mencionar: "pendiente", "falta", "incompleto"

✅ **3. TAREAS PENDIENTES** (contextData.tareas_pendientes):
- Lista de tareas asignadas al usuario
- Total disponible en: contextData.total_tareas_pendientes
- Cuando pregunten por tareas, VERIFICA PRIMERO si ya están en el contexto
- Si contextData.tareas_pendientes tiene datos, úsalos directamente
- Solo llama a consultar_tareas() si necesitas actualizar o filtrar

🎯 **AYUDA CON TAREAS - FUNCIÓN INTELIGENTE**:

Cuando el operador tenga tareas y pida ayuda:
- **Detecta tipo de tarea**: "Cliente inactivo por X días", "Reducción de actividad", etc.
- **Ofrece ayuda automáticamente**: "¿Quieres que te ayude a resolver esta tarea?"
- **Usa analizar_tarea_cliente_inactivo()** para generar mensajes automáticos

Ejemplo de flujo:
Operador: "Tengo una tarea de andrez hernandez, cliente inactivo por 71 días"
Tú: "¡Claro! Voy a analizar esta tarea y generar un mensaje para andrez..."
→ Llamas: analizar_tarea_cliente_inactivo("andrez hernandez", "Cliente inactivo por 71 días")
→ Recibes: tasa promocional calculada + mensaje listo
→ Respondes: "Aquí está el mensaje para andrez: [mensaje generado]. La tasa promocional es [X] VES. ¿Lo envío?"

Tipos de tareas que puedes resolver:
1. **Cliente inactivo 30-44 días**: Mensaje de recordatorio/cercanía (sin promoción)
2. **Cliente inactivo 45+ días**: Mensaje con promoción (tasa + 0.33% descuento)
3. **Reducción de actividad**: Mensaje con promoción (tasa + 0.33% descuento)

📊 **PRIORIDAD DE LECTURA**:
1. PRIMERO: Lee mensajes_proactivos (información más específica)
2. SEGUNDO: Lee notificaciones_pendientes
3. TERCERO: Lee tareas_pendientes
4. ÚLTIMO: Llama funciones solo si necesitas datos adicionales

DATOS DEL SISTEMA ACTUAL:
${JSON.stringify(contextData, null, 2)}

Usa estos datos cuando sea necesario para responder consultas sobre tasas, clientes, rendimiento, etc.`;

        const reply = await generateChatbotResponse(message, systemContext, userRole, username, contextData, historial, userId);
        
        // CRÍTICO: Solo marcar notificaciones como leídas si el chatbot las mencionó en su respuesta
        // Verificamos si la respuesta contiene palabras clave de notificaciones
        if (contextData.notificaciones_pendientes && contextData.notificaciones_pendientes.length > 0) {
            const replyLower = reply.toLowerCase();
            const mencionoNotificaciones = 
                replyLower.includes('notificaci') || 
                replyLower.includes('pendiente') || 
                replyLower.includes('falta') || 
                replyLower.includes('incompleto') ||
                replyLower.includes('datos') ||
                replyLower.includes('alerta');
            
            // Solo marcar como leídas si el chatbot realmente las mencionó
            if (mencionoNotificaciones) {
                const notifIds = contextData.notificaciones_pendientes.map(n => n.id);
                db.run(
                    `UPDATE notificaciones SET leida = 1 WHERE id IN (${notifIds.join(',')})`,
                    (err) => {
                        if (!err) {
                            console.log(`✅ ${notifIds.length} notificación(es) marcada(s) como leída(s) (chatbot las mencionó en su respuesta)`);
                        }
                    }
                );
            } else {
                console.log(`ℹ️ Notificaciones NO marcadas como leídas - el chatbot no las mencionó en esta respuesta`);
            }
        }
        
        // Guardar conversación en el historial
        const fechaCreacion = new Date().toISOString();
        await new Promise((resolve, reject) => {
            db.run(
                `INSERT INTO chatbot_history (usuario_id, rol, mensaje, respuesta, contexto_datos, fecha_creacion) 
                 VALUES (?, ?, ?, ?, ?, ?)`,
                [userId, userRole, message, reply, JSON.stringify(contextData), fechaCreacion],
                (err) => {
                    if (err) reject(err);
                    else resolve();
                }
            );
        });
        
        res.json({ reply });
    } catch (error) {
        console.error('Error en chatbot:', error);
        res.status(500).json({ reply: 'Lo siento, ocurrió un error. Por favor intenta de nuevo.' });
    }
});

// Función para obtener contexto del sistema para el chatbot
async function obtenerContextoSistema(userId, userRole) {
    const context = {
        tasas_actuales: null,
        rendimiento_operador: null,
        tareas_pendientes: null,
        total_clientes: 0
    };

    try {
        // Obtener tasas de VENTA (las que ve el cliente) - tasaNivel1, tasaNivel2, tasaNivel3
        const tasasVentaPromise = new Promise((resolve) => {
            readConfig('tasaNivel1', (e1, v1) => {
                readConfig('tasaNivel2', (e2, v2) => {
                    readConfig('tasaNivel3', (e3, v3) => {
                        resolve({
                            nivel1: v1 ? Number(v1) : 0.30,
                            nivel2: v2 ? Number(v2) : 0.30,
                            nivel3: v3 ? Number(v3) : 0.30
                        });
                    });
                });
            });
        });

        // Obtener total de clientes (solo los que tienen nombre, igual que el frontend)
        const totalClientesPromise = new Promise((resolve) => {
            db.get(`SELECT COUNT(*) as total FROM clientes WHERE nombre IS NOT NULL AND nombre != ''`, [], (err, row) => {
                if (err) return resolve(0);
                resolve(row.total || 0);
            });
        });

        // Obtener tasas P2P base (si existen en configuración)
        const [tasasVenta, totalClientes, tasaCOP, tasaPEN, tasaBOB, tasaARS] = await Promise.all([
            tasasVentaPromise,
            totalClientesPromise,
            readConfigValue('tasaBaseCLP_COP').catch(() => 0),
            readConfigValue('tasaBaseCLP_PEN').catch(() => 0),
            readConfigValue('tasaBaseCLP_BOB').catch(() => 0),
            readConfigValue('tasaBaseCLP_ARS').catch(() => 0)
        ]);

        context.total_clientes = totalClientes;
        
        context.tasas_actuales = {
            VES_nivel1: tasasVenta.nivel1,
            VES_nivel2: tasasVenta.nivel2,
            VES_nivel3: tasasVenta.nivel3,
            VES_descripcion: "Tasas de VENTA a clientes (≥5K, ≥100K, ≥250K CLP). Estas son las que ofrecemos.",
            COP: tasaCOP,
            COP_descripcion: "Tasa base Binance P2P ajustada con margen",
            PEN: tasaPEN,
            PEN_descripcion: "Tasa base Binance P2P ajustada con margen",
            BOB: tasaBOB,
            BOB_descripcion: "Tasa base Binance P2P ajustada con margen",
            ARS: tasaARS,
            ARS_descripcion: "Tasa base Binance P2P ajustada con margen"
        };

        // Si es operador, obtener su rendimiento
        if (userRole === 'operador' && userId) {
            const rendimientoPromise = new Promise((resolve) => {
                const now = new Date();
                const year = now.getFullYear();
                const month = now.getMonth() + 1;
                const primerDia = `${year}-${String(month).padStart(2, '0')}-01`;
                const ultimoDia = `${year}-${String(month).padStart(2, '0')}-31`;

                const sql = `
                    SELECT 
                        COUNT(*) as total_operaciones,
                        IFNULL(SUM(monto_clp), 0) as volumen_total,
                        COUNT(DISTINCT cliente_id) as clientes_unicos
                    FROM operaciones 
                    WHERE usuario_id = ? 
                    AND date(fecha) >= date(?) 
                    AND date(fecha) <= date(?)
                `;

                db.get(sql, [userId, primerDia, ultimoDia], (err, row) => {
                    if (err) return resolve(null);
                    const volumenMillones = row.volumen_total / 1000000;
                    const millones = Math.floor(volumenMillones);
                    const bonificacion = millones * 2;
                    
                    resolve({
                        total_operaciones: row.total_operaciones,
                        millones_comisionables: millones,
                        bonificacion_usd: bonificacion,
                        clientes_unicos: row.clientes_unicos
                    });
                });
            });

            context.rendimiento_operador = await rendimientoPromise;
        }

        // Obtener tareas pendientes del usuario
        if (userId) {
            const tareasPromise = new Promise((resolve) => {
                db.all(`
                    SELECT id, titulo, prioridad, estado, fecha_vencimiento
                    FROM tareas 
                    WHERE asignado_a = ? 
                    AND estado IN ('pendiente', 'en_progreso')
                    ORDER BY 
                        CASE prioridad 
                            WHEN 'urgente' THEN 1 
                            WHEN 'alta' THEN 2 
                            WHEN 'normal' THEN 3 
                            ELSE 4 
                        END,
                        fecha_vencimiento ASC
                    LIMIT 5
                `, [userId], (err, rows) => {
                    if (err) return resolve([]);
                    resolve(rows);
                });
            });

            context.tareas_pendientes = await tareasPromise;
        }

    } catch (error) {
        console.error('Error obteniendo contexto del sistema:', error);
    }

    return context;
}

// 💱 TASAS DE CAMBIO P2P (Base: CLP)
// Actualizar estas tasas regularmente según el mercado
const TASAS_CAMBIO_P2P = {
    // Moneda: tasa (1 unidad de moneda origen = X CLP)
    'CLP': 1,           // Peso Chileno (base)
    'COP': 0.25,        // Peso Colombiano (1 COP = 0.25 CLP, o 1 CLP = 4 COP)
    'VES': 33.33,       // Bolívar Venezolano (1 VES = 33.33 CLP, o 1 CLP = 0.03 VES)
    'USD': 950,         // Dólar estadounidense (1 USD = 950 CLP)
    'ARS': 1.05,        // Peso Argentino (1 ARS = 1.05 CLP)
    'PEN': 250,         // Sol Peruano (1 PEN = 250 CLP)
    'BRL': 190,         // Real Brasileño (1 BRL = 190 CLP)
    'MXN': 55,          // Peso Mexicano (1 MXN = 55 CLP)
    'EUR': 1050,        // Euro (1 EUR = 1050 CLP)
    'UYU': 23          // Peso Uruguayo (1 UYU = 23 CLP)
};

// Función para obtener tasa de cambio actualizada desde DB o usar default
async function obtenerTasaCambioActual(monedaOrigen, monedaDestino) {
    // Por ahora usar las tasas fijas, pero esto puede extenderse para
    // consultar tasas dinámicas desde la tabla de operaciones recientes
    
    if (monedaOrigen === monedaDestino) return 1;
    
    const tasaOrigenACLP = TASAS_CAMBIO_P2P[monedaOrigen.toUpperCase()];
    const tasaDestinoACLP = TASAS_CAMBIO_P2P[monedaDestino.toUpperCase()];
    
    if (!tasaOrigenACLP || !tasaDestinoACLP) {
        return null; // Moneda no soportada
    }
    
    // Convertir: Origen → CLP → Destino
    return tasaOrigenACLP / tasaDestinoACLP;
}

// 🤖 FUNCIONES DISPONIBLES PARA EL AGENTE (Function Calling)
const agentFunctions = [
    {
        name: "calcular_conversion_moneda",
        description: "Calcula conversiones entre monedas del P2P. Úsalo cuando pregunten: '¿cuánto debo transferir para que lleguen X pesos colombianos?', 'convertir X a otra moneda', 'cuál es la tasa', 'equivalencia entre monedas', etc. Monedas soportadas: CLP (Chile), COP (Colombia), VES (Venezuela), USD, ARS (Argentina), PEN (Perú), BRL (Brasil), MXN (México), EUR, UYU (Uruguay).",
        parameters: {
            type: "object",
            properties: {
                monto: {
                    type: "number",
                    description: "Cantidad a convertir"
                },
                moneda_origen: {
                    type: "string",
                    description: "Código de la moneda origen (CLP, COP, VES, USD, ARS, PEN, BRL, MXN, EUR, UYU)"
                },
                moneda_destino: {
                    type: "string",
                    description: "Código de la moneda destino (CLP, COP, VES, USD, ARS, PEN, BRL, MXN, EUR, UYU)"
                }
            },
            required: ["monto", "moneda_origen", "moneda_destino"]
        }
    },
    {
        name: "obtener_estadisticas_clientes",
        description: "Obtiene estadísticas generales sobre clientes: total de clientes registrados, cuántos tienen datos completos, cuántos incompletos, distribución, etc. Usa esto cuando pregunten '¿cuántos clientes tenemos?', 'total de clientes', 'estadísticas de clientes', 'clientes registrados', etc.",
        parameters: {
            type: "object",
            properties: {}
        }
    },
    {
        name: "buscar_cliente",
        description: "Busca un cliente en la base de datos por nombre. Usa esto cuando el usuario pregunte sobre datos de un cliente específico, si ya actualizaron un cliente, verificar información, etc.",
        parameters: {
            type: "object",
            properties: {
                nombre: {
                    type: "string",
                    description: "Nombre o parte del nombre del cliente a buscar"
                }
            },
            required: ["nombre"]
        }
    },
    {
        name: "listar_operaciones_dia",
        description: "Lista las operaciones realizadas hoy. Usa esto cuando pregunten sobre envíos, transferencias, operaciones del día, última operación, etc.",
        parameters: {
            type: "object",
            properties: {
                limite: {
                    type: "number",
                    description: "Número máximo de operaciones a listar (por defecto 10)"
                }
            }
        }
    },
    {
        name: "consultar_rendimiento",
        description: "Consulta el rendimiento del operador actual en el mes. Usa esto cuando pregunten 'cómo voy', 'mi desempeño', 'mis operaciones', 'cuánto he hecho', etc.",
        parameters: {
            type: "object",
            properties: {}
        }
    },
    {
        name: "listar_clientes_incompletos",
        description: "Lista clientes que tienen datos faltantes (RUT, email o teléfono). Usa esto cuando pregunten sobre clientes pendientes, incompletos, que faltan actualizar, etc.",
        parameters: {
            type: "object",
            properties: {
                limite: {
                    type: "number",
                    description: "Número máximo de clientes a listar (por defecto 10)"
                }
            }
        }
    },
    {
        name: "buscar_operaciones_cliente",
        description: "Busca las operaciones de un cliente específico. Usa esto cuando pregunten cuántas operaciones tiene un cliente, historial de envíos de alguien, etc.",
        parameters: {
            type: "object",
            properties: {
                nombre_cliente: {
                    type: "string",
                    description: "Nombre del cliente cuyas operaciones buscar"
                }
            },
            required: ["nombre_cliente"]
        }
    },
    {
        name: "consultar_tareas",
        description: "Consulta las tareas asignadas al operador. ÚSALO SIEMPRE cuando pregunten: '¿tengo tareas?', 'mis tareas pendientes', 'qué debo hacer hoy', 'tareas', 'pendientes', 'asignaciones', 'trabajo pendiente', etc. Esta función muestra tareas activas, su prioridad, estado y fecha de vencimiento.",
        parameters: {
            type: "object",
            properties: {
                incluir_completadas: {
                    type: "boolean",
                    description: "Si debe incluir las tareas ya completadas (por defecto false)"
                }
            }
        }
    },
    {
        name: "analizar_tarea_cliente_inactivo",
        description: "Analiza una tarea de cliente inactivo y genera una sugerencia de mensaje personalizado. Úsala cuando el operador pida ayuda con una tarea de: 'cliente inactivo', 'reducción de actividad', 'riesgo alto', o cuando pregunten '¿qué hago con esta tarea?', 'ayúdame con este cliente', '¿qué mensaje envío?'",
        parameters: {
            type: "object",
            properties: {
                nombre_cliente: {
                    type: "string",
                    description: "Nombre del cliente de la tarea"
                },
                descripcion_tarea: {
                    type: "string",
                    description: "Descripción completa de la tarea (ej: 'Cliente inactivo por 30 días')"
                }
            },
            required: ["nombre_cliente", "descripcion_tarea"]
        }
    },
    {
        name: "consultar_monitoreo_tasas",
        description: "Consulta el estado del monitoreo automático de tasas VES P2P vs tasas guardadas. Úsala cuando pregunten: '¿cómo están las tasas?', '¿hay alertas?', 'monitoreo de tasas', '¿están altas las tasas de Venezuela?', 'estado de tasas'",
        parameters: {
            type: "object",
            properties: {},
            required: []
        }
    }
];

// Función para generar respuestas del chatbot con Function Calling
async function generateChatbotResponse(userMessage, systemContext, userRole, username, contextData, historial = [], userId = null) {
    const messageLower = userMessage.toLowerCase();
    
    // SOLO respuestas ultra-rápidas de datos bancarios (se usan mucho)
    if (messageLower === 'datos bancarios' || messageLower === 'cuenta bancaria' || messageLower === 'datos banco') {
        return `🏦 **Datos Bancarios DefiOracle.cl:**\n\nBanco: BancoEstado – Chequera Electrónica\nNombre: DEFI ORACLE SPA\nCuenta: 316-7-032793-3\nRUT: 77.354.262-7\n\n✅ Listo para copiar y pegar.`;
    }
    
    // Para todo lo demás, usar OpenAI con Function Calling
    try {
        // Usar variable de entorno OPENAI_API_KEY, o fallback a la key hardcodeada
        const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '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';
        
        // Validar que hay API key
        if (!OPENAI_API_KEY || OPENAI_API_KEY === '' || OPENAI_API_KEY.includes('your-api-key-here')) {
            console.error('❌ No se encontró API key de OpenAI válida');
            return '❌ Lo siento, el chatbot no está configurado correctamente. Por favor contacta al administrador para configurar la API key de OpenAI.';
        }
        
        // Construir mensajes con historial de conversación
        const messages = [
            { role: 'system', content: systemContext }
        ];
        
        // Agregar historial de conversación (últimos 10 mensajes)
        if (historial && historial.length > 0) {
            historial.forEach(h => {
                messages.push({ role: 'user', content: h.mensaje });
                messages.push({ role: 'assistant', content: h.respuesta });
            });
        }
        
        // Agregar mensaje actual del usuario
        messages.push({ role: 'user', content: userMessage });
        
        // Primera llamada a OpenAI con function calling
        let response = await axios.post('https://api.openai.com/v1/chat/completions', {
            model: 'gpt-3.5-turbo',
            messages: messages,
            functions: agentFunctions,
            function_call: "auto",
            max_tokens: 500,
            temperature: 0.8
        }, {
            headers: {
                'Authorization': `Bearer ${OPENAI_API_KEY}`,
                'Content-Type': 'application/json'
            }
        });
        
        let responseMessage = response.data.choices[0].message;
        
        // Si OpenAI decidió llamar una función
        if (responseMessage.function_call) {
            const functionName = responseMessage.function_call.name;
            const functionArgs = JSON.parse(responseMessage.function_call.arguments);
            
            console.log(`🤖 Agente llamando función: ${functionName} con args:`, functionArgs);
            
            let functionResult = null;
            
            // Ejecutar la función solicitada
            switch (functionName) {
                case 'calcular_conversion_moneda':
                    functionResult = await new Promise(async (resolve) => {
                        const { monto, moneda_origen, moneda_destino } = functionArgs;
                        
                        const monedaOrigenUpper = moneda_origen.toUpperCase();
                        const monedaDestinoUpper = moneda_destino.toUpperCase();
                        
                        // Validar monedas soportadas
                        if (!TASAS_CAMBIO_P2P[monedaOrigenUpper]) {
                            resolve({ 
                                error: true, 
                                mensaje: `❌ La moneda "${moneda_origen}" no está soportada. Monedas disponibles: CLP, COP, VES, USD, ARS, PEN, BRL, MXN, EUR, UYU` 
                            });
                            return;
                        }
                        
                        if (!TASAS_CAMBIO_P2P[monedaDestinoUpper]) {
                            resolve({ 
                                error: true, 
                                mensaje: `❌ La moneda "${moneda_destino}" no está soportada. Monedas disponibles: CLP, COP, VES, USD, ARS, PEN, BRL, MXN, EUR, UYU` 
                            });
                            return;
                        }
                        
                        const tasa = await obtenerTasaCambioActual(monedaOrigenUpper, monedaDestinoUpper);
                        
                        if (!tasa) {
                            resolve({ error: true, mensaje: "Error al obtener tasa de cambio" });
                            return;
                        }
                        
                        const montoConvertido = monto * tasa;
                        
                        // Nombres de monedas para respuesta más amigable
                        const nombreMonedas = {
                            'CLP': 'Pesos Chilenos',
                            'COP': 'Pesos Colombianos',
                            'VES': 'Bolívares Venezolanos',
                            'USD': 'Dólares',
                            'ARS': 'Pesos Argentinos',
                            'PEN': 'Soles Peruanos',
                            'BRL': 'Reales Brasileños',
                            'MXN': 'Pesos Mexicanos',
                            'EUR': 'Euros',
                            'UYU': 'Pesos Uruguayos'
                        };
                        
                        resolve({
                            monto_origen: monto,
                            moneda_origen: monedaOrigenUpper,
                            nombre_moneda_origen: nombreMonedas[monedaOrigenUpper],
                            monto_convertido: Math.round(montoConvertido * 100) / 100,
                            moneda_destino: monedaDestinoUpper,
                            nombre_moneda_destino: nombreMonedas[monedaDestinoUpper],
                            tasa_cambio: Math.round(tasa * 10000) / 10000,
                            formula: `${monto} ${monedaOrigenUpper} × ${Math.round(tasa * 10000) / 10000} = ${Math.round(montoConvertido * 100) / 100} ${monedaDestinoUpper}`
                        });
                    });
                    break;
                
                case 'buscar_cliente':
                    functionResult = await new Promise((resolve) => {
                        db.get(
                            `SELECT id, nombre, rut, email, telefono, fecha_creacion 
                             FROM clientes 
                             WHERE LOWER(nombre) LIKE LOWER(?)
                             LIMIT 1`,
                            [`%${functionArgs.nombre}%`],
                            (err, cliente) => {
                                if (!err && cliente) {
                                    const faltan = [];
                                    if (!cliente.rut) faltan.push('RUT');
                                    if (!cliente.email) faltan.push('Email');
                                    if (!cliente.telefono) faltan.push('Teléfono');
                                    
                                    resolve({
                                        encontrado: true,
                                        id: cliente.id,
                                        nombre: cliente.nombre,
                                        rut: cliente.rut || null,
                                        email: cliente.email || null,
                                        telefono: cliente.telefono || null,
                                        fecha_creacion: cliente.fecha_creacion,
                                        datos_completos: !!(cliente.rut && cliente.email && cliente.telefono),
                                        faltan: faltan
                                    });
                                } else {
                                    resolve({ encontrado: false, mensaje: `No se encontró cliente con nombre similar a "${functionArgs.nombre}"` });
                                }
                            }
                        );
                    });
                    break;
                    
                case 'listar_operaciones_dia':
                    functionResult = await new Promise((resolve) => {
                        const limite = functionArgs.limite || 10;
                        db.all(
                            `SELECT o.id, o.numero_recibo, o.monto_clp, o.monto_ves, o.tasa, o.fecha,
                                    c.nombre as cliente_nombre,
                                    u.username as operador
                             FROM operaciones o
                             LEFT JOIN clientes c ON o.cliente_id = c.id
                             LEFT JOIN usuarios u ON o.usuario_id = u.id
                             WHERE DATE(o.fecha) = DATE('now', 'localtime')
                             ORDER BY o.fecha DESC
                             LIMIT ?`,
                            [limite],
                            (err, operaciones) => {
                                if (!err && operaciones && operaciones.length > 0) {
                                    resolve({
                                        total: operaciones.length,
                                        operaciones: operaciones.map(op => ({
                                            numero_recibo: op.numero_recibo,
                                            cliente: op.cliente_nombre,
                                            monto_clp: op.monto_clp,
                                            monto_ves: op.monto_ves,
                                            tasa: op.tasa,
                                            operador: op.operador,
                                            hora: new Date(op.fecha).toLocaleTimeString('es-CL', { hour: '2-digit', minute: '2-digit' })
                                        }))
                                    });
                                } else {
                                    resolve({ total: 0, mensaje: "No hay operaciones registradas hoy" });
                                }
                            }
                        );
                    });
                    break;
                    
                case 'consultar_rendimiento':
                    functionResult = await new Promise((resolve) => {
                        db.get(
                            `SELECT 
                                COUNT(*) as total_ops,
                                SUM(monto_clp) as total_clp,
                                SUM(ganancia_neta_clp) as ganancia_total,
                                AVG(ganancia_neta_clp) as ganancia_promedio
                             FROM operaciones
                             WHERE usuario_id = ? 
                             AND strftime('%Y-%m', fecha) = strftime('%Y-%m', 'now')`,
                            [userId],
                            (err, stats) => {
                                if (!err && stats && stats.total_ops > 0) {
                                    resolve({
                                        total_operaciones: stats.total_ops,
                                        total_procesado_clp: Math.round(stats.total_clp),
                                        ganancia_total_clp: Math.round(stats.ganancia_total),
                                        ganancia_promedio_clp: Math.round(stats.ganancia_promedio)
                                    });
                                } else {
                                    resolve({ total_operaciones: 0, mensaje: "No hay operaciones este mes" });
                                }
                            }
                        );
                    });
                    break;
                    
                case 'obtener_estadisticas_clientes':
                    functionResult = await new Promise((resolve) => {
                        // Obtener total de clientes con nombre
                        db.get(
                            `SELECT COUNT(*) as total FROM clientes WHERE nombre IS NOT NULL AND nombre != ''`,
                            [],
                            (err, totalRow) => {
                                const totalClientes = totalRow?.total || 0;
                                
                                // Obtener total de clientes incompletos
                                db.get(
                                    `SELECT COUNT(*) as total FROM clientes 
                                     WHERE (nombre IS NOT NULL AND nombre != '') 
                                     AND (rut IS NULL OR rut = '' OR email IS NULL OR email = '' OR telefono IS NULL OR telefono = '')`,
                                    [],
                                    (err, incompletosRow) => {
                                        const totalIncompletos = incompletosRow?.total || 0;
                                        const totalCompletos = totalClientes - totalIncompletos;
                                        
                                        resolve({
                                            total_clientes: totalClientes,
                                            clientes_completos: totalCompletos,
                                            clientes_incompletos: totalIncompletos,
                                            porcentaje_completos: totalClientes > 0 ? ((totalCompletos / totalClientes) * 100).toFixed(1) : 0,
                                            mensaje: `Total: ${totalClientes} clientes | Completos: ${totalCompletos} | Incompletos: ${totalIncompletos}`
                                        });
                                    }
                                );
                            }
                        );
                    });
                    break;
                    
                case 'listar_clientes_incompletos':
                    functionResult = await new Promise((resolve) => {
                        const limite = functionArgs.limite || 10;
                        
                        // Primero obtener el total de clientes incompletos
                        db.get(
                            `SELECT COUNT(*) as total
                             FROM clientes
                             WHERE (rut IS NULL OR rut = '' OR email IS NULL OR email = '' OR telefono IS NULL OR telefono = '')`,
                            [],
                            (err, countRow) => {
                                const totalIncompletos = countRow?.total || 0;
                                
                                if (totalIncompletos === 0) {
                                    resolve({ total: 0, mensaje: "Todos los clientes tienen datos completos" });
                                    return;
                                }
                                
                                // Luego obtener los ejemplos limitados
                                db.all(
                                    `SELECT id, nombre, rut, email, telefono, fecha_creacion
                                     FROM clientes
                                     WHERE (rut IS NULL OR rut = '' OR email IS NULL OR email = '' OR telefono IS NULL OR telefono = '')
                                     ORDER BY fecha_creacion DESC
                                     LIMIT ?`,
                                    [limite],
                                    (err, clientes) => {
                                        if (!err && clientes && clientes.length > 0) {
                                            resolve({
                                                total: totalIncompletos,
                                                mostrando: clientes.length,
                                                clientes: clientes.map(c => {
                                                    const faltan = [];
                                                    if (!c.rut || c.rut === '') faltan.push('RUT');
                                                    if (!c.email || c.email === '') faltan.push('Email');
                                                    if (!c.telefono || c.telefono === '') faltan.push('Teléfono');
                                                    return {
                                                        nombre: c.nombre,
                                                        faltan: faltan
                                                    };
                                                })
                                            });
                                        } else {
                                            resolve({ total: totalIncompletos, mensaje: "Error al obtener ejemplos" });
                                        }
                                    }
                                );
                            }
                        );
                    });
                    break;
                    
                case 'buscar_operaciones_cliente':
                    functionResult = await new Promise((resolve) => {
                        db.all(
                            `SELECT o.numero_recibo, o.monto_clp, o.monto_ves, o.fecha
                             FROM operaciones o
                             JOIN clientes c ON o.cliente_id = c.id
                             WHERE LOWER(c.nombre) LIKE LOWER(?)
                             ORDER BY o.fecha DESC
                             LIMIT 20`,
                            [`%${functionArgs.nombre_cliente}%`],
                            (err, operaciones) => {
                                if (!err && operaciones && operaciones.length > 0) {
                                    resolve({
                                        total: operaciones.length,
                                        operaciones: operaciones.map(op => ({
                                            numero_recibo: op.numero_recibo,
                                            monto_clp: op.monto_clp,
                                            monto_ves: op.monto_ves,
                                            fecha: new Date(op.fecha).toLocaleDateString('es-CL')
                                        }))
                                    });
                                } else {
                                    resolve({ total: 0, mensaje: `No se encontraron operaciones para cliente "${functionArgs.nombre_cliente}"` });
                                }
                            }
                        );
                    });
                    break;
                    
                case 'consultar_tareas':
                    functionResult = await new Promise((resolve) => {
                        const incluirCompletadas = functionArgs.incluir_completadas || false;
                        const condicionEstado = incluirCompletadas ? '' : `AND estado != 'completada' AND estado != 'cancelada'`;
                        
                        db.all(
                            `SELECT id, titulo, descripcion, prioridad, estado, fecha_vencimiento, fecha_creacion
                             FROM tareas
                             WHERE asignado_a = ? ${condicionEstado}
                             ORDER BY 
                                CASE prioridad 
                                    WHEN 'urgente' THEN 1
                                    WHEN 'alta' THEN 2
                                    WHEN 'normal' THEN 3
                                    WHEN 'baja' THEN 4
                                END,
                                fecha_vencimiento ASC
                             LIMIT 20`,
                            [userId],
                            (err, tareas) => {
                                if (!err && tareas && tareas.length > 0) {
                                    const ahora = new Date();
                                    resolve({
                                        total: tareas.length,
                                        tareas: tareas.map(t => {
                                            const vencimiento = t.fecha_vencimiento ? new Date(t.fecha_vencimiento) : null;
                                            const vencida = vencimiento && vencimiento < ahora;
                                            return {
                                                titulo: t.titulo,
                                                descripcion: t.descripcion || '',
                                                prioridad: t.prioridad,
                                                estado: t.estado,
                                                fecha_vencimiento: vencimiento ? vencimiento.toLocaleDateString('es-CL') : 'Sin fecha límite',
                                                vencida: vencida,
                                                dias_restantes: vencimiento ? Math.ceil((vencimiento - ahora) / (1000 * 60 * 60 * 24)) : null
                                            };
                                        })
                                    });
                                } else {
                                    resolve({ total: 0, mensaje: "No tienes tareas pendientes asignadas" });
                                }
                            }
                        );
                    });
                    break;

                case 'analizar_tarea_cliente_inactivo':
                    functionResult = await new Promise((resolve) => {
                        const nombreCliente = functionArgs.nombre_cliente;
                        const descripcionTarea = functionArgs.descripcion_tarea || '';
                        
                        // Extraer días de inactividad de la descripción
                        const matchDias = descripcionTarea.match(/(\d+)\s*d[ií]as?/i);
                        const diasInactivo = matchDias ? parseInt(matchDias[1]) : 0;
                        
                        // Determinar tipo de acción según días
                        let tipoAccion = '';
                        let requierePromocion = false;
                        
                        if (descripcionTarea.toLowerCase().includes('reducción de actividad')) {
                            tipoAccion = 'reduccion_actividad';
                            requierePromocion = true;
                        } else if (diasInactivo >= 45 || descripcionTarea.toLowerCase().includes('riesgo alto')) {
                            tipoAccion = 'inactivo_promocion';
                            requierePromocion = true;
                        } else if (diasInactivo >= 30) {
                            tipoAccion = 'inactivo_recordatorio';
                            requierePromocion = false;
                        } else {
                            tipoAccion = 'otro';
                            requierePromocion = false;
                        }
                        
                        // Buscar última tasa de compra en el historial de compras (tabla compras)
                        db.get(
                            `SELECT tasa_clp_ves, fecha
                             FROM compras
                             ORDER BY fecha DESC
                             LIMIT 1`,
                            [],
                            (err, ultimaCompra) => {
                                let tasaOriginal = null;
                                let tasaPromocional = null;
                                let mensajeSugerido = '';
                                let fechaCompra = null;
                                
                                // Calcular tasa promocional si hay compra registrada
                                if (!err && ultimaCompra && ultimaCompra.tasa_clp_ves > 0) {
                                    tasaOriginal = ultimaCompra.tasa_clp_ves;
                                    fechaCompra = ultimaCompra.fecha;
                                    // Aplicar 0.33% de DESCUENTO
                                    const descuento = tasaOriginal * 0.0033;
                                    tasaPromocional = parseFloat((tasaOriginal - descuento).toFixed(4));
                                }
                                
                                // Generar mensaje según tipo de acción
                                if (tipoAccion === 'inactivo_recordatorio') {
                                    mensajeSugerido = `Hola ${nombreCliente}! 👋\n\nHemos notado que hace ${diasInactivo} días no realizas una operación con nosotros. 😊\n\nTe esperamos pronto, siempre estamos atentos a tus operaciones. ¡Gracias por ser un cliente constante de DefiOracle! 🇻🇪🇨🇱`;
                                    
                                } else if (requierePromocion && tasaPromocional) {
                                    if (tipoAccion === 'reduccion_actividad') {
                                        mensajeSugerido = `Hola ${nombreCliente}! 👋\n\nHemos notado que últimamente has reducido tu actividad con nosotros. 😢\n\nNo queremos que te vayas, así que tenemos una tasa especial solo para ti: ${tasaPromocional.toFixed(3)} VES por cada CLP 💰\n\n¡Aprovecha esta oferta! Estamos disponibles 08:00-21:00 todos los días. 🇻🇪🇨🇱`;
                                    } else {
                                        mensajeSugerido = `Hola ${nombreCliente}! 👋\n\nTe extrañamos! Hace tiempo que no haces una operación con nosotros. 😢\n\nPorque nos importa tu regreso, tenemos una tasa de regalo especial para ti: ${tasaPromocional.toFixed(3)} VES por cada CLP 💰\n\n¡Esperamos verte pronto! Disponibles 08:00-21:00 todos los días. 🇻🇪🇨🇱`;
                                    }
                                } else if (requierePromocion && !tasaPromocional) {
                                    mensajeSugerido = `⚠️ No se pudo calcular la tasa promocional porque no hay historial de compras de USDT registrado.\n\nSugerencia: Revisa el historial de compras en /admin.html y registra al menos una compra de USDT para poder calcular tasas promocionales automáticamente.`;
                                }
                                
                                resolve({
                                    cliente: nombreCliente,
                                    tipo_accion: tipoAccion,
                                    dias_inactivo: diasInactivo,
                                    requiere_promocion: requierePromocion,
                                    tasa_original: tasaOriginal ? tasaOriginal.toFixed(4) : null,
                                    tasa_promocional: tasaPromocional ? tasaPromocional.toFixed(4) : null,
                                    descuento_aplicado: requierePromocion ? '+0.33%' : 'No aplica',
                                    fecha_ultima_compra: fechaCompra,
                                    mensaje_sugerido: mensajeSugerido
                                });
                            }
                        );
                    });
                    break;

                case 'consultar_monitoreo_tasas':
                    functionResult = await new Promise((resolve) => {
                        // 1. Obtener última verificación de monitoreo
                        db.get(
                            `SELECT * FROM tasas_monitoreo 
                             ORDER BY fecha_verificacion DESC 
                             LIMIT 1`,
                            [],
                            (err, ultimoMonitoreo) => {
                                if (err || !ultimoMonitoreo) {
                                    return resolve({
                                        error: 'No se encontró información de monitoreo',
                                        mensaje: 'El sistema de monitoreo automático aún no ha realizado ninguna verificación.'
                                    });
                                }

                                // 2. Obtener alertas activas
                                db.all(
                                    `SELECT * FROM tasas_alertas 
                                     WHERE estado = 'pendiente' 
                                     ORDER BY fecha_creacion DESC 
                                     LIMIT 5`,
                                    [],
                                    (errAlertas, alertas) => {
                                        // 3. Obtener historial reciente (últimas 10 verificaciones)
                                        db.all(
                                            `SELECT fecha_verificacion, tasa_p2p_250k, tasa_nivel3, diferencia_porcentaje, alerta_generada 
                                             FROM tasas_monitoreo 
                                             ORDER BY fecha_verificacion DESC 
                                             LIMIT 10`,
                                            [],
                                            (errHist, historial) => {
                                                const tiempoTranscurrido = new Date() - new Date(ultimoMonitoreo.fecha_verificacion);
                                                const minutosTranscurridos = Math.floor(tiempoTranscurrido / 60000);

                                                resolve({
                                                    ultima_verificacion: ultimoMonitoreo.fecha_verificacion,
                                                    minutos_desde_ultima_verificacion: minutosTranscurridos,
                                                    tasa_p2p_250k: ultimoMonitoreo.tasa_p2p_250k ? ultimoMonitoreo.tasa_p2p_250k.toFixed(4) : null,
                                                    tasa_nivel3_guardada: ultimoMonitoreo.tasa_nivel3 ? ultimoMonitoreo.tasa_nivel3.toFixed(4) : null,
                                                    diferencia_porcentaje: ultimoMonitoreo.diferencia_porcentaje ? ultimoMonitoreo.diferencia_porcentaje.toFixed(2) : null,
                                                    alerta_generada: ultimoMonitoreo.alerta_generada === 1,
                                                    alertas_activas: alertas ? alertas.length : 0,
                                                    detalles_alertas: alertas || [],
                                                    historial_reciente: historial || [],
                                                    estado: ultimoMonitoreo.alerta_generada === 1 
                                                        ? '⚠️ ALERTA: Tasas muy altas detectadas' 
                                                        : '✅ Tasas normales',
                                                    recomendacion: ultimoMonitoreo.alerta_generada === 1
                                                        ? `La tasa P2P (${ultimoMonitoreo.tasa_p2p_250k?.toFixed(4)}) es más baja que nuestra tasa guardada (${ultimoMonitoreo.tasa_nivel3?.toFixed(4)}), lo que significa que nuestras tasas están muy altas. Se recomienda ajustar.`
                                                        : 'Las tasas están en un rango normal, no se requiere acción.'
                                                });
                                            }
                                        );
                                    }
                                );
                            }
                        );
                    });
                    break;
            }
            
            // Agregar el resultado de la función a los mensajes
            messages.push(responseMessage);
            messages.push({
                role: 'function',
                name: functionName,
                content: JSON.stringify(functionResult)
            });
            
            // Segunda llamada a OpenAI para que genere respuesta final con los datos
            response = await axios.post('https://api.openai.com/v1/chat/completions', {
                model: 'gpt-3.5-turbo',
                messages: messages,
                max_tokens: 500,
                temperature: 0.8
            }, {
                headers: {
                    'Authorization': `Bearer ${OPENAI_API_KEY}`,
                    'Content-Type': 'application/json'
                }
            });
            
            return response.data.choices[0].message.content;
        }
        
        // Si no llamó ninguna función, retornar respuesta directa
        return responseMessage.content;
        
    } catch (error) {
        console.error('❌ Error API OpenAI:', error.response?.data || error.message);
        
        // Si el error es de API key inválida, dar mensaje específico
        if (error.response?.data?.error?.code === 'invalid_api_key') {
            return `❌ **Configuración pendiente**\n\nLo siento, la API key de OpenAI no está configurada correctamente.\n\n**Administrador:** Configure la variable de entorno \`OPENAI_API_KEY\` en Render con una key válida de https://platform.openai.com/api-keys`;
        }
        
        // Si falla OpenAI por otro motivo, respuesta genérica humanizada
        return `Entiendo tu consulta, ${username}. Como asistente de DefiOracle.cl puedo ayudarte con conversiones, datos bancarios, tareas, y más. ¿Podrías darme más detalles de lo que necesitas?\n\n_Nota: El servicio de IA está experimentando problemas técnicos._`;
    }
}

// Endpoint para ver logs del sistema (solo master)
app.get('/api/logs/sistema', apiAuth, onlyMaster, (req, res) => {
    const { tipo, limite = 50 } = req.query;
    
    const logs = [];
    const promises = [];
    
    // Log de operaciones recientes
    if (!tipo || tipo === 'operaciones') {
        promises.push(new Promise((resolve) => {
            db.all(`
                SELECT o.*, c.nombre as cliente_nombre, u.username as operador
                FROM operaciones o
                LEFT JOIN clientes c ON o.cliente_id = c.id
                LEFT JOIN usuarios u ON o.usuario_id = u.id
                ORDER BY o.fecha DESC, o.id DESC
                LIMIT ?
            `, [parseInt(limite)], (err, rows) => {
                if (!err && rows) {
                    rows.forEach(op => {
                        logs.push({
                            tipo: 'operacion',
                            fecha: op.fecha,
                            mensaje: `💰 Operación #${op.numero_recibo || op.id} - ${op.cliente_nombre} - ${op.monto_clp} CLP (${op.operador})`,
                            detalles: op
                        });
                    });
                }
                resolve();
            });
        }));
    }
    
    // Log de notificaciones
    if (!tipo || tipo === 'notificaciones') {
        promises.push(new Promise((resolve) => {
            db.all(`
                SELECT n.*, u.username
                FROM notificaciones n
                LEFT JOIN usuarios u ON n.usuario_id = u.id
                ORDER BY n.fecha_creacion DESC
                LIMIT ?
            `, [parseInt(limite)], (err, rows) => {
                if (!err && rows) {
                    rows.forEach(not => {
                        logs.push({
                            tipo: 'notificacion',
                            fecha: not.fecha_creacion,
                            mensaje: `🔔 ${not.titulo} - ${not.username} - ${not.leida ? 'Leída' : 'No leída'}`,
                            detalles: not
                        });
                    });
                }
                resolve();
            });
        }));
    }
    
    // Log de alertas
    if (!tipo || tipo === 'alertas') {
        promises.push(new Promise((resolve) => {
            db.all(`
                SELECT a.*, c.nombre as cliente_nombre
                FROM alertas a
                LEFT JOIN clientes c ON a.cliente_id = c.id
                ORDER BY a.fecha_creacion DESC
                LIMIT ?
            `, [parseInt(limite)], (err, rows) => {
                if (!err && rows) {
                    rows.forEach(alerta => {
                        logs.push({
                            tipo: 'alerta',
                            fecha: alerta.fecha_creacion,
                            mensaje: `⚠️ ${alerta.tipo} - ${alerta.cliente_nombre} - Severidad: ${alerta.severidad}`,
                            detalles: alerta
                        });
                    });
                }
                resolve();
            });
        }));
    }
    
    // Log de clientes con datos incompletos
    if (!tipo || tipo === 'clientes_incompletos') {
        promises.push(new Promise((resolve) => {
            db.all(`
                SELECT id, nombre, rut, email, telefono, fecha_creacion
                FROM clientes
                WHERE (rut IS NULL OR rut = '' OR 
                       email IS NULL OR email = '' OR 
                       telefono IS NULL OR telefono = '')
                ORDER BY fecha_creacion DESC
                LIMIT ?
            `, [parseInt(limite)], (err, rows) => {
                if (!err && rows) {
                    rows.forEach(cliente => {
                        const faltantes = [];
                        if (!cliente.rut || cliente.rut.trim() === '') faltantes.push('RUT');
                        if (!cliente.email || cliente.email.trim() === '') faltantes.push('Email');
                        if (!cliente.telefono || cliente.telefono.trim() === '') faltantes.push('Teléfono');
                        
                        logs.push({
                            tipo: 'cliente_incompleto',
                            fecha: cliente.fecha_creacion,
                            mensaje: `📋 Cliente "${cliente.nombre}" - Faltan: ${faltantes.join(', ')}`,
                            detalles: { ...cliente, datos_faltantes: faltantes }
                        });
                    });
                }
                resolve();
            });
        }));
    }
    
    Promise.all(promises).then(() => {
        // Ordenar todos los logs por fecha descendente
        logs.sort((a, b) => new Date(b.fecha) - new Date(a.fecha));
        res.json({
            total: logs.length,
            logs: logs.slice(0, parseInt(limite))
        });
    });
});

// =================================================================
// 🤖 SISTEMA DE MONITOREO PROACTIVO DEL CHATBOT
// =================================================================

async function generarMensajesProactivos() {
    console.log('🔍 Ejecutando monitoreo proactivo...');
    
    try {
        // Obtener todos los usuarios activos
        const usuarios = await new Promise((resolve) => {
            db.all('SELECT id, username, role FROM usuarios', (err, rows) => {
                if (err) return resolve([]);
                resolve(rows);
            });
        });

        for (const usuario of usuarios) {
            const mensajesGenerados = [];
            const ahora = new Date();
            const hoyStr = ahora.toISOString().split('T')[0];

            // 1️⃣ CELEBRACIÓN - Operaciones del día
            const operacionesHoy = await new Promise((resolve) => {
                db.all(`
                    SELECT COUNT(*) as total, SUM(monto_clp) as volumen
                    FROM operaciones
                    WHERE usuario_id = ? AND DATE(fecha) = DATE('now', 'localtime')
                `, [usuario.id], (err, rows) => {
                    if (err || !rows || !rows[0]) return resolve(null);
                    resolve(rows[0]);
                });
            });
            console.log(`📊 ${usuario.username} - Operaciones hoy:`, operacionesHoy);

            if (operacionesHoy && operacionesHoy.total >= 5) {
                mensajesGenerados.push({
                    tipo: 'celebracion',
                    mensaje: `🎉 ¡Vas genial hoy! Ya llevas ${operacionesHoy.total} operaciones y has procesado $${Math.round(operacionesHoy.volumen).toLocaleString()} CLP. ¡Sigue así!`,
                    prioridad: 'normal',
                    contexto: JSON.stringify({ operaciones: operacionesHoy.total, volumen: operacionesHoy.volumen })
                });
                console.log(`✅ Agregado mensaje: celebracion`);
            }

            // 2️⃣ RECORDATORIO - Tareas pendientes urgentes
            const tareasPendientes = await new Promise((resolve) => {
                db.all(`
                    SELECT COUNT(*) as total
                    FROM tareas
                    WHERE asignado_a = ? 
                    AND estado IN ('pendiente', 'en_progreso')
                    AND prioridad IN ('alta', 'urgente')
                    AND (fecha_vencimiento IS NULL OR DATE(fecha_vencimiento) <= DATE('now', '+2 days'))
                `, [usuario.id], (err, rows) => {
                    if (err || !rows || !rows[0]) return resolve(null);
                    resolve(rows[0]);
                });
            });

            if (tareasPendientes && tareasPendientes.total > 0) {
                mensajesGenerados.push({
                    tipo: 'recordatorio',
                    mensaje: `⏰ Hey! Tienes ${tareasPendientes.total} tarea(s) importante(s) pendiente(s). ¿Quieres que te las muestre?`,
                    prioridad: 'alta',
                    contexto: JSON.stringify({ tareas_pendientes: tareasPendientes.total })
                });
            }

            // 3️⃣ ALERTA - Clientes con datos incompletos que operaron recientemente
            const clientesIncompletos = await new Promise((resolve) => {
                db.all(`
                    SELECT DISTINCT c.nombre, c.id
                    FROM clientes c
                    JOIN operaciones o ON c.id = o.cliente_id
                    WHERE o.usuario_id = ?
                    AND DATE(o.fecha) >= DATE('now', '-7 days')
                    AND (c.rut IS NULL OR c.rut = '' OR c.email IS NULL OR c.email = '' OR c.telefono IS NULL OR c.telefono = '')
                    LIMIT 3
                `, [usuario.id], (err, rows) => {
                    if (err || !rows) return resolve([]);
                    resolve(rows);
                });
            });

            if (clientesIncompletos.length > 0) {
                const nombres = clientesIncompletos.map(c => c.nombre).slice(0, 2).join(', ');
                const resto = clientesIncompletos.length > 2 ? ` y ${clientesIncompletos.length - 2} más` : '';
                mensajesGenerados.push({
                    tipo: 'alerta',
                    mensaje: `⚠️ Ojo: ${nombres}${resto} operaron esta semana pero les faltan datos. ¿Los actualizamos?`,
                    prioridad: 'normal',
                    contexto: JSON.stringify({ clientes: clientesIncompletos.map(c => c.nombre) })
                });
            }

            // 4️⃣ SUGERENCIA - Clientes con datos completos que operaron recientemente
            const clientesCompletosRecientes = await new Promise((resolve) => {
                db.all(`
                    SELECT DISTINCT c.nombre, c.id
                    FROM clientes c
                    JOIN operaciones o ON c.id = o.cliente_id
                    WHERE o.usuario_id = ?
                    AND DATE(o.fecha) >= DATE('now', '-7 days')
                    AND c.rut IS NOT NULL AND c.rut != ''
                    AND c.email IS NOT NULL AND c.email != ''
                    AND c.telefono IS NOT NULL AND c.telefono != ''
                    LIMIT 3
                `, [usuario.id], (err, rows) => {
                    if (err || !rows) return resolve([]);
                    resolve(rows);
                });
            });
            console.log(`✅ ${usuario.username} - Clientes completos recientes:`, clientesCompletosRecientes.length);

            if (clientesCompletosRecientes.length > 0) {
                mensajesGenerados.push({
                    tipo: 'sugerencia',
                    mensaje: `✅ ¡Genial! ${clientesCompletosRecientes[0].nombre} ya tiene todos los datos completos. Un cliente menos en pendientes 🎯`,
                    prioridad: 'baja',
                    contexto: JSON.stringify({ cliente: clientesCompletosRecientes[0].nombre })
                });
                console.log(`✅ Agregado mensaje: sugerencia`);
            }

            // 5️⃣ INFORMATIVO - Rendimiento semanal
            const esLunes = ahora.getDay() === 1; // 0 = Domingo, 1 = Lunes
            if (esLunes && ahora.getHours() >= 9 && ahora.getHours() <= 10) {
                const rendimientoSemanal = await new Promise((resolve) => {
                    db.get(`
                        SELECT COUNT(*) as ops, SUM(monto_clp) as volumen
                        FROM operaciones
                        WHERE usuario_id = ?
                        AND DATE(fecha) >= DATE('now', '-7 days')
                    `, [usuario.id], (err, row) => {
                        if (err || !row) return resolve(null);
                        resolve(row);
                    });
                });

                if (rendimientoSemanal && rendimientoSemanal.ops > 0) {
                    mensajesGenerados.push({
                        tipo: 'informativo',
                        mensaje: `📊 Resumen semanal: ${rendimientoSemanal.ops} operaciones, volumen de $${Math.round(rendimientoSemanal.volumen).toLocaleString()} CLP. ¡Buen trabajo!`,
                        prioridad: 'baja',
                        contexto: JSON.stringify({ ops: rendimientoSemanal.ops, volumen: rendimientoSemanal.volumen })
                    });
                }
            }

            // Guardar mensajes generados en la base de datos
            console.log(`📋 Usuario ${usuario.username}: ${mensajesGenerados.length} mensajes candidatos`);
            for (const msg of mensajesGenerados) {
                // Verificar que no exista un mensaje similar reciente (últimas 6 horas)
                const mensajeDuplicado = await new Promise((resolve) => {
                    db.get(`
                        SELECT id FROM chatbot_mensajes_proactivos
                        WHERE usuario_id = ?
                        AND tipo = ?
                        AND datetime(fecha_creacion) >= datetime('now', '-6 hours')
                    `, [usuario.id, msg.tipo], (err, row) => {
                        if (err) return resolve(null);
                        resolve(row);
                    });
                });

                if (!mensajeDuplicado) {
                    await new Promise((resolve) => {
                        db.run(`
                            INSERT INTO chatbot_mensajes_proactivos 
                            (usuario_id, tipo, mensaje, contexto, prioridad, fecha_creacion)
                            VALUES (?, ?, ?, ?, ?, ?)
                        `, [usuario.id, msg.tipo, msg.mensaje, msg.contexto, msg.prioridad, ahora.toISOString()],
                        (err) => {
                            if (!err) {
                                console.log(`💬 Mensaje proactivo generado para ${usuario.username}: ${msg.tipo}`);
                            } else {
                                console.error(`❌ Error guardando mensaje ${msg.tipo}:`, err.message);
                            }
                            resolve();
                        });
                    });
                } else {
                    console.log(`⏭️ Mensaje tipo "${msg.tipo}" ya existe (ID ${mensajeDuplicado.id}), omitiendo...`);
                }
            }
        }
    } catch (error) {
        console.error('❌ Error en monitoreo proactivo:', error);
    }
}

// Endpoint para obtener mensajes proactivos
app.get('/api/chatbot/mensajes-proactivos', apiAuth, (req, res) => {
    const userId = req.session.user.id;
    console.log(`🔍 GET /api/chatbot/mensajes-proactivos - userId: ${userId}`);
    
    db.all(`
        SELECT * FROM chatbot_mensajes_proactivos
        WHERE usuario_id = ? AND mostrado = 0
        ORDER BY prioridad DESC, fecha_creacion DESC
        LIMIT 3
    `, [userId], (err, mensajes) => {
        if (err) {
            console.error('Error obteniendo mensajes proactivos:', err);
            return res.json({ mensajes: [] });
        }
        
        console.log(`📨 Mensajes encontrados para userId ${userId}:`, mensajes.length);
        res.json({ mensajes: mensajes || [] });
    });
});

// Endpoint para marcar mensaje proactivo como mostrado
app.post('/api/chatbot/mensajes-proactivos/:id/mostrado', apiAuth, (req, res) => {
    const { id } = req.params;
    
    db.run(`
        UPDATE chatbot_mensajes_proactivos
        SET mostrado = 1, fecha_mostrado = ?
        WHERE id = ?
    `, [new Date().toISOString(), id], (err) => {
        if (err) {
            console.error('Error marcando mensaje como mostrado:', err);
            return res.status(500).json({ error: 'Error al actualizar' });
        }
        res.json({ success: true });
    });
});

// Ejecutar monitoreo cada 30 segundos (para pruebas - cambiar a 10 min en producción)
const INTERVALO_MONITOREO = 30 * 1000; // 30 segundos
let intervaloMonitoreo = null;

function iniciarMonitoreoProactivo() {
    // Ejecutar inmediatamente
    setTimeout(generarMensajesProactivos, 3000); // 3 segundos después del inicio
    
    // Luego cada 30 segundos
    intervaloMonitoreo = setInterval(generarMensajesProactivos, INTERVALO_MONITOREO);
    console.log('🤖 Sistema de monitoreo proactivo iniciado (cada 30 segundos)');
}

// =================================================================
// FIN: SISTEMA DE MONITOREO PROACTIVO
// =================================================================

// Iniciar el servidor solo después de que las migraciones se hayan completado
runMigrations()
    .then(() => {
        app.listen(PORT, () => {
            console.log(`🚀 Servidor corriendo en http://localhost:${PORT}`);
            iniciarMonitoreoProactivo(); // ✅ Iniciar monitoreo proactivo
            
            // ✅ Iniciar monitoreo automático de tasas P2P
            console.log('🔍 Iniciando monitoreo automático de tasas P2P...');
            monitorearTasas(); // Ejecutar primera vez inmediatamente
            setInterval(monitorearTasas, 10 * 60 * 1000); // Cada 10 minutos
            console.log('⏰ Monitoreo de tasas configurado: cada 10 minutos');
        });
    })
    .catch(err => {
        console.error("❌ No se pudo iniciar el servidor debido a un error en la migración de la base de datos:", err);
        process.exit(1); // Detiene la aplicación si la BD no se puede inicializar
    });
