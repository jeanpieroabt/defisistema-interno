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
                    db.get(`SELECT IFNULL(SUM(monto_clp),0) as totalClpEnviado, IFNULL(SUM(monto_ves),0) as totalVesEnviado FROM operaciones WHERE date(fecha)=date(?)`, [hoy], (e, rowOps) => {
                        if (e) { console.error(e); return resolve({totalClpEnviadoDia: 0, totalVesEnviadoDia: 0}); }
                        resolve({ totalClpEnviadoDia: rowOps.totalClpEnviado, totalVesEnviadoDia: rowOps.totalVesEnviado });
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
            const { totalClpEnviadoDia, totalVesEnviadoDia } = ventas;
            const { tasaCompraPromedio } = tasas;
            const { capitalInicialClp = 0, totalGananciaAcumuladaClp = 0 } = saldos;
            const tasaVentaPromedio = totalVesEnviadoDia > 0 ? totalVesEnviadoDia / totalClpEnviadoDia : 0;
            const costoTotalVesEnviadoClpDia = totalVesEnviadoDia * costoClpPorVes;
            const gananciaBrutaDia = totalClpEnviadoDia - costoTotalVesEnviadoClpDia;
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
            SELECT DISTINCT cliente_id FROM operaciones WHERE fecha >= ?
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

// Iniciar el servidor solo después de que las migraciones se hayan completado
runMigrations()
    .then(() => {
        app.listen(PORT, () => console.log(`🚀 Servidor corriendo en http://localhost:${PORT}`));
    })
    .catch(err => {
        console.error("❌ No se pudo iniciar el servidor debido a un error en la migración de la base de datos:", err);
        process.exit(1); // Detiene la aplicación si la BD no se puede inicializar
    });
