// servidor.js
// =======================================================
// Defi Oracle – Backend (Auth, Envíos, Histórico, Tasas, Compras, Operadores)
// =======================================================

const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');
const session = require('express-session');
const path = require('path');

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
app.get('/api/clientes', apiAuth, (req, res) => {
    db.all('SELECT * FROM clientes ORDER BY nombre', [], (err, rows) => {
        if (err) return res.status(500).json({ message: 'Error al obtener clientes.' });
        res.json(rows);
    });
});
app.get('/api/clientes/:id', apiAuth, (req, res) => {
    db.get('SELECT * FROM clientes WHERE id = ?', [req.params.id], (err, row) => {
        if (err) return res.status(500).json({ message: 'Error al obtener el cliente.' });
        if (!row) return res.status(404).json({ message: 'Cliente no encontrado.' });
        res.json(row);
    });
});
app.post('/api/clientes', apiAuth, (req, res) => {
    const { nombre, rut, email, telefono, datos_bancarios } = req.body;
    if (!nombre) return res.status(400).json({ message: 'El nombre es obligatorio.' });
    const sql = `INSERT INTO clientes (nombre, rut, email, telefono, datos_bancarios, fecha_creacion) VALUES (?, ?, ?, ?, ?, ?)`;
    db.run(sql, [nombre, rut, email, telefono, datos_bancarios, hoyLocalYYYYMMDD()], function(err) {
        if (err) {
            if (err.message.includes('UNIQUE constraint failed')) return res.status(400).json({ message: 'Ya existe un cliente con ese nombre.' });
            return res.status(500).json({ message: 'Error al crear el cliente.' });
        }
        res.status(201).json({ id: this.lastID, message: 'Cliente creado con éxito.' });
    });
});
app.put('/api/clientes/:id', apiAuth, (req, res) => {
    const { nombre, rut, email, telefono, datos_bancarios } = req.body;
    if (!nombre) return res.status(400).json({ message: 'El nombre es obligatorio.' });
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

app.get('/api/dashboard', async (req, res) => {
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
                        if (err || rateHoy > 0) return resolve({ tasaCompraPromedio: rateHoy || 0 });
                        const ayer = new Date(); ayer.setDate(ayer.getDate() - 1);
                        getAvgPurchaseRate(ayer.toISOString().slice(0, 10), (errAyer, rateAyer) => resolve({ tasaCompraPromedio: rateAyer || 0 }));
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
      const findOrCreateCliente = new Promise((resolve, reject) => {
          db.get(`SELECT id FROM clientes WHERE nombre = ?`, [cliente_nombre], (err, cliente) => {
              if (err) return reject(new Error('Error al buscar cliente.'));
              if (cliente) return resolve(cliente.id);
              db.run(`INSERT INTO clientes(nombre, fecha_creacion) VALUES (?,?)`, [cliente_nombre, hoyLocalYYYYMMDD()], function(err) {
                  if (err) return reject(new Error('Error al crear nuevo cliente.'));
                  resolve(this.lastID);
              });
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
                
                db.get(`SELECT id FROM clientes WHERE nombre=?`, [cliente_nombre], (err, cliente) => {
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

// Iniciar el servidor solo después de que las migraciones se hayan completado
runMigrations()
    .then(() => {
        app.listen(PORT, () => console.log(`🚀 Servidor corriendo en http://localhost:${PORT}`));
    })
    .catch(err => {
        console.error("❌ No se pudo iniciar el servidor debido a un error en la migración de la base de datos:", err);
        process.exit(1); // Detiene la aplicación si la BD no se puede inicializar
    });