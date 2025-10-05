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

db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS usuarios(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('master','operador'))
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS clientes(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT UNIQUE NOT NULL,
    fecha_creacion TEXT NOT NULL
  )`);

  // fecha = YYYY-MM-DD (local)
  db.run(`CREATE TABLE IF NOT EXISTS operaciones(
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
    FOREIGN KEY(usuario_id) REFERENCES usuarios(id),
    FOREIGN KEY(cliente_id) REFERENCES clientes(id)
  )`);

  // Nueva tabla de Compras Simplificada: CLP -> VES
  db.run(`CREATE TABLE IF NOT EXISTS compras(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    usuario_id INTEGER NOT NULL,
    clp_invertido REAL NOT NULL,
    ves_obtenido REAL NOT NULL,
    tasa_clp_ves REAL NOT NULL,
    fecha TEXT NOT NULL,
    FOREIGN KEY(usuario_id) REFERENCES usuarios(id)
  )`);
  
  // Key-Value para tasas, capital y estados de saldo
  db.run(`CREATE TABLE IF NOT EXISTS configuracion(
    clave TEXT PRIMARY KEY,
    valor TEXT
  )`);
});

// Semilla mínima
db.get(`SELECT COUNT(*) c FROM usuarios`, async (err, row) => {
  if (!row || row.c === 0) {
    const hash = await bcrypt.hash('master123', 10);
    db.run(
      `INSERT INTO usuarios(username,password,role) VALUES (?,?,?)`,
      ['master', hash, 'master']
    );
    console.log('Usuario semilla: master/master123');
  }
});

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
  // Formato: YYYY-MM-01
  return local.toISOString().slice(0, 7) + '-01'; 
};

// Helper para actualizar un valor de estado en la tabla configuracion
const updateConfigState = (clave, delta, cb) => {
    db.get(`SELECT valor FROM configuracion WHERE clave=?`, [clave], (err, row) => {
        if(err) return cb(err);
        const currentValue = row ? Number(row.valor) : 0;
        const newValue = currentValue + delta;
        
        db.run(
            `INSERT INTO configuracion(clave,valor) VALUES(?,?) ON CONFLICT(clave) DO UPDATE SET valor=excluded.valor`,
            [clave, String(newValue)],
            cb
        );
    });
};
// Helper para leer valores de configuración
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
app.get('/admin.html', pageAuth, onlyMaster, (req, res) =>
  res.sendFile(path.join(__dirname, 'admin.html'))
);
app.get('/historico.html', pageAuth, (req, res) => 
  res.sendFile(path.join(__dirname, 'historico.html'))
);
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

// -------------------- Rutas de búsqueda añadidas --------------------
app.get('/api/clientes/search', apiAuth, (req, res) => {
    const term = String(req.query.term || '').trim();
    if (!term || term.length < 2) return res.json([]);
    db.all(
      `SELECT nombre FROM clientes WHERE nombre LIKE ? ORDER BY nombre LIMIT 10`,
      [`%${term}%`],
      (err, rows) => {
        if (err) return res.status(500).json({ message: 'Error al buscar clientes.' });
        res.json((rows || []).map(r => r.nombre));
      }
    );
});

app.get('/api/usuarios/search', apiAuth, onlyMaster, (req, res) => {
    const term = String(req.query.term || '').trim();
    if (!term || term.length < 1) return res.json([]);
    db.all(
      `SELECT username FROM usuarios WHERE username LIKE ? ORDER BY username LIMIT 10`,
      [`%${term}%`],
      (err, rows) => {
        if (err) return res.status(500).json({ message: 'Error al buscar usuarios.' });
        res.json((rows || []).map(r => r.username));
      }
    );
});

// -------------------- APIs de Costos y KPIs --------------------
const calcularCostoVesPorClp = (fecha, callback) => {
    db.get(
        `SELECT
            IFNULL(SUM(clp_invertido),0) as totalClpInvertido,
            IFNULL(SUM(ves_obtenido),0) as totalVesComprado
         FROM compras`, 
        [], 
        (e, rowCompras) => {
            if (e) return callback(e, 0);
            const totalClpInvertido = rowCompras.totalClpInvertido;
            const totalVesComprado = rowCompras.totalVesComprado;
            let costoVesPorClp = totalVesComprado > 0 ? totalClpInvertido / totalVesComprado : 0;
            if (costoVesPorClp === 0) {
                 db.get(`SELECT valor FROM configuracion WHERE clave='capitalCostoVesPorClp'`, [], (err, row) => {
                    const costoInicial = row ? Number(row.valor) : 0;
                    callback(null, costoInicial);
                 });
            } else {
                 callback(null, costoVesPorClp);
            }
        }
    );
};

app.get('/api/dashboard', apiAuth, (req, res) => { 
  const userRole = req.session.user?.role;
  const hoy = hoyLocalYYYYMMDD();
  
  const queries = [
    readConfigValue('saldoVesOnline').then(saldo => ({ saldoVesOnline: saldo })),
    
    new Promise(resolve => {
        if (userRole !== 'master') return resolve({});

        Promise.all([
            new Promise(resolve => calcularCostoVesPorClp(hoy, (e, costo) => resolve({ costoVesPorClp: costo || 0 }))),
            new Promise(resolve => {
                db.get(`SELECT IFNULL(SUM(monto_clp),0) as totalClpEnviado, IFNULL(SUM(monto_ves),0) as totalVesEnviado FROM operaciones WHERE date(fecha)=date('now','localtime')`, [], (e, rowOps) => resolve({ totalClpEnviadoDia: rowOps?.totalClpEnviado || 0, totalVesEnviadoDia: rowOps?.totalVesEnviado || 0 }));
            }),
            new Promise(resolve => {
                db.get(`SELECT IFNULL(SUM(ves_obtenido),0) as totalVesComprado, IFNULL(SUM(clp_invertido),0) as totalClpInvertido, (IFNULL(SUM(ves_obtenido),0) / IFNULL(SUM(clp_invertido), 1)) AS tasaCompra FROM compras`, [], (e, rowCompras) => resolve({ tasaCompraPromedio: rowCompras?.tasaCompra || 0 }));
            }),
            new Promise(resolve => {
                db.all(`SELECT clave, valor FROM configuracion WHERE clave IN ('capitalInicialClp', 'totalGananciaAcumuladaClp')`, (e, rows) => {
                    const result = {};
                    rows?.forEach(r => result[r.clave] = Number(r.valor) || 0);
                    resolve(result);
                });
            }),
        ]).then(([costo, ventas, tasas, saldos]) => {
            const { costoVesPorClp } = costo;
            const { totalClpEnviadoDia, totalVesEnviadoDia } = ventas;
            const { tasaCompraPromedio } = tasas;
            const { capitalInicialClp, totalGananciaAcumuladaClp } = saldos;
            const tasaVentaPromedio = totalClpEnviadoDia > 0 ? totalVesEnviadoDia / totalClpEnviadoDia : 0;
            const costoTotalVesEnviadoClpDia = totalVesEnviadoDia * costoVesPorClp; 
            const gananciaBrutaDia = totalClpEnviadoDia - costoTotalVesEnviadoClpDia;
            const comisionDelDia = totalClpEnviadoDia * 0.003;
            const gananciaNetaDelDia = gananciaBrutaDia - comisionDelDia;
            const capitalTotalClp = (capitalInicialClp || 0) + totalGananciaAcumuladaClp;
            resolve({ totalClpEnviado: totalClpEnviadoDia, gananciaBruta: gananciaNetaDelDia, margenBruto: totalClpEnviadoDia ? (gananciaNetaDelDia / totalClpEnviadoDia) * 100 : 0, tasaCompraPromedio: tasaCompraPromedio, tasaVentaPromedio: tasaVentaPromedio, capitalInicialClp: capitalInicialClp || 0, totalGananciaAcumuladaClp: totalGananciaAcumuladaClp, capitalTotalClp: capitalTotalClp, saldoDisponibleClp: 0 });
        }).catch(e => { console.error("Error en Master Dashboard Queries:", e); resolve({}); });
    }),
  ];
  
  Promise.all(queries).then(results => {
      const saldoVes = results[0].saldoVesOnline;
      const masterData = results[1] || {};
      const tasaCompra = masterData.tasaCompraPromedio || 0;
      const valorClpDelInventarioVes = tasaCompra > 0 ? saldoVes / tasaCompra : 0;
      masterData.saldoDisponibleClp = valorClpDelInventarioVes;
      const resData = { saldoVesOnline: saldoVes, ...masterData };
      res.json(resData);
  }).catch(e => {
      console.error("Error general en dashboard:", e);
      res.status(500).json({ message: 'Error al obtener datos del dashboard.' });
  });
});

// -------------------- Contador Mensual del Operador --------------------
app.get('/api/monthly-sales', apiAuth, (req, res) => {
  const user = req.session.user;
  const inicioMes = inicioMesLocalYYYYMMDD();
  let sql = `SELECT IFNULL(SUM(monto_clp), 0) AS totalClpMensual FROM operaciones WHERE date(fecha) >= date(?)`;
  const params = [inicioMes];
  if (user.role !== 'master') {
    sql += ` AND usuario_id = ?`;
    params.push(user.id);
  }
  db.get(sql, params, (err, row) => {
    if (err) return res.status(500).json({ message: 'Error al leer ventas mensuales' });
    res.json({ totalClpMensual: row.totalClpMensual });
  });
});

// -------------------- Operaciones --------------------
app.get('/api/operaciones', apiAuth, (req, res) => {
  const user = req.session.user;
  let sql = `
    SELECT op.*, u.username AS operador, c.nombre AS cliente_nombre
    FROM operaciones op
    JOIN usuarios u ON op.usuario_id = u.id
    JOIN clientes c ON op.cliente_id = c.id`;
  const params = [];

  if (user.role !== 'master' && !req.query.historico) {
    sql += ` WHERE op.usuario_id = ? AND date(op.fecha) = date('now','localtime')`;
    params.push(user.id);
  }

  const where = [];
  if (req.query.startDate) { where.push(`date(op.fecha) >= date(?)`); params.push(req.query.startDate); }
  if (req.query.endDate)   { where.push(`date(op.fecha) <= date(?)`); params.push(req.query.endDate); }
  if (req.query.cliente)   { where.push(`c.nombre LIKE ?`); params.push(`%${req.query.cliente}%`); }
  
  // =======================================================
  // INICIO DE LA MODIFICACIÓN
  // =======================================================
  if (user.role === 'master' && req.query.operador) {
    // Si es master y filtra por operador, se aplica el filtro
    where.push(`u.username LIKE ?`); params.push(`%${req.query.operador}%`);
  } else if (user.role === 'operador' && req.query.historico) {
    // Si es un operador consultando el histórico, forzamos a que solo vea sus propias operaciones.
    where.push(`op.usuario_id = ?`); params.push(user.id);
  }
  // =======================================================
  // FIN DE LA MODIFICACIÓN
  // =======================================================

  if (where.length) {
    sql += (sql.includes('WHERE') ? ' AND ' : ' WHERE ') + where.join(' AND ');
  }

  sql += ` ORDER BY op.id DESC`;

  db.all(sql, params, (err, rows) => {
    if (err) return res.status(500).json({ message: 'Error al leer operaciones' });
    res.json(rows || []);
  });
});

app.post('/api/operaciones', apiAuth, (req, res) => {
  const { cliente_nombre, monto_clp, monto_ves, tasa, observaciones, fecha } = req.body;
  const fechaGuardado = fecha && /^\d{4}-\d{2}-\d{2}$/.test(fecha) ? fecha : hoyLocalYYYYMMDD();
  const montoVes = Number(monto_ves || 0);
  const montoClp = Number(monto_clp || 0);
  const comisionVes = montoVes * 0.003;
  const vesTotalDescontar = montoVes + comisionVes; 

  readConfigValue('saldoVesOnline').then(saldoVes => {
      if (vesTotalDescontar > saldoVes) return res.status(400).json({ message: 'Saldo VES online insuficiente.' });
      calcularCostoVesPorClp(fechaGuardado, (e, costoVesPorClp) => {
          if (e) return res.status(500).json({ message: 'Error al calcular costo' });
          const costoVesEnviadoClp = montoVes * costoVesPorClp;
          const gananciaBruta = montoClp - costoVesEnviadoClp;
          const comisionClp = montoClp * 0.003;
          const gananciaNeta = gananciaBruta - comisionClp;
          db.get(`SELECT id FROM clientes WHERE nombre=?`, [cliente_nombre], (err, c) => {
            const guardar = (cliente_id) => {
              db.run(`INSERT INTO operaciones(usuario_id,cliente_id,fecha,monto_clp,monto_ves,tasa,observaciones,costo_clp,comision_ves) VALUES (?,?,?,?,?,?,?,?,?)`, [req.session.user.id, cliente_id, fechaGuardado, montoClp, montoVes, Number(tasa || 0), observaciones || '', costoVesEnviadoClp, comisionVes], function (e) {
                  if (e) return res.status(500).json({ message: 'Error al guardar' });
                  updateConfigState('saldoVesOnline', -vesTotalDescontar, (err) => { 
                      if(err) console.error("Error al restar VES:", err);
                      updateConfigState('totalGananciaAcumuladaClp', gananciaNeta, (err) => {
                          if(err) console.error("Error al sumar ganancia:", err);
                          updateConfigState('capitalAcumulativoClp', gananciaNeta, (err) => {
                              if(err) console.error("Error al sumar ganancia al capital CLP:", err);
                              res.json({ id: this.lastID });
                          });
                      });
                  });
                }
              );
            };
            if (c) return guardar(c.id);
            db.run(`INSERT INTO clientes(nombre,fecha_creacion) VALUES (?,?)`, [cliente_nombre, new Date().toISOString()], function (e2) {
                if (e2) return res.status(500).json({ message: 'Error cliente' });
                guardar(this.lastID);
              }
            );
          });
      });
  }).catch(e => {
      console.error("Error validando saldo VES:", e);
      res.status(500).json({ message: 'Error al obtener saldo VES.' });
  });
});

app.put('/api/operaciones/:id', apiAuth, (req, res) => {
    const operacionId = req.params.id;
    const user = req.session.user;
    const { cliente_nombre, monto_clp, tasa, observaciones, fecha } = req.body;
    const montoClp = Number(monto_clp || 0);
    const tasaOp = Number(tasa || 0);
    const montoVes = montoClp * tasaOp;
    db.get('SELECT * FROM operaciones WHERE id = ?', [operacionId], (err, op) => {
        if (err || !op) { return res.status(404).json({ message: 'Operación no encontrada.' }); }
        const esMaster = user.role === 'master';
        const esSuOperacion = op.usuario_id === user.id;
        const esDeHoy = op.fecha === hoyLocalYYYYMMDD();
        if (!esMaster && !(esSuOperacion && esDeHoy)) { return res.status(403).json({ message: 'No tienes permiso para editar esta operación.' }); }
        calcularCostoVesPorClp(fecha, (e, costoVesPorClp) => {
            if (e) return res.status(500).json({ message: 'Error al recalcular el costo.' });
            const nuevoCostoClp = montoVes * costoVesPorClp;
            db.get(`SELECT id FROM clientes WHERE nombre=?`, [cliente_nombre], (err, c) => {
                if (err || !c) return res.status(400).json({ message: 'Cliente no encontrado.' });
                const sql = `UPDATE operaciones SET cliente_id=?, fecha=?, monto_clp=?, monto_ves=?, tasa=?, observaciones=?, costo_clp=? WHERE id = ?`;
                db.run(sql, [c.id, fecha, montoClp, montoVes, tasaOp, observaciones || '', nuevoCostoClp, operacionId], function (e) {
                    if (e) return res.status(500).json({ message: 'Error al actualizar la operación.' });
                    res.json({ message: 'Operación actualizada con éxito. Los saldos globales no se reajustan automáticamente.' });
                });
            });
        });
    });
});

app.get('/api/operaciones/export', apiAuth, (req, res) => {
  const user = req.session.user;
  let sql = `SELECT op.fecha, u.username AS operador, c.nombre AS cliente, op.monto_clp, op.tasa, op.monto_ves, op.observaciones, op.costo_clp, op.comision_ves FROM operaciones op JOIN usuarios u ON op.usuario_id = u.id JOIN clientes c ON op.cliente_id = c.id`;
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
    const header = ['Fecha', 'Operador', 'Cliente', 'Monto CLP', 'Costo CLP', 'Comision VES', 'Tasa', 'Monto VES', 'Obs.'];
    const csv = [header.join(',')]
      .concat(rows.map((r) => [r.fecha, `"${r.operador}"`, `"${r.cliente}"`, r.monto_clp, r.costo_clp, r.comision_ves, r.tasa, r.monto_ves, `"${(r.observaciones || '').replace(/"/g, '""')}"`].join(',')))
      .join('\n');
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="historico_envios.csv"');
    res.send('\uFEFF' + csv);
  });
});

// -------------------- API de Resumen Histórico --------------------
app.get('/api/historico/resumen', apiAuth, onlyMaster, (req, res) => {
    const { startDate, endDate } = req.query;
    let sql = `SELECT c.nombre AS cliente_nombre, IFNULL(SUM(op.monto_clp), 0) AS total_clp_recibido, IFNULL(SUM(op.costo_clp), 0) AS total_costo_clp FROM operaciones op JOIN clientes c ON op.cliente_id = c.id`;
    const where = []; const params = [];
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
            return { cliente_nombre: row.cliente_nombre, total_clp_recibido: row.total_clp_recibido, total_costo_clp: row.total_costo_clp, ganancia_bruta: gananciaNeta };
        });
        res.json(resumen);
    });
});

// -------------------- API de Rendimiento por Operador --------------------
app.get('/api/rendimiento/operadores', apiAuth, onlyMaster, (req, res) => {
    const { startDate, endDate } = req.query;
    let sql = `SELECT u.username AS operador_nombre, IFNULL(COUNT(op.id), 0) AS total_operaciones, IFNULL(COUNT(DISTINCT op.cliente_id), 0) AS clientes_unicos, IFNULL(SUM(op.monto_clp), 0) AS total_clp_enviado FROM operaciones op JOIN usuarios u ON op.usuario_id = u.id`;
    const where = []; const params = [];
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

// -------------------- Tasas (configuracion) --------------------
const readConfig = (clave, cb) => db.get(`SELECT valor FROM configuracion WHERE clave=?`, [clave], (e, row) => cb(e, row ? row.valor : null));
const upsertConfig = (clave, valor, cb) => db.run(`INSERT INTO configuracion(clave,valor) VALUES(?,?) ON CONFLICT(clave) DO UPDATE SET valor=excluded.valor`, [clave, valor], cb);

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

// -------------------- Capital Inicial Config --------------------
app.post('/api/config/capital', apiAuth, onlyMaster, (req, res) => {
    const { capitalInicialClp } = req.body;
    const capClp = Number(capitalInicialClp) || 0;
    upsertConfig('capitalInicialClp', String(capClp), (err) => {
        if (err) { console.error("Error al guardar capital inicial:", err); return res.status(500).json({ message: 'Error al actualizar el capital inicial.' }); }
        res.json({ message: 'Capital inicial actualizado con éxito.' });
    });
});
app.get('/api/config/capital', apiAuth, onlyMaster, (req, res) => {
    const keys = ['capitalInicialClp', 'saldoInicialVes', 'capitalCostoVesPorClp'];
    Promise.all(keys.map(readConfigValue))
        .then(([capitalInicialClp, saldoInicialVes, costoVesPorClp]) => res.json({ capitalInicialClp, saldoInicialVes, costoVesPorClp }))
        .catch(e => res.status(500).json({ message: 'Error al leer configuración' }));
});

// -------------------- APIs de Gestión de Usuarios --------------------
app.get('/api/usuarios', apiAuth, onlyMaster, (req, res) => {
    db.all(`SELECT id, username, role FROM usuarios`, [], (err, rows) => {
        if (err) return res.status(500).json({ message: 'Error al listar usuarios' });
        res.json(rows || []);
    });
});

app.put('/api/usuarios/:id', apiAuth, onlyMaster, async (req, res) => {
    const userId = req.params.id;
    const { username, password, role } = req.body;
    let sql = `UPDATE usuarios SET username = ?, role = ? WHERE id = ?`;
    let params = [username, role, userId];
    if (password) {
        const hash = await bcrypt.hash(password, 10);
        sql = `UPDATE usuarios SET username = ?, role = ?, password = ? WHERE id = ?`;
        params = [username, role, hash, userId];
    }
    db.run(sql, params, function (e) {
        if (e) return res.status(500).json({ message: 'Error al actualizar usuario' });
        res.json({ message: 'Usuario actualizado con éxito.' });
    });
});

// -------------------- Operadores (crear) --------------------
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

// -------------------- APIs de Gestión de Compras --------------------
app.get('/api/compras', apiAuth, onlyMaster, (req, res) => {
    db.all(`SELECT * FROM compras ORDER BY fecha DESC`, [], (err, rows) => {
        if (err) return res.status(500).json({ message: 'Error al listar compras' });
        res.json(rows || []);
    });
});

app.post('/api/compras', apiAuth, onlyMaster, (req, res) => {
    const { clp_invertido, ves_obtenido } = req.body;
    const clp = Number(clp_invertido || 0);
    const ves = Number(ves_obtenido || 0);
    if (clp <= 0 || ves <= 0) { return res.status(400).json({ message: 'Los montos de CLP y VES deben ser mayores a cero.' }); }
    const tasa = clp / ves;
    const fecha = hoyLocalYYYYMMDD();
    db.run(`INSERT INTO compras(usuario_id, clp_invertido, ves_obtenido, tasa_clp_ves, fecha) VALUES (?, ?, ?, ?, ?)`, [req.session.user.id, clp, ves, tasa, fecha], function(err) {
        if (err) return res.status(500).json({ message: 'Error al guardar la compra.' });
        updateConfigState('saldoVesOnline', ves, (errVes) => {
            if (errVes) console.error("Error al actualizar saldo VES:", errVes);
            updateConfigState('capitalAcumulativoClp', -clp, (errClp) => {
                if (errClp) console.error("Error al actualizar capital CLP:", errClp);
                res.json({ message: 'Compra registrada con éxito.' });
            });
        });
    });
});

app.put('/api/compras/:id', apiAuth, onlyMaster, (req, res) => {
    const compraId = req.params.id;
    const { clp_invertido, ves_obtenido, fecha } = req.body;
    const clp = Number(clp_invertido || 0);
    const ves = Number(ves_obtenido || 0);
    const tasa = clp / (ves || 1);
    const sql = `UPDATE compras SET clp_invertido = ?, ves_obtenido = ?, tasa_clp_ves = ?, fecha = ? WHERE id = ?`;
    db.run(sql, [clp, ves, tasa, fecha, compraId], function (e) {
        if (e) return res.status(500).json({ message: 'Error al actualizar compra' });
        res.json({ message: 'Compra actualizada con éxito. Recargue la página para ver los KPIs actualizados.' });
    });
});

// -------------------- Start --------------------
app.listen(PORT, () => console.log(`http://localhost:${PORT}`));