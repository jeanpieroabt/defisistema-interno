// servidor.js
// =======================================================
// Defi Oracle – Backend (Auth, Envíos, Histórico, Tasas, Compras, Operadores)
// =======================================================

const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');
const session = require('express-session');
const path = require('path');

const DB_PATH = path.join(process.env.DATA_DIR || '.', 'database.db');

const app = express();
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

  db.run(`CREATE TABLE IF NOT EXISTS compras(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    usuario_id INTEGER NOT NULL,
    clp_invertido REAL NOT NULL,
    ves_obtenido REAL NOT NULL,
    tasa_clp_ves REAL NOT NULL,
    fecha TEXT NOT NULL,
    FOREIGN KEY(usuario_id) REFERENCES usuarios(id)
  )`);
  
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
  return local.toISOString().slice(0, 7) + '-01'; 
};
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
const readConfigValue = (clave) => {
  return new Promise((resolve, reject) => {
    db.get(`SELECT valor FROM configuracion WHERE clave=?`, [clave], (e, row) => {
      if (e) return reject(e);
      resolve(row ? Number(row.valor) : 0);
    });
  });
};

// -------------------- Páginas y Autenticación --------------------
app.get('/', (req, res) => res.redirect('/login.html'));
app.get('/app.html', pageAuth, (req, res) => res.sendFile(path.join(__dirname, 'app.html')));
app.get('/admin.html', pageAuth, onlyMaster, (req, res) => res.sendFile(path.join(__dirname, 'admin.html')));
app.get('/historico.html', pageAuth, (req, res) => res.sendFile(path.join(__dirname, 'historico.html')));

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


// --- API PARA BUSCAR CLIENTES (AUTOCOMPLETAR) ---
app.get('/api/clientes/search', apiAuth, (req, res) => {
    const term = req.query.term;
    if (!term || term.length < 2) {
        return res.json([]);
    }
    const sql = `SELECT nombre FROM clientes WHERE nombre LIKE ? ORDER BY nombre LIMIT 10`;
    const params = [`%${term}%`];
    db.all(sql, params, (err, rows) => {
        if (err) return res.status(500).json({ message: 'Error al buscar clientes.' });
        res.json(rows.map(row => row.nombre));
    });
});

// --- API PARA BUSCAR USUARIOS (AUTOCOMPLETAR) ---
app.get('/api/usuarios/search', apiAuth, onlyMaster, (req, res) => {
    const term = req.query.term;
    if (!term) {
        return res.json([]);
    }
    const sql = `SELECT username FROM usuarios WHERE username LIKE ? ORDER BY username LIMIT 10`;
    const params = [`%${term}%`];
    db.all(sql, params, (err, rows) => {
        if (err) return res.status(500).json({ message: 'Error al buscar usuarios.' });
        res.json(rows.map(row => row.username));
    });
});

// -------------------- APIs de KPIs y Dashboard --------------------
const calcularCostoVesPorClp = (callback) => {
    db.get(
        `SELECT IFNULL(SUM(clp_invertido),0) as totalClp, IFNULL(SUM(ves_obtenido),0) as totalVes FROM compras`, 
        [], 
        (e, row) => {
            if (e) return callback(e, 0);
            callback(null, row.totalVes > 0 ? row.totalClp / row.totalVes : 0);
        }
    );
};

app.get('/api/dashboard', apiAuth, (req, res) => { 
  const userRole = req.session.user?.role;
  const queries = [readConfigValue('saldoVesOnline').then(saldo => ({ saldoVesOnline: saldo }))];
  
  if (userRole === 'master') {
    queries.push(new Promise(resolve => {
        Promise.all([
            new Promise(res => calcularCostoVesPorClp((e, costo) => res({ costoVesPorClp: costo }))),
            new Promise(res => db.get(`SELECT IFNULL(SUM(monto_clp),0) as totalClp, IFNULL(SUM(monto_ves),0) as totalVes FROM operaciones WHERE date(fecha)=date('now','localtime')`, [], (e, row) => res({ ventasDia: row }))),
            new Promise(res => db.get(`SELECT (IFNULL(SUM(ves_obtenido),0) / IFNULL(SUM(clp_invertido), 1)) AS tasa FROM compras`, [], (e, row) => res({ tasaCompraPromedio: row?.tasa || 0 }))),
            new Promise(res => db.all(`SELECT clave, valor FROM configuracion WHERE clave IN ('capitalInicialClp', 'totalGananciaAcumuladaClp')`, (e, rows) => {
                const saldos = {};
                rows?.forEach(r => saldos[r.clave] = Number(r.valor));
                res({ saldos });
            })),
        ]).then(([{costoVesPorClp}, {ventasDia}, {tasaCompraPromedio}, {saldos}]) => {
            const { totalClp, totalVes } = ventasDia;
            const tasaVentaPromedio = totalClp > 0 ? totalVes / totalClp : 0;
            const costoTotalVesDia = totalVes * costoVesPorClp; 
            const gananciaBrutaDia = totalClp - costoTotalVesDia;
            const comisionDia = totalClp * 0.003;
            const gananciaNetaDia = gananciaBrutaDia - comisionDia;
            const capitalTotalClp = (saldos.capitalInicialClp || 0) + (saldos.totalGananciaAcumuladaClp || 0);
            resolve({
                gananciaBruta: gananciaNetaDia,
                totalClpEnviado: totalClp,
                margenBruto: totalClp ? (gananciaNetaDia / totalClp) * 100 : 0,
                tasaCompraPromedio,
                tasaVentaPromedio,
                capitalTotalClp,
                totalGananciaAcumuladaClp: saldos.totalGananciaAcumuladaClp || 0,
            });
        }).catch(e => { console.error("Error en dashboard master:", e); resolve({}); });
    }));
  }
  
  Promise.all(queries).then(([saldoData, masterData = {}]) => {
      const tasaCompra = masterData.tasaCompraPromedio || 0;
      const valorClpInventario = tasaCompra > 0 ? saldoData.saldoVesOnline / tasaCompra : 0;
      res.json({ ...saldoData, ...masterData, saldoDisponibleClp: valorClpInventario });
  }).catch(e => res.status(500).json({ message: 'Error al obtener dashboard.' }));
});

// ... (El resto de las APIs de operaciones, usuarios, compras, etc. siguen aquí sin cambios) ...

// -------------------- Start --------------------
app.listen(PORT, () => console.log(`http://localhost:${PORT}`));