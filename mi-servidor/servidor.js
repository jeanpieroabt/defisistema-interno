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


// API PARA BUSCAR CLIENTES (AUTOCOMPLETAR)
app.get('/api/clientes/search', apiAuth, (req, res) => {
    const term = req.query.term;
    if (!term || term.length < 2) {
        return res.json([]);
    }
    
    const sql = `
        SELECT nombre FROM clientes 
        WHERE nombre LIKE ? 
        ORDER BY nombre
        LIMIT 10`;
    
    const params = [`%${term}%`];

    db.all(sql, params, (err, rows) => {
        if (err) {
            return res.status(500).json({ message: 'Error al buscar clientes.' });
        }
        res.json(rows.map(row => row.nombre));
    });
});


// -------------------- APIs de Costos y KPIs --------------------

// ... (El resto del código de servidor.js sigue exactamente igual)
// ...

// -------------------- Start --------------------
app.listen(PORT, () => console.log(`http://localhost:${PORT}`));