// servidor.js
// =======================================================
// Defi Oracle - Backend (Auth, Envíos, Histórico, Tasas, Compras, Operadores)
// =======================================================

// Cargar variables de entorno desde .env
require('dotenv').config();

const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');
const session = require('express-session');
const path = require('path');
const axios = require('axios');
const crypto = require('crypto');
const cors = require('cors');
const fs = require('fs');
const multer = require('multer');
const compression = require('compression');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const openaiHelper = require('./openai-helper');

// Configuracion de multer para uploads con validación estricta
const ALLOWED_IMAGE_TYPES = ['image/jpeg', 'image/png', 'image/webp', 'image/gif'];
const ALLOWED_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.webp', '.gif'];
const upload = multer({
    storage: multer.memoryStorage(),
    limits: {
        fileSize: 5 * 1024 * 1024, // 5MB máximo
        files: 2 // máximo 2 archivos por request
    },
    fileFilter: (req, file, cb) => {
        // Validar MIME type
        if (!ALLOWED_IMAGE_TYPES.includes(file.mimetype)) {
            return cb(new Error('Solo se permiten imágenes (JPEG, PNG, WebP, GIF)'));
        }
        // Validar extensión del archivo
        const ext = path.extname(file.originalname).toLowerCase();
        if (!ALLOWED_EXTENSIONS.includes(ext)) {
            return cb(new Error('Extensión de archivo no permitida'));
        }
        // Sanitizar nombre del archivo
        file.originalname = file.originalname.replace(/[^a-zA-Z0-9._-]/g, '_');
        cb(null, true);
    }
});

// =================================================================
// FUNCIONES DE VALIDACIÓN Y SANITIZACIÓN
// =================================================================
function sanitizarTexto(str, maxLength = 255) {
    if (typeof str !== 'string') return '';
    return str.trim().slice(0, maxLength).replace(/[<>]/g, '');
}

function validarEmail(email) {
    if (!email) return true; // campo opcional
    const re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return typeof email === 'string' && email.length <= 255 && re.test(email);
}

function validarTelefono(tel) {
    if (!tel) return true; // campo opcional
    const re = /^[+\d\s()-]{6,20}$/;
    return typeof tel === 'string' && re.test(tel);
}

function validarMontoPositivo(monto) {
    const num = Number(monto);
    return !isNaN(num) && isFinite(num) && num > 0;
}

function validarMoneda(moneda) {
    const permitidas = ['CLP', 'VES', 'USD', 'COP', 'ARS', 'PEN', 'BRL', 'MXN', 'EUR'];
    return permitidas.includes(moneda);
}

// Clave secreta para tokens de la app cliente
const JWT_SECRET = process.env.JWT_SECRET || 'defi-oracle-jwt-secret-key-' + crypto.randomBytes(16).toString('hex');

// =================================================================
// CONFIGURACI'N BOT TELEGRAM PARA NOTIFICACIONES
// =================================================================
let TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || ''; // Token del bot
let TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';     // Chat/Grupo ID para notificaciones

// Función para cargar configuración de Telegram desde BD
async function cargarConfigTelegram() {
    try {
        const [botToken, chatId] = await Promise.all([
            dbGet("SELECT valor FROM configuracion WHERE clave = 'telegram_bot_token'"),
            dbGet("SELECT valor FROM configuracion WHERE clave = 'telegram_chat_id'")
        ]);
        if (botToken?.valor) TELEGRAM_BOT_TOKEN = botToken.valor;
        if (chatId?.valor) TELEGRAM_CHAT_ID = chatId.valor;
        if (TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID) {
            console.log('... Configuración de Telegram cargada');
        }
    } catch (error) {
        console.log('️ No se pudo cargar configuración de Telegram');
    }
}

// Funcion para enviar notificacion a Telegram
async function enviarNotificacionTelegram(mensaje, parseMode = 'HTML', botones = null) {
    // Cargar config actualizada de BD
    await cargarConfigTelegram();

    if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
        console.log('Telegram no configurado - Notificacion omitida');
        return false;
    }

    try {
        const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
        const payload = {
            chat_id: TELEGRAM_CHAT_ID,
            text: mensaje,
            parse_mode: parseMode
        };
        
        if (botones) {
            payload.reply_markup = { inline_keyboard: botones };
        }
        
        const response = await axios.post(url, payload);
        console.log('Notificacion Telegram enviada');
        return response.data;
    } catch (error) {
        console.error('Error enviando notificacion Telegram:', error.message);
        return false;
    }
}

// Editar mensaje de Telegram (actualizar texto y quitar/cambiar botones)
async function editarMensajeTelegram(messageId, nuevoTexto, botones = []) {
    await cargarConfigTelegram();
    if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID || !messageId) return false;
    try {
        await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/editMessageText`, {
            chat_id: TELEGRAM_CHAT_ID,
            message_id: messageId,
            text: nuevoTexto,
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: botones }
        });
        return true;
    } catch (error) {
        console.error('Error editando mensaje Telegram:', error.message);
        return false;
    }
}

// Helper: convertir documento_tipo a inicial para formato bancario
function inicialDocumento(tipo) {
    const map = { 'cedula': 'V', 'rut': 'V', 'pasaporte': 'P', 'dni': 'E' };
    return map[(tipo || '').toLowerCase()] || 'V';
}

// Enviar pedido al bot pagador de Bancamiga vía API HTTP
async function enviarAlBotPagador(pedidoId) {
    try {
        // Leer config del bot pagador
        const [urlConfig, tokenConfig] = await Promise.all([
            dbGet("SELECT valor FROM configuracion WHERE clave = 'pagador_api_url'"),
            dbGet("SELECT valor FROM configuracion WHERE clave = 'pagador_api_token'")
        ]);
        const apiUrl = urlConfig?.valor;
        const apiToken = tokenConfig?.valor;

        if (!apiUrl || !apiToken) {
            console.log('[BOT PAGADOR] No configurado (falta URL o token de la API)');
            return false;
        }

        // Obtener datos completos del pedido + beneficiario
        const pedido = await dbGet(
            `SELECT s.*, b.nombre_completo, b.banco as benef_banco, b.numero_cuenta as benef_cuenta,
                    b.tipo_cuenta as benef_tipo_cuenta,
                    b.documento_tipo as benef_doc_tipo, b.documento_numero as benef_doc_numero,
                    b.telefono as benef_telefono
             FROM solicitudes_transferencia s
             JOIN beneficiarios b ON s.beneficiario_id = b.id
             WHERE s.id = ?`, [pedidoId]
        );

        if (!pedido) {
            console.error(`[BOT PAGADOR] Pedido #${pedidoId} no encontrado`);
            return false;
        }

        const docInicial = inicialDocumento(pedido.benef_doc_tipo);
        const docNumero = (pedido.benef_doc_numero || '').replace(/[^0-9]/g, '');
        const nombre = pedido.nombre_completo || 'Sin nombre';
        const cuenta = (pedido.benef_cuenta || '').replace(/[^0-9]/g, '');
        const montoDestino = pedido.monto_destino || 0;
        const telefono = (pedido.benef_telefono || '').replace(/[^0-9]/g, '');
        // Detectar pago móvil: si el beneficiario tiene teléfono y no tiene cuenta bancaria
        const esPagoMovil = telefono.length > 0 && cuenta.length === 0;
        // Para pago móvil el código de banco viene del campo banco del beneficiario
        const codigoBanco = esPagoMovil
            ? (pedido.benef_banco || '').replace(/[^0-9]/g, '').substring(0, 4)
            : cuenta.substring(0, 4);

        // Construir transferencia según tipo (pago móvil vs transferencia a terceros)
        let transferencia;
        if (esPagoMovil) {
            // Pago Móvil: banco + teléfono + doc + nombre + monto
            transferencia = {
                tipo: 'pago_movil',
                banco_destino: codigoBanco,
                telefono: telefono,
                doc_tipo: docInicial,
                doc_numero: docNumero,
                beneficiario: nombre,
                monto: String(montoDestino),
                concepto: `Pedido #${pedidoId}`
            };
        } else {
            // Transferencia a terceros: cuenta completa + doc + nombre + monto
            transferencia = {
                tipo: 'transferencia',
                banco_destino: codigoBanco,
                cuenta_destino: cuenta,
                doc_tipo: docInicial,
                doc_numero: docNumero,
                beneficiario: nombre,
                monto: String(montoDestino),
                concepto: `Pedido #${pedidoId}`
            };
        }

        console.log(`[BOT PAGADOR] Enviando pedido #${pedidoId} (${esPagoMovil ? 'PAGO MÓVIL' : 'TRANSFERENCIA'}) a API: ${JSON.stringify(transferencia)}`);

        // POST /ejecutar
        const response = await axios.post(`${apiUrl}/ejecutar`, {
            transferencias: [transferencia],
            origen: 'defi_conciliacion'
        }, {
            headers: {
                'Authorization': `Bearer ${apiToken}`,
                'Content-Type': 'application/json'
            },
            timeout: 15000
        });

        const data = response.data;
        console.log(`[BOT PAGADOR] Pedido #${pedidoId} → Lote: ${data.lote_id} | Status: ${data.status}`);

        // Guardar lote_id en el pedido para hacer polling después
        await dbRun(
            `UPDATE solicitudes_transferencia SET notas_operador = ? WHERE id = ?`,
            [`lote:${data.lote_id}`, pedidoId]
        );

        // Iniciar polling de resultados en background
        pollingResultadoPago(pedidoId, data.lote_id, apiUrl, apiToken);

        return true;
    } catch (error) {
        console.error(`[BOT PAGADOR] Error enviando pedido #${pedidoId}:`, error.message);

        // Notificar error en Telegram
        await enviarNotificacionTelegram(
            `❌ <b>Error enviando al Bot Pagador</b>\n\n` +
            `📋 Pedido #${pedidoId}\n` +
            `⚠️ ${error.message}\n\n` +
            `🔧 Requiere intervención manual.`
        );
        return false;
    }
}

// Polling de resultados del bot pagador
async function pollingResultadoPago(pedidoId, loteId, apiUrl, apiToken) {
    const MAX_INTENTOS = 60; // 60 * 10s = 10 minutos máximo
    let intentos = 0;

    const intervalo = setInterval(async () => {
        intentos++;
        try {
            const response = await axios.get(`${apiUrl}/resultados/${loteId}`, {
                headers: { 'Authorization': `Bearer ${apiToken}` },
                timeout: 10000
            });

            const data = response.data;
            console.log(`[BOT PAGADOR] Polling #${intentos} pedido #${pedidoId} → ${data.status}`);

            // Estados finales
            if (data.status === 'completed' || data.status === 'failed' || data.status === 'cancelled') {
                clearInterval(intervalo);

                if (data.status === 'completed' && data.resultados?.length > 0) {
                    const resultado = data.resultados[0];
                    if (resultado.success) {
                        // PAGO EXITOSO - Completar pedido
                        const referencia = resultado.referencia || null;
                        await dbRun(
                            `UPDATE solicitudes_transferencia
                             SET estado = 'completada',
                                 referencia_pago = ?,
                                 fecha_completada = ?
                             WHERE id = ?`,
                            [referencia, new Date().toISOString(), pedidoId]
                        );

                        console.log(`[BOT PAGADOR] ✅ Pedido #${pedidoId} COMPLETADO - Ref: ${referencia}`);

                        // Obtener datos para notificación
                        const pedido = await dbGet(
                            `SELECT s.*, c.nombre as cliente_nombre, b.nombre_completo as beneficiario_nombre
                             FROM solicitudes_transferencia s
                             JOIN clientes_app c ON s.cliente_app_id = c.id
                             JOIN beneficiarios b ON s.beneficiario_id = b.id
                             WHERE s.id = ?`, [pedidoId]
                        );

                        // Editar mensaje original en Telegram
                        if (pedido?.telegram_message_id) {
                            await editarMensajeTelegram(
                                pedido.telegram_message_id,
                                `✅ <b>PEDIDO COMPLETADO</b>\n\n` +
                                `📋 Pedido #${pedidoId}\n` +
                                `👤 ${pedido.cliente_nombre}\n` +
                                `🏦 ${pedido.beneficiario_nombre}\n` +
                                `💰 $${Math.round(pedido.monto_origen).toLocaleString('es-CL')} CLP → Bs. ${Math.round(pedido.monto_destino).toLocaleString('es-VE')}\n\n` +
                                `🔗 Referencia: <b>${referencia || 'N/A'}</b>\n` +
                                `🤖 Procesado automáticamente\n` +
                                `🕐 ${new Date().toLocaleString('es-VE', { timeZone: 'America/Caracas' })}`,
                                []
                            );
                        }

                        // Notificación
                        await enviarNotificacionTelegram(
                            `✅ <b>Pago Completado</b>\n\n` +
                            `📋 Pedido #${pedidoId}\n` +
                            `👤 ${pedido?.beneficiario_nombre || ''}\n` +
                            `💰 Bs. ${Math.round(pedido?.monto_destino || 0).toLocaleString('es-VE')}\n` +
                            `🔗 Referencia: <b>${referencia || 'N/A'}</b>`
                        );
                    } else {
                        // Transferencia falló dentro del lote completado
                        const errorMsg = resultado.error || 'Error desconocido';
                        console.log(`[BOT PAGADOR] ❌ Pedido #${pedidoId} FALLÓ: ${errorMsg}`);
                        await enviarNotificacionTelegram(
                            `❌ <b>Pago Fallido</b>\n\n` +
                            `📋 Pedido #${pedidoId}\n` +
                            `⚠️ ${errorMsg}\n\n` +
                            `🔧 Pedido sigue en <b>procesando</b>. Requiere intervención manual.`
                        );
                    }
                } else if (data.status === 'failed') {
                    const errorMsg = data.resultados?.[0]?.error || 'Error desconocido';
                    console.log(`[BOT PAGADOR] ❌ Lote ${loteId} FALLÓ: ${errorMsg}`);
                    await enviarNotificacionTelegram(
                        `❌ <b>Pago Fallido</b>\n\n` +
                        `📋 Pedido #${pedidoId} | Lote: ${loteId}\n` +
                        `⚠️ ${errorMsg}\n\n` +
                        `🔧 Pedido sigue en <b>procesando</b>. Requiere intervención manual.`
                    );
                } else if (data.status === 'cancelled') {
                    console.log(`[BOT PAGADOR] ⚠️ Lote ${loteId} CANCELADO`);
                    await enviarNotificacionTelegram(
                        `⚠️ <b>Pago Cancelado</b>\n\n` +
                        `📋 Pedido #${pedidoId} | Lote: ${loteId}\n` +
                        `🔧 Pedido sigue en <b>procesando</b>. Requiere intervención manual.`
                    );
                }
            }

            if (intentos >= MAX_INTENTOS) {
                clearInterval(intervalo);
                console.log(`[BOT PAGADOR] ⏰ Timeout polling pedido #${pedidoId} (${MAX_INTENTOS} intentos)`);
                await enviarNotificacionTelegram(
                    `⏰ <b>Timeout Bot Pagador</b>\n\n` +
                    `📋 Pedido #${pedidoId} | Lote: ${loteId}\n` +
                    `⚠️ No se obtuvo resultado en 10 minutos.\n` +
                    `🔧 Verificar manualmente.`
                );
            }
        } catch (error) {
            console.error(`[BOT PAGADOR] Error polling pedido #${pedidoId}:`, error.message);
            if (intentos >= MAX_INTENTOS) {
                clearInterval(intervalo);
            }
        }
    }, 10000); // Cada 10 segundos
}


















// Enviar foto a Telegram via sendPhoto
async function enviarFotoTelegram(photoBuffer, caption, filename = 'comprobante.jpg') {
    await cargarConfigTelegram();
    if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
        console.log('Telegram no configurado - Foto omitida');
        return false;
    }
    try {
        const FormData = require('form-data');
        const form = new FormData();
        form.append('chat_id', TELEGRAM_CHAT_ID);
        form.append('photo', photoBuffer, { filename, contentType: 'image/jpeg' });
        if (caption) {
            form.append('caption', caption);
            form.append('parse_mode', 'HTML');
        }
        const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendPhoto`;
        const response = await axios.post(url, form, { headers: form.getHeaders() });
        console.log('Foto Telegram enviada');
        return response.data.ok;
    } catch (error) {
        console.error('Error enviando foto Telegram:', error.message);
        return false;
    }
}

// Función para notificar nueva solicitud de la app
// Función para enmascarar datos sensibles (muestra primeros y últimos caracteres)
function enmascararDato(dato, visibles = 4) {
    if (!dato || dato.length <= visibles * 2) return dato || '';
    return dato.slice(0, visibles) + '***' + dato.slice(-visibles);
}

async function notificarNuevaSolicitud(solicitud, comprobanteBuffer = null) {
    // Determinar tipo de entrega
    let tipoEntrega = '🏦 TRANSFERENCIA';
    if (solicitud.tipo_cuenta === 'pago_movil') tipoEntrega = '📲 PAGO MÓVIL';

    // Determinar método de pago (cómo pagó el cliente en Chile)
    const metodoPago = solicitud.metodo_pago || 'transferencia_bancaria';
    const esCajaVecina = metodoPago === 'caja_vecina';
    
    // Valores con fallback para evitar undefined
    const cuenta = solicitud.beneficiario_cuenta || 'No registrada';
    const cedula = solicitud.beneficiario_cedula || 'No registrada';
    const tipoCuenta = solicitud.beneficiario_tipo_cuenta || 'No especificado';
    const nombreBeneficiario = solicitud.beneficiario_nombre || 'Sin nombre';
    const banco = solicitud.beneficiario_banco || 'Sin banco';
    const telefonoBenef = solicitud.beneficiario_telefono || '';
    
    const esPagoMovil = solicitud.tipo_cuenta === 'pago_movil';

    // Construir sección de beneficiario según tipo de entrega
    const seccionBeneficiario = esPagoMovil
        ? [
            `━━━━━━ ${tipoEntrega} ━━━━━━`,
            `👤 ${nombreBeneficiario}`,
            `🪪 Cédula: ${cedula}`,
            `🏦 Banco: ${banco}`,
            `📞 Teléfono: ${telefonoBenef || 'No registrado'}`
        ]
        : [
            `━━━━━━ ${tipoEntrega} ━━━━━━`,
            `👤 ${nombreBeneficiario}`,
            `🪪 Cédula: ${cedula}`,
            `🏦 ${banco}`,
            `📋 Tipo: ${tipoCuenta}`,
            `💳 Cuenta: ${cuenta}`,
            telefonoBenef ? `📞 Tel: ${telefonoBenef}` : null
        ];

    const mensaje = [
        `🔔 NUEVO PEDIDO #${solicitud.id} - APP CLIENTE`,
        esCajaVecina ? '🏪 <b>PAGO: CAJA VECINA</b>' : null,
        '',
        '━━━━━━ 👤 CLIENTE ━━━━━━',
        `📛 ${solicitud.cliente_nombre || 'Sin nombre'}`,
        `📧 ${enmascararDato(solicitud.cliente_email || 'Sin email', 3)}`,
        solicitud.cliente_telefono ? `📱 ${solicitud.cliente_telefono}` : null,
        solicitud.cliente_documento ? `🪪 ${(solicitud.cliente_documento_tipo || 'DOC').toUpperCase()}: ${solicitud.cliente_documento}` : null,
        '',
        '━━━━━━ 💰 OPERACIÓN ━━━━━━',
        `💵 Envía: $${Number(solicitud.monto_origen || 0).toLocaleString('es-CL')} ${solicitud.moneda_origen || 'CLP'}`,
        `💴 Recibe: ${Number(solicitud.monto_destino || 0).toLocaleString('es-VE')} ${solicitud.moneda_destino || 'VES'}`,
        `📊 Tasa: ${solicitud.tasa_aplicada || 'N/A'}`,
        '',
        ...seccionBeneficiario,
        '',
        `⏰ ${new Date().toLocaleString('es-VE', { timeZone: 'America/Caracas' })}`,
        '',
        '⬇️ Usa los botones para gestionar este pedido'
    ].filter(line => line !== null).join('\n');
    
    // Botones interactivos para gestionar el pedido
    // Usar copy_text para copiar automáticamente al portapapeles
    const botones = [
        // Primera fila: Tomar pedido
        [{ text: '📥 TOMAR PEDIDO', callback_data: `tomar_${solicitud.id}` }],
        // Segunda fila: Botones de copiar automático (diferentes según tipo de entrega)
        esPagoMovil
            ? [
                { text: `📞 ${telefonoBenef || 'Sin tel'}`, copy_text: { text: telefonoBenef || '' } },
                { text: `🪪 ${cedula}`, copy_text: { text: cedula } }
            ]
            : [
                { text: `💳 ${cuenta}`, copy_text: { text: cuenta } },
                { text: `🪪 ${cedula}`, copy_text: { text: cedula } }
            ],
        // Tercera fila: Más opciones de copiar
        [
            { text: `👤 ${nombreBeneficiario.substring(0, 15)}`, copy_text: { text: nombreBeneficiario } },
            { text: `🏦 ${banco.substring(0, 12)}`, copy_text: { text: banco } }
        ],
        // Cuarta fila: Acciones finales
        [
            { text: '✅ COMPLETADO', callback_data: `completar_${solicitud.id}` },
            { text: '❌ CANCELAR', callback_data: `cancelar_${solicitud.id}` }
        ]
    ];
    
    // Agregar WhatsApp si hay teléfono del cliente
    if (solicitud.cliente_telefono) {
        const telefonoLimpio = solicitud.cliente_telefono.replace(/[^0-9]/g, '');
        botones.push([{ text: '📱 WhatsApp Cliente', url: `https://wa.me/${telefonoLimpio}` }]);
    }
    
    // Si hay comprobante de Caja Vecina, enviar foto primero
    if (comprobanteBuffer) {
        await enviarFotoTelegram(
            comprobanteBuffer,
            `🏪 <b>Comprobante Caja Vecina - Pedido #${solicitud.id}</b>`,
            `comprobante_${solicitud.id}.jpg`
        );
    }

    return await enviarNotificacionTelegram(mensaje, 'HTML', botones);
}

// Función para notificar cambio de estado
async function notificarCambioEstado(solicitud, nuevoEstado) {
    const estados = {
        comprobante_enviado: '\u2705 Comprobante recibido',
        verificando: '\u23F3 Verificando pago',
        procesando: '\u2699\uFE0F Procesando envio',
        completada: '\u2705 Completada',
        rechazada: '\u274C Rechazada',
        cancelada: '\u274C Cancelada'
    };

    const mensaje = [
        `\u2757 ACTUALIZACION DE SOLICITUD #${solicitud.id}`,
        '',
        `${estados[nuevoEstado] || nuevoEstado}`,
        '',
        `\uD83D\uDC64 Cliente: ${solicitud.cliente_nombre}`,
        `\uD83D\uDCB0 ${Number(solicitud.monto_origen).toLocaleString('es-CL')} CLP -> ${Number(solicitud.monto_destino).toLocaleString('es-VE')} VES`,
        '',
        `\u23F0 ${new Date().toLocaleString('es-VE', { timeZone: 'America/Caracas' })}`
    ].join('\n');

    return await enviarNotificacionTelegram(mensaje);
}

// =================================================================
// SISTEMA DE TELEGRAM - WEBHOOK
// Recibe callbacks de botones + mensajes del grupo bot pagador
// =================================================================
const pedidosTomados = new Map(); // Track de pedidos tomados

// Procesar mensajes del grupo del bot pagador
async function procesarMensajeGrupoPagador(message) {
    try {
        const chatConfig = await dbGet("SELECT valor FROM configuracion WHERE clave = 'telegram_pagador_chat_id'");
        const pagadorChatId = chatConfig?.valor;

        // Solo procesar mensajes del grupo del bot pagador
        if (!pagadorChatId || String(message.chat.id) !== String(pagadorChatId)) return;

        const texto = message.text || '';
        console.log(`[WEBHOOK] Mensaje del grupo pagador: ${texto.substring(0, 100)}...`);

        // Detectar Transferencia EXITOSA
        if (texto.includes('Transferencia EXITOSA')) {
            const refMatch = texto.match(/Referencia:\s*(\d+)/);
            const montoMatch = texto.match(/Monto:\s*Bs\.?\s*([\d.,]+)/);
            const benefMatch = texto.match(/Beneficiario:\s*(.+)/);

            const referencia = refMatch ? refMatch[1] : null;
            const montoTexto = montoMatch ? montoMatch[1].replace(/\./g, '').replace(',', '.') : null;
            const beneficiario = benefMatch ? benefMatch[1].trim() : null;

            console.log(`[WEBHOOK] Pago EXITOSO - Ref: ${referencia}, Monto: ${montoTexto}, Benef: ${beneficiario}`);

            // Buscar pedido en procesando que matchee
            let pedido = null;
            if (montoTexto) {
                const montoNum = parseFloat(montoTexto);
                // Buscar por monto_destino (bolívares) con tolerancia de 1
                pedido = await dbGet(
                    `SELECT s.id, s.monto_destino, s.telegram_message_id, s.monto_origen,
                            c.nombre as cliente_nombre, b.nombre_completo as beneficiario_nombre
                     FROM solicitudes_transferencia s
                     JOIN clientes_app c ON s.cliente_app_id = c.id
                     JOIN beneficiarios b ON s.beneficiario_id = b.id
                     WHERE s.estado = 'procesando'
                       AND s.tomado_por_nombre = 'Bot Conciliación'
                       AND ABS(s.monto_destino - ?) < 1
                     ORDER BY s.fecha_tomado DESC
                     LIMIT 1`,
                    [montoNum]
                );
            }

            if (pedido) {
                // Completar pedido automáticamente
                await dbRun(
                    `UPDATE solicitudes_transferencia
                     SET estado = 'completada',
                         referencia_pago = ?,
                         fecha_completada = ?
                     WHERE id = ?`,
                    [referencia, new Date().toISOString(), pedido.id]
                );

                console.log(`[WEBHOOK] Pedido #${pedido.id} COMPLETADO automáticamente - Ref: ${referencia}`);

                // Editar mensaje original en chat de notificaciones
                if (pedido.telegram_message_id) {
                    await editarMensajeTelegram(
                        pedido.telegram_message_id,
                        `✅ <b>PEDIDO COMPLETADO</b>\n\n` +
                        `📋 Pedido #${pedido.id}\n` +
                        `👤 ${pedido.cliente_nombre}\n` +
                        `🏦 ${pedido.beneficiario_nombre}\n` +
                        `💰 $${Math.round(pedido.monto_origen).toLocaleString('es-CL')} CLP → Bs. ${Math.round(pedido.monto_destino).toLocaleString('es-VE')}\n\n` +
                        `🔗 Referencia: <b>${referencia || 'N/A'}</b>\n` +
                        `🤖 Procesado automáticamente\n` +
                        `🕐 ${new Date().toLocaleString('es-VE', { timeZone: 'America/Caracas' })}`,
                        [] // Sin botones
                    );
                }

                // Notificación adicional
                await enviarNotificacionTelegram(
                    `✅ <b>Pago Completado Automáticamente</b>\n\n` +
                    `📋 Pedido #${pedido.id}\n` +
                    `👤 ${pedido.beneficiario_nombre}\n` +
                    `💰 Bs. ${Math.round(pedido.monto_destino).toLocaleString('es-VE')}\n` +
                    `🔗 Referencia: <b>${referencia || 'N/A'}</b>`
                );
            } else {
                console.log('[WEBHOOK] Pago EXITOSO pero no se encontró pedido en procesando que matchee');
            }
        }

        // Detectar Transferencia FALLIDA
        else if (texto.includes('Transferencia FALLIDA') || texto.includes('FALLIDA')) {
            const errorMatch = texto.match(/Error:\s*(.+)/);
            const montoMatch = texto.match(/Bs\.?\s*([\d.,]+)/);
            const errorMsg = errorMatch ? errorMatch[1].trim() : 'Error desconocido';

            console.log(`[WEBHOOK] Pago FALLIDO - Error: ${errorMsg}`);

            // Notificar en chat de notificaciones
            await enviarNotificacionTelegram(
                `❌ <b>Pago Fallido - Atención Manual Requerida</b>\n\n` +
                `⚠️ Error: ${errorMsg}\n` +
                `💰 ${montoMatch ? 'Monto: Bs. ' + montoMatch[1] : ''}\n\n` +
                `🔧 El pedido sigue en estado <b>procesando</b>. Requiere intervención manual.`
            );
        }
    } catch (error) {
        console.error('[WEBHOOK] Error procesando mensaje:', error.message);
    }
}

// Manejar cada callback de Telegram
async function manejarCallbackTelegram(callback) {
    const data = callback.data;
    const mensaje = callback.message;
    const operador = callback.from.first_name || callback.from.username || 'Operador';
    const chatId = mensaje.chat.id;
    const messageId = mensaje.message_id;
    
    try {
        // Parsear acción e ID
        const parts = data.split('_');
        const accion = parts[0];
        const subAccion = parts.length > 2 ? parts[1] : null;
        const solicitudId = parseInt(parts[parts.length - 1]);
        
        // Obtener datos de la solicitud
        const solicitud = await dbGet(
            `SELECT st.*, b.numero_cuenta, b.documento_numero, b.nombre_completo, b.banco, b.telefono as benef_telefono,
                    c.nombre as cliente_nombre, c.telefono as cliente_telefono
             FROM solicitudes_transferencia st
             LEFT JOIN beneficiarios b ON st.beneficiario_id = b.id
             LEFT JOIN clientes_app c ON st.cliente_app_id = c.id
             WHERE st.id = ?`, [solicitudId]
        );
        
        if (!solicitud) {
            await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/answerCallbackQuery`, {
                callback_query_id: callback.id,
                text: '❌ Solicitud no encontrada',
                show_alert: true
            });
            return;
        }
        
        let alertText = '';
        const fechaActual = new Date().toISOString();
        
        // Manejar tomar pedido
        if (accion === 'tomar') {
            if (pedidosTomados.has(solicitudId)) {
                alertText = `⚠️ Ya tomado por ${pedidosTomados.get(solicitudId)}`;
            } else if (solicitud.estado === 'procesando' || solicitud.estado === 'completada') {
                alertText = `⚠️ Este pedido ya está ${solicitud.estado}`;
            } else {
                pedidosTomados.set(solicitudId, operador);
                await dbRun(
                    `UPDATE solicitudes_transferencia SET estado = 'procesando', fecha_tomado = ?, tomado_por_nombre = ? WHERE id = ?`,
                    [fechaActual, operador, solicitudId]
                );
                
                // Actualizar mensaje con estado tomado
                const textoActualizado = mensaje.text + `\n\n✅ TOMADO POR: ${operador}\n⏰ ${new Date().toLocaleString('es-VE', { timeZone: 'America/Caracas' })}`;
                
                // Datos para botones de copiar automático
                const cuenta = solicitud.numero_cuenta || 'No disponible';
                const cedula = solicitud.documento_numero || 'No disponible';
                const nombre = solicitud.nombre_completo || 'No disponible';
                const banco = solicitud.banco || 'No disponible';
                
                // Botones con copy_text para copiar automáticamente
                const botonesActualizados = [
                    [
                        { text: `💳 ${cuenta}`, copy_text: { text: cuenta } },
                        { text: `🪪 ${cedula}`, copy_text: { text: cedula } }
                    ],
                    [
                        { text: `👤 ${nombre.substring(0, 15)}`, copy_text: { text: nombre } },
                        { text: `🏦 ${banco.substring(0, 12)}`, copy_text: { text: banco } }
                    ],
                    [
                        { text: '✅ COMPLETADO', callback_data: `completar_${solicitudId}` },
                        { text: '❌ CANCELAR', callback_data: `cancelar_${solicitudId}` }
                    ]
                ];
                
                await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/editMessageText`, {
                    chat_id: chatId,
                    message_id: messageId,
                    text: textoActualizado,
                    reply_markup: { inline_keyboard: botonesActualizados }
                });
                
                alertText = `✅ Pedido #${solicitudId} tomado`;
            }
        }
        // Manejar completar
        else if (accion === 'completar') {
            // Obtener datos de la solicitud antes de completar
            const solicitud = await dbGet('SELECT cliente_app_id, monto_origen FROM solicitudes_transferencia WHERE id = ?', [solicitudId]);
            
            await dbRun(
                `UPDATE solicitudes_transferencia SET estado = 'completada', fecha_completada = ? WHERE id = ?`,
                [fechaActual, solicitudId]
            );
            pedidosTomados.delete(solicitudId);
            
            // Actualizar progreso de referido si aplica
            if (solicitud && solicitud.cliente_app_id && solicitud.monto_origen) {
                await actualizarProgresoReferido(solicitud.cliente_app_id, solicitud.monto_origen);
            }
            
            const textoCompletado = mensaje.text.split('\n\n✅ TOMADO')[0] + 
                `\n\n🎉 COMPLETADO POR: ${operador}\n⏰ ${new Date().toLocaleString('es-VE', { timeZone: 'America/Caracas' })}`;
            
            await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/editMessageText`, {
                chat_id: chatId,
                message_id: messageId,
                text: textoCompletado,
                reply_markup: { inline_keyboard: [] }
            });
            
            alertText = `🎉 Pedido #${solicitudId} COMPLETADO`;
        }
        // Manejar cancelar
        else if (accion === 'cancelar') {
            await dbRun(
                `UPDATE solicitudes_transferencia SET estado = 'cancelada' WHERE id = ?`,
                [solicitudId]
            );
            pedidosTomados.delete(solicitudId);
            
            const textoCancelado = mensaje.text.split('\n\n✅ TOMADO')[0].split('\n\n⬇️')[0] + 
                `\n\n❌ CANCELADO POR: ${operador}\n⏰ ${new Date().toLocaleString('es-VE', { timeZone: 'America/Caracas' })}`;
            
            await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/editMessageText`, {
                chat_id: chatId,
                message_id: messageId,
                text: textoCancelado,
                reply_markup: { inline_keyboard: [] }
            });
            
            alertText = `❌ Pedido #${solicitudId} CANCELADO`;
        }
        
        // Responder al callback
        await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/answerCallbackQuery`, {
            callback_query_id: callback.id,
            text: alertText,
            show_alert: false
        });
        
    } catch (error) {
        console.error('Error procesando callback Telegram:', error.message);
        try {
            await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/answerCallbackQuery`, {
                callback_query_id: callback.id,
                text: '❌ Error procesando acción',
                show_alert: true
            });
        } catch (e) {}
    }
}

// Configurar webhook de Telegram
async function configurarWebhookTelegram() {
    await cargarConfigTelegram();
    if (!TELEGRAM_BOT_TOKEN) {
        console.log('⚠️ Telegram: No hay token configurado, webhook no activado');
        return;
    }
    try {
        // Determinar URL base del servidor
        const baseUrl = process.env.RENDER_EXTERNAL_URL || process.env.BASE_URL || 'https://defisistema-interno-backup.onrender.com';
        const webhookUrl = `${baseUrl}/webhook/telegram`;

        await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/setWebhook`, {
            url: webhookUrl,
            allowed_updates: ['message', 'callback_query']
        });
        console.log(`📢 Telegram Webhook configurado: ${webhookUrl}`);
    } catch (error) {
        console.error('Error configurando webhook Telegram:', error.message);
    }

}

// =================================================================

// ... RUTA DE LA BASE DE DATOS AJUSTADA PARA DESPLIEGUE
const DB_PATH = path.join(process.env.DATA_DIR || '.', 'database.db');

const app = express();
// ... PUERTO AJUSTADO PARA DESPLIEGUE
const PORT = process.env.PORT || 3000;

// Zona horaria ajustada a Caracas, Venezuela
process.env.TZ = process.env.TZ || 'America/Caracas';

// Headers de seguridad HTTP
app.use(helmet({
    contentSecurityPolicy: false, // deshabilitado para no romper scripts inline existentes
    crossOriginEmbedderPolicy: false
}));

// Habilitar compresión Gzip/Brotli para todas las respuestas
app.use(compression({
    level: 6, // Nivel de compresión (0-9, 6 es el balance óptimo)
    threshold: 1024 // Comprimir solo archivos > 1KB
}));

app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true, limit: '1mb' }));

// Rate limiting general - 100 peticiones por minuto por IP
const generalLimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 100,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Demasiadas peticiones. Intenta de nuevo en un minuto.' }
});
app.use('/api/', generalLimiter);

// Rate limiting estricto para endpoints de autenticación - 10 intentos por 15 min
const authLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 10,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Demasiados intentos de login. Intenta de nuevo en 15 minutos.' }
});
app.use('/login', authLimiter);
app.use('/api/cliente/auth/google', authLimiter);

// Middleware para configurar headers de caché en archivos estáticos
const setStaticCacheHeaders = (res, path) => {
    // Archivos inmutables (imágenes, fuentes, assets)
    if (path.match(/\.(jpg|jpeg|png|gif|svg|webp|woff|woff2|ttf|eot)$/i)) {
        res.setHeader('Cache-Control', 'public, max-age=31536000, immutable'); // 1 año
    }
    // Archivos CSS y JS (versionados)
    else if (path.match(/\.(css|js)$/i)) {
        res.setHeader('Cache-Control', 'public, max-age=86400'); // 1 día
    }
    // HTML (sin caché agresivo para permitir actualizaciones)
    else if (path.match(/\.html$/i)) {
        res.setHeader('Cache-Control', 'public, max-age=3600, must-revalidate'); // 1 hora con revalidación
    }
    // Otros archivos
    else {
        res.setHeader('Cache-Control', 'public, max-age=3600'); // 1 hora
    }
};

app.use(express.static('.', { setHeaders: setStaticCacheHeaders }));
// Servir uploads desde DATA_DIR si esta configurado (disco persistente en Render), sino desde directorio actual
const UPLOADS_BASE_DIR = process.env.DATA_DIR || __dirname;
app.use('/uploads', express.static(path.join(UPLOADS_BASE_DIR, 'uploads'), { setHeaders: setStaticCacheHeaders }));
// CORS para frontend (localhost y dominios render)
const allowedOrigins = [
    'http://localhost:3000',
    'http://127.0.0.1:3000',
    'https://defi-app-cliente.onrender.com',
    'https://def-app-cliente.onrender.com',
    'https://defisistema-interno.onrender.com',
    'https://defioracleapp.com',
    'https://www.defioracleapp.com'
];
app.use(cors({ origin: allowedOrigins, credentials: true }));
app.options('*', cors());
// Confiar en el proxy de Render/Cloudflare para que secure cookies funcionen
app.set('trust proxy', 1);
const SESSION_SECRET = process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex');
app.use(
  session({
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      secure: process.env.NODE_ENV === 'production',
      httpOnly: true,
      sameSite: 'lax',
      maxAge: 1000 * 60 * 60 * 8 // 8h
    },
  })
);

// -------------------- DB --------------------
// ... CONEXI'N USANDO LA RUTA DINÁMICA
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
// FUNCIONES DE PROMEDIO PONDERADO MULTI-MONEDA (SISTEMA UNIFICADO)
// =================================================================
async function getPromedioUsdt() {
    const row = await dbGet('SELECT SUM(clp_invertido) as totalClp, SUM(usdt_obtenido) as totalUsdt FROM compras_usdt');
    if (!row || !row.totalClp || row.totalUsdt === 0) return 0;
    return row.totalClp / row.totalUsdt; // CLP por 1 USDT
}

async function getPromedioVesEnUsdt() {
    const row = await dbGet('SELECT SUM(usdt_invertido) as totalUsdt, SUM(ves_obtenido) as totalVes FROM compras_ves');
    if (!row || !row.totalUsdt || row.totalVes === 0) return 0;
    return row.totalVes / row.totalUsdt; // VES por 1 USDT
}

// Tasa VES/CLP derivada del sistema de stock (equivale a legacy tasa_clp_ves)
async function getTasaVesPorClp() {
    const pppUsdtClp = await getPromedioUsdt();    // CLP por 1 USDT
    const pppVesUsdt = await getPromedioVesEnUsdt(); // VES por 1 USDT
    if (pppUsdtClp <= 0 || pppVesUsdt <= 0) {
        // Fallback: buscar en tabla legacy compras si no hay datos nuevos
        const legacy = await dbGet('SELECT tasa_clp_ves FROM compras ORDER BY id DESC LIMIT 1');
        return legacy && legacy.tasa_clp_ves > 0 ? legacy.tasa_clp_ves : 0;
    }
    return pppVesUsdt / pppUsdtClp; // VES por 1 CLP
}

// getAvgPurchaseRate: retorna VES/CLP (usado por dashboard y calculo de costo)
const getAvgPurchaseRate = (date, callback) => {
    getTasaVesPorClp()
        .then(rate => callback(null, rate))
        .catch(err => {
            console.error(`Error obteniendo tasa de compra:`, err.message);
            callback(err, 0);
        });
};

// calcularCostoClpPorVes: retorna CLP por 1 VES (costo)
const calcularCostoClpPorVes = (fecha, callback) => {
    getTasaVesPorClp()
        .then(rate => {
            if (rate > 0) {
                return callback(null, 1 / rate); // CLP por 1 VES
            }
            // Ultimo fallback
            readConfigValue('capitalCostoVesPorClp')
                .then(costoConfig => callback(null, costoConfig || 0))
                .catch(e => callback(e));
        })
        .catch(err => {
            console.error(`Error calculando costo CLP/VES:`, err.message);
            callback(err);
        });
};
// =================================================================
// FIN: FUNCIONES DE PROMEDIO PONDERADO
// =================================================================

// =================================================================
// INICIO: MIGRACI'N Y VERIFICACI'N DE BASE DE DATOS
// =================================================================
const runMigrations = async () => {
    console.log('Iniciando verificación de la estructura de la base de datos...');

    const addColumn = async (tableName, columnDef) => {
        const columnName = columnDef.split(' ')[0];
        try {
            await dbRun(`ALTER TABLE ${tableName} ADD COLUMN ${columnDef}`);
            console.log(`... Columna '${columnName}' añadida a la tabla '${tableName}'.`);
        } catch (err) {
            if (!err.message.includes('duplicate column name')) {
                console.error(` Error al añadir columna ${columnName} a ${tableName}:`, err.message);
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
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('app_activa', '1')`);

    // Tasas independientes para la App Cliente
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('tasaAppModo', 'enlazada')`);
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('tasaAppNivel1', '0')`);
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('tasaAppNivel2', '0')`);
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('tasaAppNivel3', '0')`);

    // Tasa BCV (referencia USD/VES)
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('tasaBCV', '0')`);
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('tasaBCVFecha', '')`);
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('bcvApiKey', '')`);
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('conciliacion_activa', '1')`);
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('telegram_pagador_bot_token', '')`);
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('telegram_pagador_chat_id', '')`);
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('pagador_api_url', '')`);
    await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('pagador_api_token', '')`);


    // ... NUEVA TABLA PARA METAS
    await dbRun(`CREATE TABLE IF NOT EXISTS metas(id INTEGER PRIMARY KEY AUTOINCREMENT, mes TEXT NOT NULL UNIQUE, meta_clientes_activos INTEGER DEFAULT 0, meta_nuevos_clientes INTEGER DEFAULT 0, meta_volumen_clp REAL DEFAULT 0, meta_operaciones INTEGER DEFAULT 0)`);

    // ... TABLA PARA TAREAS
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
        resolucion_agente TEXT CHECK(resolucion_agente IN ('automatica','asistida','manual')),
        mensaje_generado TEXT,
        accion_requerida TEXT,
        metadata TEXT,
        fecha_mensaje_enviado TEXT,
        respuesta_cliente TEXT,
        FOREIGN KEY(asignado_a) REFERENCES usuarios(id),
        FOREIGN KEY(creado_por) REFERENCES usuarios(id)
    )`);

    // ... TABLA PARA NOTIFICACIONES
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

    // ... TABLA PARA ALERTAS DE CLIENTES
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

    // ... TABLA PARA HISTORIAL DE CHATBOT
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

    // ... TABLA PARA MENSAJES PROACTIVOS DEL BOT
    await dbRun(`CREATE TABLE IF NOT EXISTS chatbot_mensajes_proactivos(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario_id INTEGER NOT NULL,
        tipo TEXT NOT NULL CHECK(tipo IN ('celebracion','recordatorio','alerta','sugerencia','informativo')),
        mensaje TEXT NOT NULL,
        contexto TEXT,
        prioridad TEXT DEFAULT 'normal' CHECK(prioridad IN ('baja','normal','alta','urgente')),
        mostrado INTEGER DEFAULT 0,
        fecha_creacion TEXT NOT NULL,
        fecha_mostrado TEXT,
        FOREIGN KEY(usuario_id) REFERENCES usuarios(id)
    )`);

    // " MIGRACI'N: Agregar columnas de resolución automática a tareas
    try {
        await dbRun(`ALTER TABLE tareas ADD COLUMN resolucion_agente TEXT CHECK(resolucion_agente IN ('automatica','asistida','manual'))`);
        console.log('... Columna resolucion_agente agregada');
    } catch (e) {
        if (!e.message.includes('duplicate column')) console.log('"️  resolucion_agente ya existe');
    }
    
    try {
        await dbRun(`ALTER TABLE tareas ADD COLUMN mensaje_generado TEXT`);
        console.log('... Columna mensaje_generado agregada');
    } catch (e) {
        if (!e.message.includes('duplicate column')) console.log('"️  mensaje_generado ya existe');
    }
    
    try {
        await dbRun(`ALTER TABLE tareas ADD COLUMN accion_requerida TEXT`);
        console.log('... Columna accion_requerida agregada');
    } catch (e) {
        if (!e.message.includes('duplicate column')) console.log('"️  accion_requerida ya existe');
    }
    
    try {
        await dbRun(`ALTER TABLE tareas ADD COLUMN metadata TEXT`);
        console.log('... Columna metadata agregada');
    } catch (e) {
        if (!e.message.includes('duplicate column')) console.log('"️  metadata ya existe');
    }
    
    try {
        await dbRun(`ALTER TABLE tareas ADD COLUMN fecha_mensaje_enviado TEXT`);
        console.log('... Columna fecha_mensaje_enviado agregada');
    } catch (e) {
        if (!e.message.includes('duplicate column')) console.log('"️  fecha_mensaje_enviado ya existe');
    }
    
    try {
        await dbRun(`ALTER TABLE tareas ADD COLUMN respuesta_cliente TEXT`);
        console.log('... Columna respuesta_cliente agregada');
    } catch (e) {
        if (!e.message.includes('duplicate column')) console.log('"️  respuesta_cliente ya existe');
    }
    
    try {
        await dbRun(`ALTER TABLE tareas ADD COLUMN tipo_alerta TEXT`);
        console.log('... Columna tipo_alerta agregada');
    } catch (e) {
        if (!e.message.includes('duplicate column')) console.log('"️  tipo_alerta ya existe');
    }
    
    try {
        await dbRun(`ALTER TABLE tareas ADD COLUMN cliente_id INTEGER`);
        console.log('... Columna cliente_id agregada');
    } catch (e) {
        if (!e.message.includes('duplicate column')) console.log('"️  cliente_id ya existe');
    }
    
    try {
        await dbRun(`ALTER TABLE tareas ADD COLUMN cliente_nombre TEXT`);
        console.log('... Columna cliente_nombre agregada');
    } catch (e) {
        if (!e.message.includes('duplicate column')) console.log('"️  cliente_nombre ya existe');
    }

    // ... TABLA PARA MONITOREO DE ACTIVIDAD DE OPERADORES
    await dbRun(`CREATE TABLE IF NOT EXISTS actividad_operadores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario_id INTEGER NOT NULL,
        tipo_actividad TEXT NOT NULL CHECK(tipo_actividad IN ('login', 'logout', 'heartbeat', 'operacion', 'tarea', 'mensaje')),
        timestamp TEXT NOT NULL,
        fecha TEXT GENERATED ALWAYS AS (DATE(timestamp)) VIRTUAL,
        metadata TEXT,
        FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
    )`);
    console.log('... Tabla actividad_operadores verificada');

    // Índices para actividad_operadores
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_actividad_usuario_timestamp ON actividad_operadores(usuario_id, timestamp)`);
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_actividad_tipo ON actividad_operadores(tipo_actividad, timestamp)`);
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_actividad_fecha ON actividad_operadores(fecha)`);
    console.log('... Índices de actividad_operadores verificados');

    // ... TABLAS PARA SISTEMA DE N"MINA
    await dbRun(`CREATE TABLE IF NOT EXISTS periodos_pago (
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
    )`);
    console.log('... Tabla periodos_pago verificada');

    await dbRun(`CREATE TABLE IF NOT EXISTS nomina (
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
    )`);
    console.log('... Tabla nomina verificada');

    await dbRun(`CREATE TABLE IF NOT EXISTS atencion_rapida (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario_id INTEGER NOT NULL,
        cliente_id INTEGER NOT NULL,
        tipo TEXT NOT NULL CHECK(tipo IN ('operacion', 'mensaje')),
        fecha TEXT NOT NULL,
        tiempo_respuesta_minutos REAL NOT NULL,
        creado_en TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
        FOREIGN KEY (cliente_id) REFERENCES clientes(id)
    )`);
    console.log('... Tabla atencion_rapida verificada');

    // Índices para nómina
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_nomina_periodo ON nomina(periodo_id)`);
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_nomina_usuario ON nomina(usuario_id)`);
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_atencion_rapida_usuario_fecha ON atencion_rapida(usuario_id, fecha)`);
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_periodos_pago_estado ON periodos_pago(estado)`);
    console.log('... Índices de nómina verificados');

    // =================================================================
    // TABLAS PARA APP CLIENTE M"VIL
    // =================================================================
    
    // Tabla de usuarios de la app cliente (autenticación con Google)
    await dbRun(`CREATE TABLE IF NOT EXISTS clientes_app (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        google_id TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL,
        nombre TEXT NOT NULL,
        foto_url TEXT,
        telefono TEXT,
        documento_tipo TEXT CHECK(documento_tipo IN ('cedula', 'rut', 'pasaporte', 'dni')),
        documento_numero TEXT,
        pais TEXT,
        ciudad TEXT,
        direccion TEXT,
        fecha_nacimiento TEXT,
        registro_completo INTEGER DEFAULT 0,
        activo INTEGER DEFAULT 1,
        token_sesion TEXT,
        token_expiracion TEXT,
        fecha_registro TEXT NOT NULL,
        ultimo_acceso TEXT,
        verificacion_estado TEXT DEFAULT 'no_verificado' CHECK(verificacion_estado IN ('no_verificado', 'pendiente', 'verificado', 'rechazado')),
        verificacion_doc_frente TEXT,
        verificacion_doc_reverso TEXT,
        verificacion_fecha_solicitud TEXT,
        verificacion_fecha_respuesta TEXT,
        verificacion_notas TEXT,
        UNIQUE(documento_tipo, documento_numero)
    )`);
    // Agregar columnas de verificacion si no existen
    await dbRun(`ALTER TABLE clientes_app ADD COLUMN verificacion_estado TEXT DEFAULT 'no_verificado'`).catch(() => {});
    await dbRun(`ALTER TABLE clientes_app ADD COLUMN verificacion_doc_frente TEXT`).catch(() => {});
    await dbRun(`ALTER TABLE clientes_app ADD COLUMN verificacion_doc_reverso TEXT`).catch(() => {});
    await dbRun(`ALTER TABLE clientes_app ADD COLUMN verificacion_fecha_solicitud TEXT`).catch(() => {});
    await dbRun(`ALTER TABLE clientes_app ADD COLUMN verificacion_fecha_respuesta TEXT`).catch(() => {});
    await dbRun(`ALTER TABLE clientes_app ADD COLUMN verificacion_notas TEXT`).catch(() => {});
    await dbRun(`ALTER TABLE clientes_app ADD COLUMN token_expiracion TEXT`).catch(() => {});
    console.log('... Tabla clientes_app verificada');

    // Tabla de beneficiarios de transferencias
    await dbRun(`CREATE TABLE IF NOT EXISTS beneficiarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_app_id INTEGER NOT NULL,
        alias TEXT NOT NULL,
        nombre_completo TEXT NOT NULL,
        documento_tipo TEXT CHECK(documento_tipo IN ('cedula', 'rut', 'pasaporte', 'dni')),
        documento_numero TEXT,
        banco TEXT NOT NULL,
        tipo_cuenta TEXT CHECK(tipo_cuenta IN ('corriente', 'ahorro', 'vista', 'pago_movil')),
        numero_cuenta TEXT NOT NULL,
        pais TEXT NOT NULL,
        telefono TEXT,
        email TEXT,
        isFavorite INTEGER DEFAULT 0,
        activo INTEGER DEFAULT 1,
        fecha_creacion TEXT NOT NULL,
        fecha_actualizacion TEXT,
        FOREIGN KEY (cliente_app_id) REFERENCES clientes_app(id)
    )`);
    // Asegurar columna isFavorite si la tabla ya existía
    await dbRun(`ALTER TABLE beneficiarios ADD COLUMN isFavorite INTEGER DEFAULT 0`).catch(() => {});

    // Migración: actualizar CHECK constraint de tipo_cuenta para incluir 'pago_movil'
    // SQLite no permite ALTER CHECK, hay que recrear la tabla
    try {
        // Recuperar datos de beneficiarios_old si quedó de una migración fallida anterior
        const oldTableExists = await dbGet(`SELECT name FROM sqlite_master WHERE type='table' AND name='beneficiarios_old'`);
        if (oldTableExists) {
            console.log('... Recuperando datos de beneficiarios_old...');
            const countOld = await dbGet(`SELECT COUNT(*) as total FROM beneficiarios_old`);
            const countNew = await dbGet(`SELECT COUNT(*) as total FROM beneficiarios`);
            if (countOld.total > 0 && countNew.total === 0) {
                await dbRun(`INSERT INTO beneficiarios SELECT * FROM beneficiarios_old`);
                console.log(`... Recuperados ${countOld.total} beneficiarios`);
            }
            await dbRun(`DROP TABLE beneficiarios_old`);
            console.log('... Tabla beneficiarios_old limpiada');
        }

        const checkResult = await dbGet(`SELECT sql FROM sqlite_master WHERE type='table' AND name='beneficiarios'`);
        if (checkResult && checkResult.sql && !checkResult.sql.includes('pago_movil')) {
            console.log('... Migrando tabla beneficiarios para soportar pago_movil...');
            const countAntes = await dbGet(`SELECT COUNT(*) as total FROM beneficiarios`);
            await dbRun(`ALTER TABLE beneficiarios RENAME TO beneficiarios_old`);
            await dbRun(`CREATE TABLE beneficiarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cliente_app_id INTEGER NOT NULL,
                alias TEXT NOT NULL,
                nombre_completo TEXT NOT NULL,
                documento_tipo TEXT CHECK(documento_tipo IN ('cedula', 'rut', 'pasaporte', 'dni')),
                documento_numero TEXT,
                banco TEXT NOT NULL,
                tipo_cuenta TEXT CHECK(tipo_cuenta IN ('corriente', 'ahorro', 'vista', 'pago_movil')),
                numero_cuenta TEXT NOT NULL,
                pais TEXT NOT NULL,
                telefono TEXT,
                email TEXT,
                isFavorite INTEGER DEFAULT 0,
                activo INTEGER DEFAULT 1,
                fecha_creacion TEXT NOT NULL,
                fecha_actualizacion TEXT,
                FOREIGN KEY (cliente_app_id) REFERENCES clientes_app(id)
            )`);
            await dbRun(`INSERT INTO beneficiarios SELECT * FROM beneficiarios_old`);
            const countDespues = await dbGet(`SELECT COUNT(*) as total FROM beneficiarios`);
            if (countDespues.total >= countAntes.total) {
                await dbRun(`DROP TABLE beneficiarios_old`);
                console.log(`... Migración completada: ${countDespues.total} beneficiarios preservados`);
            } else {
                console.error(`... ALERTA: Solo se copiaron ${countDespues.total} de ${countAntes.total}. beneficiarios_old preservada.`);
            }
        }
    } catch (migErr) {
        console.error('Error en migración de beneficiarios:', migErr.message);
    }

    console.log('... Tabla beneficiarios verificada');

    // Tabla de cuentas de pago (donde recibe dinero la empresa)
    await dbRun(`CREATE TABLE IF NOT EXISTS cuentas_pago (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        banco TEXT NOT NULL,
        tipo_cuenta TEXT CHECK(tipo_cuenta IN ('corriente', 'ahorro', 'vista', 'pago_movil')),
        numero_cuenta TEXT NOT NULL,
        titular TEXT NOT NULL,
        rut_titular TEXT,
        pais TEXT NOT NULL,
        moneda TEXT NOT NULL CHECK(moneda IN ('CLP', 'USD', 'VES', 'COP', 'PEN')),
        activo INTEGER DEFAULT 1,
        orden INTEGER DEFAULT 0,
        fecha_creacion TEXT NOT NULL
    )`);
    console.log('... Tabla cuentas_pago verificada');

    // Tabla de solicitudes de transferencia desde la app
    await dbRun(`CREATE TABLE IF NOT EXISTS solicitudes_transferencia (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_app_id INTEGER NOT NULL,
        beneficiario_id INTEGER NOT NULL,
        cuenta_pago_id INTEGER NOT NULL,
        monto_origen REAL NOT NULL,
        moneda_origen TEXT NOT NULL,
        monto_destino REAL,
        moneda_destino TEXT NOT NULL,
        tasa_aplicada REAL,
        estado TEXT NOT NULL DEFAULT 'pendiente' CHECK(estado IN ('pendiente', 'comprobante_enviado', 'verificando', 'procesando', 'completada', 'rechazada', 'cancelada')),
        comprobante_url TEXT,
        referencia TEXT,
        notas_cliente TEXT,
        notas_operador TEXT,
        operador_id INTEGER,
        operacion_id INTEGER,
        fecha_solicitud TEXT NOT NULL,
        fecha_verificacion TEXT,
        fecha_completada TEXT,
        FOREIGN KEY (cliente_app_id) REFERENCES clientes_app(id),
        FOREIGN KEY (beneficiario_id) REFERENCES beneficiarios(id),
        FOREIGN KEY (cuenta_pago_id) REFERENCES cuentas_pago(id),
        FOREIGN KEY (operador_id) REFERENCES usuarios(id),
        FOREIGN KEY (operacion_id) REFERENCES operaciones(id)
    )`);
    console.log('... Tabla solicitudes_transferencia verificada');

    // Migración: agregar columna para vincular transferencia bancaria conciliada
    await dbRun(`ALTER TABLE solicitudes_transferencia ADD COLUMN transferencia_banco_id INTEGER`).catch(() => {});
    await dbRun(`ALTER TABLE solicitudes_transferencia ADD COLUMN metodo_pago TEXT DEFAULT 'transferencia_bancaria'`).catch(() => {});
    await dbRun(`ALTER TABLE solicitudes_transferencia ADD COLUMN telegram_message_id INTEGER`).catch(() => {});
    await dbRun(`ALTER TABLE solicitudes_transferencia ADD COLUMN conciliacion_notificado INTEGER DEFAULT 0`).catch(() => {});
    await dbRun(`ALTER TABLE solicitudes_transferencia ADD COLUMN referencia_pago TEXT`).catch(() => {});

    // Tabla de codigos promocionales
    await dbRun(`CREATE TABLE IF NOT EXISTS codigos_promocionales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT UNIQUE NOT NULL,
        descripcion TEXT,
        tasa_especial REAL NOT NULL,
        activo INTEGER DEFAULT 1,
        usos_maximos INTEGER DEFAULT NULL,
        usos_actuales INTEGER DEFAULT 0,
        fecha_inicio TEXT,
        fecha_expiracion TEXT,
        solo_primer_envio INTEGER DEFAULT 0,
        creado_por INTEGER,
        fecha_creacion TEXT NOT NULL,
        FOREIGN KEY (creado_por) REFERENCES usuarios(id)
    )`);
    console.log('Tabla codigos_promocionales verificada');
    
    // Agregar columnas para descuento en CLP
    await dbRun(`ALTER TABLE codigos_promocionales ADD COLUMN tipo_descuento TEXT DEFAULT 'tasa'`).catch(() => {});
    await dbRun(`ALTER TABLE codigos_promocionales ADD COLUMN monto_descuento_clp REAL DEFAULT 0`).catch(() => {});
    await dbRun(`ALTER TABLE codigos_promocionales ADD COLUMN cliente_exclusivo_id INTEGER`).catch(() => {});
    await dbRun(`ALTER TABLE codigos_promocionales ADD COLUMN bono_referido_id INTEGER`).catch(() => {});

    // Tabla de uso de codigos promocionales
    await dbRun(`CREATE TABLE IF NOT EXISTS uso_codigos_promocionales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo_id INTEGER NOT NULL,
        cliente_app_id INTEGER NOT NULL,
        solicitud_id INTEGER,
        fecha_uso TEXT NOT NULL,
        FOREIGN KEY (codigo_id) REFERENCES codigos_promocionales(id),
        FOREIGN KEY (cliente_app_id) REFERENCES clientes_app(id),
        FOREIGN KEY (solicitud_id) REFERENCES solicitudes_transferencia(id),
        UNIQUE(codigo_id, cliente_app_id)
    )`);
    console.log('Tabla uso_codigos_promocionales verificada');
    // Índices para app cliente
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_clientes_app_google ON clientes_app(google_id)`);
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_clientes_app_email ON clientes_app(email)`);
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_beneficiarios_cliente ON beneficiarios(cliente_app_id)`);
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_solicitudes_cliente ON solicitudes_transferencia(cliente_app_id)`);
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_solicitudes_estado ON solicitudes_transferencia(estado)`);
    console.log('... Índices de app cliente verificados');

    // Agregar columnas para tracking de tiempo en pedidos
    await dbRun(`ALTER TABLE solicitudes_transferencia ADD COLUMN fecha_tomado TEXT`).catch(() => {});
    await dbRun(`ALTER TABLE solicitudes_transferencia ADD COLUMN tomado_por_nombre TEXT`).catch(() => {});
    // Agregar columnas para cupones/bonos
    await dbRun(`ALTER TABLE solicitudes_transferencia ADD COLUMN cupon_codigo TEXT`).catch(() => {});
    await dbRun(`ALTER TABLE solicitudes_transferencia ADD COLUMN cupon_descuento_clp REAL DEFAULT 0`).catch(() => {});
    await dbRun(`ALTER TABLE solicitudes_transferencia ADD COLUMN monto_sin_cupon REAL`).catch(() => {});
    console.log('... Columnas de tracking de pedidos verificadas');

    // =================================================================
    // SISTEMA DE REFERIDOS
    // =================================================================
    
    // Agregar columna codigo_referido a clientes_app (codigo unico del usuario para referir)
    await dbRun(`ALTER TABLE clientes_app ADD COLUMN codigo_referido TEXT`).catch(() => {});
    // Agregar columna referido_por (id del usuario que lo refirio)
    await dbRun(`ALTER TABLE clientes_app ADD COLUMN referido_por INTEGER`).catch(() => {});
    // Agregar columna fecha_referido
    await dbRun(`ALTER TABLE clientes_app ADD COLUMN fecha_referido TEXT`).catch(() => {});
    
    // Tabla de referidos y bonos
    await dbRun(`CREATE TABLE IF NOT EXISTS referidos_bonos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referidor_id INTEGER NOT NULL,
        referido_id INTEGER NOT NULL,
        monto_acumulado REAL DEFAULT 0,
        meta_monto REAL DEFAULT 100000,
        dias_limite INTEGER DEFAULT 45,
        fecha_inicio TEXT NOT NULL,
        fecha_limite TEXT NOT NULL,
        bono_monto REAL DEFAULT 10000,
        estado TEXT DEFAULT 'pendiente' CHECK(estado IN ('pendiente', 'completado', 'reclamado', 'pagado', 'expirado', 'cancelado')),
        fecha_completado TEXT,
        fecha_reclamado TEXT,
        fecha_pagado TEXT,
        codigo_cupon TEXT,
        notas TEXT,
        FOREIGN KEY (referidor_id) REFERENCES clientes_app(id),
        FOREIGN KEY (referido_id) REFERENCES clientes_app(id),
        UNIQUE(referidor_id, referido_id)
    )`);
    console.log('... Tabla referidos_bonos verificada');
    
    // Agregar columnas adicionales si no existen
    await dbRun(`ALTER TABLE referidos_bonos ADD COLUMN fecha_reclamado TEXT`).catch(() => {});
    await dbRun(`ALTER TABLE referidos_bonos ADD COLUMN codigo_cupon TEXT`).catch(() => {});
    
    // Indices para busquedas rapidas de referidos
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_referidos_referidor ON referidos_bonos(referidor_id)`).catch(() => {});
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_referidos_estado ON referidos_bonos(estado)`).catch(() => {});
    await dbRun(`CREATE UNIQUE INDEX IF NOT EXISTS idx_clientes_codigo_referido ON clientes_app(codigo_referido) WHERE codigo_referido IS NOT NULL`).catch(() => {});
    console.log('... Indices de referidos verificados');

    // =================================================================
    // TABLA TRANSFERENCIAS BANCO (datos del bot bancario)
    // =================================================================
    await dbRun(`CREATE TABLE IF NOT EXISTS transferencias_banco (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        n_operacion TEXT NOT NULL,
        fecha TEXT NOT NULL,
        rut_origen TEXT,
        nombre_origen TEXT,
        cuenta_origen TEXT,
        banco TEXT,
        monto TEXT,
        monto_numerico INTEGER DEFAULT 0,
        estado TEXT DEFAULT 'disponible' CHECK(estado IN ('disponible', 'tomado', 'procesado')),
        tomado_por INTEGER,
        tomado_fecha TEXT,
        lote_id TEXT,
        cuenta_destino TEXT DEFAULT '',
        fecha_importacion TEXT NOT NULL,
        FOREIGN KEY (tomado_por) REFERENCES usuarios(id)
    )`);
    await dbRun(`CREATE UNIQUE INDEX IF NOT EXISTS idx_transferencias_banco_op ON transferencias_banco(n_operacion, fecha)`).catch(() => {});
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_transferencias_banco_estado ON transferencias_banco(estado)`).catch(() => {});
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_transferencias_banco_lote ON transferencias_banco(lote_id)`).catch(() => {});
    // Migración: agregar columna cuenta_destino si no existe
    await dbRun(`ALTER TABLE transferencias_banco ADD COLUMN cuenta_destino TEXT DEFAULT ''`).catch(() => {});
    console.log('... Tabla transferencias_banco verificada');

    return new Promise(resolve => {
        db.get(`SELECT COUNT(*) c FROM usuarios`, async (err, row) => {
            if (err) return console.error('Error al verificar usuarios semilla:', err.message);
            if (!row || row.c === 0) {
                const hash = await bcrypt.hash('master123', 10);
                await dbRun(`INSERT INTO usuarios(username,password,role) VALUES (?,?,?)`, ['master', hash, 'master']);
                console.log('... Usuario semilla creado: master/master123');
            }
            // Tablas de control de stock multi-moneda
            await dbRun(`CREATE TABLE IF NOT EXISTS compras_usdt (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id INTEGER NOT NULL,
                clp_invertido REAL NOT NULL,
                usdt_obtenido REAL NOT NULL,
                tasa_clp_usdt REAL NOT NULL,
                fecha TEXT NOT NULL,
                comprobante_url TEXT,
                observaciones TEXT,
                FOREIGN KEY(usuario_id) REFERENCES usuarios(id)
            )`);
            await dbRun(`CREATE TABLE IF NOT EXISTS compras_ves (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id INTEGER NOT NULL,
                usdt_invertido REAL NOT NULL,
                ves_obtenido REAL NOT NULL,
                tasa_usdt_ves REAL NOT NULL,
                fecha TEXT NOT NULL,
                comprobante_url TEXT,
                observaciones TEXT,
                FOREIGN KEY(usuario_id) REFERENCES usuarios(id)
            )`);
            await dbRun(`CREATE TABLE IF NOT EXISTS movimientos_stock (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id INTEGER NOT NULL,
                tipo TEXT NOT NULL,
                moneda TEXT NOT NULL,
                monto REAL NOT NULL,
                saldo_antes REAL,
                saldo_despues REAL,
                fecha TEXT NOT NULL,
                observaciones TEXT,
                FOREIGN KEY(usuario_id) REFERENCES usuarios(id)
            )`);
            await dbRun(`CREATE TABLE IF NOT EXISTS ventas_usdt (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id INTEGER NOT NULL,
                usdt REAL NOT NULL,
                clp REAL NOT NULL,
                tasa REAL NOT NULL,
                pais TEXT NOT NULL,
                fecha TEXT NOT NULL,
                observaciones TEXT,
                FOREIGN KEY(usuario_id) REFERENCES usuarios(id)
            )`);
            await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('saldoUsdt', '0')`);
            await dbRun(`INSERT OR IGNORE INTO configuracion(clave, valor) VALUES ('saldoClpDisponible', '0')`);
            console.log('... Tablas de stock multi-moneda verificadas');

            // Migrar RUTs existentes al formato estándar XX.XXX.XXX-X
            try {
                const clientesRut = await dbAll("SELECT id, documento_numero FROM clientes_app WHERE documento_tipo = 'rut' AND documento_numero IS NOT NULL");
                let rutsMigrados = 0;
                for (const c of (clientesRut || [])) {
                    const limpio = c.documento_numero.replace(/[^0-9kK]/g, '');
                    if (limpio.length > 1 && limpio.length <= 9) {
                        const cuerpo = limpio.slice(0, -1);
                        const dv = limpio.slice(-1).toUpperCase();
                        let formateado = '';
                        let j = 0;
                        for (let i = cuerpo.length - 1; i >= 0; i--) {
                            formateado = cuerpo[i] + formateado;
                            j++;
                            if (j % 3 === 0 && i > 0) formateado = '.' + formateado;
                        }
                        const rutFormateado = formateado + '-' + dv;
                        if (rutFormateado !== c.documento_numero) {
                            await dbRun('UPDATE clientes_app SET documento_numero = ? WHERE id = ?', [rutFormateado, c.id]);
                            rutsMigrados++;
                        }
                    }
                }
                if (rutsMigrados > 0) console.log(`... ${rutsMigrados} RUTs migrados al formato estándar`);
            } catch (e) { console.error('Error migrando RUTs:', e.message); }

            console.log('... Verificación de base de datos completada.');
            resolve();
        });
    });
};
// =================================================================
// FIN: MIGRACI'N Y VERIFICACI'N DE BASE DE DATOS
// =================================================================


// -------------------- Diagnóstico temporal de BD --------------------
app.get('/api/diagnostico/beneficiarios', async (req, res) => {
    try {
        const tablas = await dbAll(`SELECT name, sql FROM sqlite_master WHERE type='table' AND name LIKE '%beneficiari%'`);
        const countBeneficiarios = await dbGet(`SELECT COUNT(*) as total FROM beneficiarios`).catch(() => ({ total: 'tabla no existe' }));
        const countOld = await dbGet(`SELECT COUNT(*) as total FROM beneficiarios_old`).catch(() => ({ total: 'tabla no existe' }));
        const muestra = await dbAll(`SELECT id, alias, cliente_app_id FROM beneficiarios LIMIT 5`).catch(() => []);
        const muestraOld = await dbAll(`SELECT id, alias, cliente_app_id FROM beneficiarios_old LIMIT 5`).catch(() => []);
        res.json({
            tablas_encontradas: tablas.map(t => t.name),
            beneficiarios_count: countBeneficiarios.total,
            beneficiarios_old_count: countOld.total,
            muestra_beneficiarios: muestra,
            muestra_beneficiarios_old: muestraOld
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
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

// -------------------- Pginas --------------------
const sendClientePage = (page, res) =>
  res.sendFile(path.join(__dirname, 'app-cliente', page));

app.get('/', (req, res) => res.redirect('/app-cliente/login.html'));
app.get('/login.html', (req, res) => sendClientePage('login.html', res));
app.get('/home.html', (req, res) => sendClientePage('home.html', res));
app.get('/destinatarios.html', (req, res) => sendClientePage('destinatarios.html', res));
app.get('/destinatario-form.html', (req, res) => sendClientePage('destinatario-form.html', res));
app.get('/enviar.html', (req, res) => sendClientePage('enviar.html', res));
app.get('/registro.html', (req, res) => sendClientePage('registro.html', res));
app.get('/soporte.html', (req, res) => sendClientePage('soporte.html', res));
app.get('/perfil.html', (req, res) => sendClientePage('perfil.html', res));
app.get('/app.html', pageAuth, (req, res) => res.sendFile(path.join(__dirname, 'app.html')));
app.get('/admin.html', pageAuth, onlyMaster, (req, res) => res.sendFile(path.join(__dirname, 'admin.html')));
app.get('/historico.html', pageAuth, (req, res) => res.sendFile(path.join(__dirname, 'historico.html')));
app.get('/clientes.html', pageAuth, (req, res) => res.sendFile(path.join(__dirname, 'clientes.html')));
app.get('/analytics.html', pageAuth, onlyMaster, (req, res) => res.sendFile(path.join(__dirname, 'analytics.html')));
app.get('/transferencias-banco.html', pageAuth, (req, res) => res.sendFile(path.join(__dirname, 'transferencias-banco.html')));

// -------------------- Auth --------------------
app.post('/login', (req, res) => {
  const { username, password } = req.body;
  db.get(`SELECT * FROM usuarios WHERE username=?`, [username], (err, u) => {
    if (err || !u) return res.status(400).json({ message: 'Credenciales inválidas' });
    bcrypt.compare(password, u.password, (e, ok) => {
      if (e || !ok) return res.status(400).json({ message: 'Credenciales inválidas' });
      req.session.user = { id: u.id, username: u.username, role: u.role };
      
      // Registrar login en actividad
      const timestamp = new Date().toISOString();
      db.run(`
        INSERT INTO actividad_operadores(usuario_id, tipo_actividad, timestamp)
        VALUES (?, 'login', ?)
      `, [u.id, timestamp], (errLog) => {
        if (errLog) console.error('Error registrando login:', errLog);
      });
      
      res.json({ message: 'Login OK' });
    });
  });
});
app.get('/logout', (req, res) => {
  // Registrar logout en actividad
  if (req.session.user) {
    const timestamp = new Date().toISOString();
    db.run(`
      INSERT INTO actividad_operadores(usuario_id, tipo_actividad, timestamp)
      VALUES (?, 'logout', ?)
    `, [req.session.user.id, timestamp]);
  }
  
  req.session.destroy(() => {
    res.clearCookie('connect.sid');
    res.redirect('/login.html');
  });
});
app.get('/api/user-info', apiAuth, (req, res) => res.json(req.session.user));

// ========== WEBHOOK DE TELEGRAM ==========
app.post('/webhook/telegram', async (req, res) => {
    res.sendStatus(200); // Responder inmediato a Telegram
    try {
        const update = req.body;

        if (update.callback_query) {
            await manejarCallbackTelegram(update.callback_query);
        }

        if (update.message) {
            await procesarMensajeGrupoPagador(update.message);
        }
    } catch (error) {
        console.error('[WEBHOOK] Error:', error.message);
    }
});

// Endpoint de heartbeat para monitoreo de actividad
app.post('/api/actividad/heartbeat', apiAuth, (req, res) => {
    const timestamp = new Date().toISOString();
    db.run(`
        INSERT INTO actividad_operadores(usuario_id, tipo_actividad, timestamp)
        VALUES (?, 'heartbeat', ?)
    `, [req.session.user.id, timestamp], (err) => {
        if (err) {
            console.error('Error registrando heartbeat:', err);
            return res.status(500).json({ error: 'Error al registrar actividad' });
        }
        res.json({ success: true });
    });
});

// Endpoint para obtener actividad de operadores (solo master)
app.get('/api/actividad/operadores', apiAuth, onlyMaster, async (req, res) => {
    try {
        const { fecha } = req.query; // Formato YYYY-MM-DD
        const fechaFiltro = fecha || hoyLocalYYYYMMDD();
        
        const operadores = await dbAll(`
            SELECT id, username FROM usuarios WHERE role != 'master' ORDER BY username
        `);
        
        const resultado = [];
        
        for (const operador of operadores) {
            // Obtener todas las actividades del día
            const actividades = await dbAll(`
                SELECT tipo_actividad, timestamp
                FROM actividad_operadores
                WHERE usuario_id = ?
                AND fecha = ?
                ORDER BY timestamp ASC
            `, [operador.id, fechaFiltro]);
            
            // Calcular horas online con gaps de máximo 30 minutos
            let horasOnline = 0;
            let sesionInicio = null;
            let ultimaActividad = null;
            const UMBRAL_MINUTOS = 30;
            
            for (const act of actividades) {
                const timestamp = new Date(act.timestamp);
                
                if (!sesionInicio) {
                    // Iniciar nueva sesión
                    sesionInicio = timestamp;
                    ultimaActividad = timestamp;
                } else {
                    // Calcular diferencia con última actividad
                    const diffMinutos = (timestamp - ultimaActividad) / (1000 * 60);
                    
                    if (diffMinutos > UMBRAL_MINUTOS) {
                        // Gap > 30 min: cerrar sesión anterior e iniciar nueva
                        horasOnline += (ultimaActividad - sesionInicio) / (1000 * 60 * 60);
                        sesionInicio = timestamp;
                    }
                    
                    ultimaActividad = timestamp;
                }
            }
            
            // Cerrar última sesión si existe
            if (sesionInicio && ultimaActividad) {
                horasOnline += (ultimaActividad - sesionInicio) / (1000 * 60 * 60);
            }
            
            // Contar actividades específicas
            const operaciones = await dbGet(`
                SELECT COUNT(*) as cnt FROM actividad_operadores
                WHERE usuario_id = ? AND tipo_actividad = 'operacion' AND fecha = ?
            `, [operador.id, fechaFiltro]);
            
            const tareas = await dbGet(`
                SELECT COUNT(*) as cnt FROM actividad_operadores
                WHERE usuario_id = ? AND tipo_actividad = 'tarea' AND fecha = ?
            `, [operador.id, fechaFiltro]);
            
            const mensajes = await dbGet(`
                SELECT COUNT(*) as cnt FROM actividad_operadores
                WHERE usuario_id = ? AND tipo_actividad = 'mensaje' AND fecha = ?
            `, [operador.id, fechaFiltro]);
            
            const ultimoHeartbeat = actividades.length > 0 
                ? actividades[actividades.length - 1].timestamp 
                : null;
            
            resultado.push({
                operador_id: operador.id,
                operador_nombre: operador.username,
                fecha: fechaFiltro,
                horas_online: Math.round(horasOnline * 100) / 100, // 2 decimales
                operaciones: operaciones.cnt,
                tareas: tareas.cnt,
                mensajes: mensajes.cnt,
                ultimo_heartbeat: ultimoHeartbeat,
                estado: ultimoHeartbeat && new Date() - new Date(ultimoHeartbeat) < 5 * 60 * 1000 ? 'online' : 'offline'
            });
        }
        
        res.json(resultado);
    } catch (error) {
        console.error('Error obteniendo actividad operadores:', error);
        res.status(500).json({ error: 'Error al obtener actividad' });
    }
});

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
// INICIO: ENDPOINTS PARA LA GESTI'N DE CLIENTES (CRUD)
// =================================================================
// ... ENDPOINT DE CLIENTES MODIFICADO PARA PAGINACI'N Y BSQUEDA
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

// " ENDPOINT PARA BUSCAR POSIBLES DUPLICADOS (debe ir ANTES de /api/clientes/:id)
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

// - ENDPOINT PARA FUSIONAR CLIENTES DUPLICADOS (debe ir ANTES de /api/clientes/:id)
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
// FIN: ENDPOINTS PARA LA GESTI'N DE CLIENTES (CRUD)
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
                    // Obtener tasa VES/CLP desde sistema de stock unificado
                    getTasaVesPorClp()
                        .then(tasa => resolve({ tasaCompraPromedio: tasa || 0 }))
                        .catch(() => resolve({ tasaCompraPromedio: 0 }));
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
        fechaPreviaObj.setDate(0); // ltimo día del mes anterior
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
      
      console.log(`\n" Nueva operación - Usuario: ${req.session.user.username}, Cliente: ${nombreNormalizado}, Monto: ${montoClpNum} CLP`);
      
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
                // Reingresar CLP del cliente al stock
                db.run(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoClpDisponible'`, [montoClpNum]);

                console.log(`... Operación #${numero_recibo} registrada exitosamente`);
                console.log(`[STOCK] +${montoClpNum} CLP reingresado (operación #${numero_recibo})`);
                console.log(`   Cliente ID: ${cliente_id}, Monto: ${montoClpNum} CLP †' ${montoVesNum} VES`);
                console.log(`   Ganancia Neta: ${gananciaNeta.toFixed(2)} CLP`);
                
                // Registrar actividad de operación
                const timestamp = new Date().toISOString();
                db.run(`
                    INSERT INTO actividad_operadores(usuario_id, tipo_actividad, timestamp, metadata)
                    VALUES (?, 'operacion', ?, ?)
                `, [req.session.user.id, timestamp, JSON.stringify({ 
                    operacion_id: this.lastID,
                    monto_clp: montoClpNum,
                    cliente_id: cliente_id
                })]);
                
                // Verificar si el cliente tiene datos completos y generar alerta si faltan
                db.get(`SELECT nombre, rut, email, telefono FROM clientes WHERE id = ?`, [cliente_id], (errCliente, cliente) => {
                    if (!errCliente && cliente) {
                        const datosFaltantes = [];
                        if (!cliente.rut || cliente.rut.trim() === '') datosFaltantes.push('RUT');
                        if (!cliente.email || cliente.email.trim() === '') datosFaltantes.push('Email');
                        if (!cliente.telefono || cliente.telefono.trim() === '') datosFaltantes.push('Teléfono');
                        
                        if (datosFaltantes.length > 0) {
                            console.log(`\n️  ALERTA: Cliente "${cliente.nombre}" tiene datos incompletos!`);
                            console.log(`   Faltan: ${datosFaltantes.join(', ')}`);
                            console.log(`   Se creará notificación para el operador\n`);
                            
                            const mensaje = `️ Cliente "${cliente.nombre}" realizó una operación pero le faltan datos: ${datosFaltantes.join(', ')}. Por favor actualizar su información.`;
                            const fechaCreacion = new Date().toISOString();
                            
                            // Crear notificación para el operador que registró la operación
                            db.run(
                                `INSERT INTO notificaciones(usuario_id, tipo, titulo, mensaje, fecha_creacion) VALUES (?, ?, ?, ?, ?)`,
                                [req.session.user.id, 'alerta', 'Datos de cliente incompletos', mensaje, fechaCreacion],
                                (errNot) => {
                                    if (errNot) console.error(' Error al crear notificación de datos incompletos:', errNot);
                                    else console.log(`... Notificación creada para usuario ID ${req.session.user.id}`);
                                }
                            );
                        } else {
                            console.log(`... Cliente "${cliente.nombre}" tiene datos completos\n`);
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
                    const deltaClp = montoClpNuevo - opOriginal.monto_clp;

                    db.serialize(() => {
                        db.run('BEGIN TRANSACTION');
                        db.run(`UPDATE operaciones SET cliente_id=?, fecha=?, monto_clp=?, monto_ves=?, tasa=?, observaciones=?, costo_clp=?, comision_ves=?, numero_recibo=? WHERE id = ?`,
                            [cliente.id, fecha, montoClpNuevo, montoVesNuevo, tasaNueva, observaciones || '', costoClpNuevo, comisionVesNuevo, numero_recibo, operacionId]);
                        db.run(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'totalGananciaAcumuladaClp'`, [deltaGanancia]);
                        db.run(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoVesOnline'`, [deltaVes]);
                        // Ajustar CLP en stock (delta entre monto nuevo y original)
                        db.run(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoClpDisponible'`, [deltaClp], (err) => {
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
            db.run(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoVesOnline'`, [vesTotalARevertir]);
            // Revertir CLP que se reingresó al stock
            db.run(`UPDATE configuracion SET valor = CAST(valor AS REAL) - ? WHERE clave = 'saldoClpDisponible'`, [op.monto_clp], (err) => {
                if (err) {
                    db.run('ROLLBACK');
                    return res.status(500).json({ message: 'Error al revertir saldos, se canceló el borrado.' });
                }
                db.run('COMMIT');
                console.log(`[STOCK] -${op.monto_clp} CLP revertido (operación #${op.numero_recibo} borrada)`);
                res.json({ message: 'Operación borrada y saldos revertidos con éxito.' });
            });
        });
    });
});

// ... NUEVO ENDPOINT PARA RECALCULAR COSTOS
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
        readConfig('margenToleranciaAlertas', (e4, v4) => {
          result.margenToleranciaAlertas = v4 ? Number(v4) : 2.0;
          res.json(result);
        });
      });
    });
  });
});

app.post('/api/tasas', apiAuth, onlyMaster, (req, res) => {
  const { tasaNivel1, tasaNivel2, tasaNivel3, margenToleranciaAlertas } = req.body;
  upsertConfig('tasaNivel1', String(tasaNivel1 ?? ''), () => {
    upsertConfig('tasaNivel2', String(tasaNivel2 ?? ''), () => {
      upsertConfig('tasaNivel3', String(tasaNivel3 ?? ''), () => {
        upsertConfig('margenToleranciaAlertas', String(margenToleranciaAlertas ?? '2.0'), () => {
          // ... Ejecutar verificación inmediata después de guardar tasas
          console.log('"" Tasas actualizadas por Master - Ejecutando verificación inmediata...');
          setTimeout(() => monitorearTasasVES(), 2000); // Verificar en 2 segundos
          res.json({ ok: true });
        });
      });
    });
  });
});

// GET /api/tasas-app - Obtener configuración de tasas para la app cliente
app.get('/api/tasas-app', apiAuth, onlyMaster, async (req, res) => {
    try {
        const [modo, t1, t2, t3] = await Promise.all([
            dbGet("SELECT valor FROM configuracion WHERE clave = 'tasaAppModo'"),
            dbGet("SELECT valor FROM configuracion WHERE clave = 'tasaAppNivel1'"),
            dbGet("SELECT valor FROM configuracion WHERE clave = 'tasaAppNivel2'"),
            dbGet("SELECT valor FROM configuracion WHERE clave = 'tasaAppNivel3'")
        ]);
        res.json({
            modo: modo?.valor || 'enlazada',
            tasaAppNivel1: t1 ? Number(t1.valor) : 0,
            tasaAppNivel2: t2 ? Number(t2.valor) : 0,
            tasaAppNivel3: t3 ? Number(t3.valor) : 0
        });
    } catch (error) {
        console.error('Error obteniendo tasas app:', error);
        res.status(500).json({ message: 'Error al obtener configuración de tasas app' });
    }
});

// POST /api/tasas-app - Guardar configuración de tasas para la app cliente
app.post('/api/tasas-app', apiAuth, onlyMaster, async (req, res) => {
    try {
        const { modo, tasaAppNivel1, tasaAppNivel2, tasaAppNivel3 } = req.body;

        if (modo && !['enlazada', 'independiente'].includes(modo)) {
            return res.status(400).json({ message: 'Modo inválido' });
        }

        await new Promise((resolve, reject) => {
            upsertConfig('tasaAppModo', String(modo || 'enlazada'), (err) => {
                if (err) reject(err); else resolve();
            });
        });

        if (modo === 'independiente') {
            if (!tasaAppNivel1 || !tasaAppNivel2 || !tasaAppNivel3) {
                return res.status(400).json({ message: 'Ingresa las 3 tasas para modo independiente' });
            }
            await new Promise((resolve, reject) => {
                upsertConfig('tasaAppNivel1', String(tasaAppNivel1), () => {
                    upsertConfig('tasaAppNivel2', String(tasaAppNivel2), () => {
                        upsertConfig('tasaAppNivel3', String(tasaAppNivel3), (err) => {
                            if (err) reject(err); else resolve();
                        });
                    });
                });
            });
        }

        console.log(`📱 Tasas App actualizadas: modo=${modo}, N1=${tasaAppNivel1}, N2=${tasaAppNivel2}, N3=${tasaAppNivel3}`);
        res.json({ ok: true });
    } catch (error) {
        console.error('Error guardando tasas app:', error);
        res.status(500).json({ message: 'Error al guardar configuración de tasas app' });
    }
});

// GET /api/config/bcv - Obtener config BCV
app.get('/api/config/bcv', apiAuth, onlyMaster, async (req, res) => {
    try {
        const [apiKey, tasa, fecha] = await Promise.all([
            dbGet("SELECT valor FROM configuracion WHERE clave = 'bcvApiKey'"),
            dbGet("SELECT valor FROM configuracion WHERE clave = 'tasaBCV'"),
            dbGet("SELECT valor FROM configuracion WHERE clave = 'tasaBCVFecha'")
        ]);
        res.json({
            apiKey: apiKey?.valor || '',
            tasa: tasa ? parseFloat(tasa.valor) : 0,
            fecha: fecha?.valor || ''
        });
    } catch (error) {
        res.status(500).json({ error: 'Error obteniendo config BCV' });
    }
});

// POST /api/config/bcv - Forzar actualización de tasa BCV
app.post('/api/config/bcv', apiAuth, onlyMaster, async (req, res) => {
    try {
        const tasa = await obtenerTasaBCV();
        if (tasa) {
            return res.json({ ok: true, tasa, mensaje: `Tasa actualizada: ${tasa} VES/USD` });
        }
        res.status(400).json({ error: 'No se pudo obtener la tasa del BCV' });
    } catch (error) {
        res.status(500).json({ error: 'Error actualizando tasa BCV' });
    }
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

// =================================================================
// CONFIGURACI'N BOT TELEGRAM
// =================================================================
app.get('/api/config/telegram', apiAuth, onlyMaster, async (req, res) => {
    try {
        const [botToken, chatId] = await Promise.all([
            dbGet("SELECT valor FROM configuracion WHERE clave = 'telegram_bot_token'"),
            dbGet("SELECT valor FROM configuracion WHERE clave = 'telegram_chat_id'")
        ]);
        res.json({
            bot_token: botToken?.valor || '',
            chat_id: chatId?.valor || '',
            configurado: !!(botToken?.valor && chatId?.valor)
        });
    } catch (error) {
        res.status(500).json({ error: 'Error al obtener configuración' });
    }
});

app.post('/api/config/telegram', apiAuth, onlyMaster, async (req, res) => {
    try {
        const { bot_token, chat_id } = req.body;
        
        await new Promise((resolve, reject) => {
            upsertConfig('telegram_bot_token', bot_token || '', (err) => err ? reject(err) : resolve());
        });
        await new Promise((resolve, reject) => {
            upsertConfig('telegram_chat_id', chat_id || '', (err) => err ? reject(err) : resolve());
        });

        // Actualizar variables globales
        global.TELEGRAM_BOT_TOKEN = bot_token;
        global.TELEGRAM_CHAT_ID = chat_id;

        // Enviar mensaje de prueba si está configurado
        if (bot_token && chat_id) {
            const testResult = await enviarNotificacionTelegram('... <b>Bot configurado correctamente</b>\n\nRecibirás notificaciones de nuevas solicitudes de la App Defi Oracle.');
            if (testResult) {
                res.json({ mensaje: 'Configuración guardada y mensaje de prueba enviado' });
            } else {
                res.json({ mensaje: 'Configuración guardada, pero no se pudo enviar mensaje de prueba. Verifica el token y chat ID.' });
            }
        } else {
            res.json({ mensaje: 'Configuración guardada' });
        }
    } catch (error) {
        console.error('Error guardando config Telegram:', error);
        res.status(500).json({ error: 'Error al guardar configuración' });
    }
});

app.post('/api/config/telegram/test', apiAuth, onlyMaster, async (req, res) => {
    const result = await enviarNotificacionTelegram('"" <b>Mensaje de prueba</b>\n\n¡Las notificaciones de Telegram están funcionando correctamente!');
    if (result) {
        res.json({ mensaje: 'Mensaje de prueba enviado correctamente' });
    } else {
        res.status(400).json({ error: 'No se pudo enviar el mensaje. Verifica la configuración.' });
    }
});

// =================================================================
// CONFIGURACION ESTADO APP (KILL SWITCH MAESTRO)
// =================================================================
app.get('/api/config/estado-app', apiAuth, onlyMaster, async (req, res) => {
    try {
        const row = await dbGet("SELECT valor FROM configuracion WHERE clave = 'app_activa'");
        res.json({ app_activa: row ? row.valor === '1' : true });
    } catch (error) {
        console.error('Error obteniendo estado app:', error);
        res.status(500).json({ error: 'Error al obtener estado de la app' });
    }
});

app.post('/api/config/estado-app', apiAuth, onlyMaster, async (req, res) => {
    try {
        const { app_activa } = req.body;
        const nuevoValor = app_activa ? '1' : '0';
        await new Promise((resolve, reject) => {
            upsertConfig('app_activa', nuevoValor, (err) => err ? reject(err) : resolve());
        });
        res.json({
            app_activa: nuevoValor === '1',
            mensaje: nuevoValor === '1' ? 'App activada correctamente' : 'App desactivada correctamente'
        });
    } catch (error) {
        console.error('Error actualizando estado app:', error);
        res.status(500).json({ error: 'Error al actualizar estado de la app' });
    }
});

// Estado de conciliación automática
app.get('/api/conciliacion/estado', apiAuth, async (req, res) => {
    try {
        const row = await dbGet("SELECT valor FROM configuracion WHERE clave = 'conciliacion_activa'");
        res.json({ conciliacion_activa: row ? row.valor === '1' : true });
    } catch (error) {
        res.status(500).json({ error: 'Error al obtener estado de conciliación' });
    }
});

app.put('/api/conciliacion/estado', apiAuth, onlyMaster, async (req, res) => {
    try {
        const { conciliacion_activa } = req.body;
        const nuevoValor = conciliacion_activa ? '1' : '0';
        await new Promise((resolve, reject) => {
            upsertConfig('conciliacion_activa', nuevoValor, (err) => err ? reject(err) : resolve());
        });
        console.log(`[CONCILIACIÓN] ${nuevoValor === '1' ? 'ACTIVADA' : 'DESACTIVADA'} por administrador`);
        res.json({
            conciliacion_activa: nuevoValor === '1',
            mensaje: nuevoValor === '1' ? 'Conciliación automática activada' : 'Conciliación automática desactivada'
        });
    } catch (error) {
        res.status(500).json({ error: 'Error al actualizar estado de conciliación' });
    }
});

// ========== CONFIG BOT PAGADOR ==========
app.get('/api/config/pagador', apiAuth, async (req, res) => {
    try {
        const [urlRow, tokenRow] = await Promise.all([
            dbGet("SELECT valor FROM configuracion WHERE clave = 'pagador_api_url'"),
            dbGet("SELECT valor FROM configuracion WHERE clave = 'pagador_api_token'")
        ]);
        res.json({ api_url: urlRow?.valor || '', api_token: tokenRow?.valor || '' });
    } catch (error) {
        res.status(500).json({ error: 'Error al obtener config del bot pagador' });
    }
});

app.get('/api/config/pagador/test', apiAuth, async (req, res) => {
    try {
        const urlRow = await dbGet("SELECT valor FROM configuracion WHERE clave = 'pagador_api_url'");
        const tokenRow = await dbGet("SELECT valor FROM configuracion WHERE clave = 'pagador_api_token'");
        const apiUrl = urlRow?.valor;
        const apiToken = tokenRow?.valor;
        if (!apiUrl || !apiToken) {
            return res.json({ ok: false, error: 'URL o Token no configurados' });
        }
        const resp = await axios.get(`${apiUrl}/status`, {
            headers: { 'Authorization': `Bearer ${apiToken}` },
            timeout: 10000
        });
        res.json({ ok: true, data: resp.data });
    } catch (error) {
        const msg = error.response ? `HTTP ${error.response.status}: ${JSON.stringify(error.response.data)}` : error.message;
        res.json({ ok: false, error: msg });
    }
});

app.put('/api/config/pagador', apiAuth, onlyMaster, async (req, res) => {
    try {
        const { api_url, api_token } = req.body;
        if (api_url !== undefined) {
            await new Promise((resolve, reject) => {
                upsertConfig('pagador_api_url', api_url, (err) => err ? reject(err) : resolve());
            });
        }
        if (api_token !== undefined) {
            await new Promise((resolve, reject) => {
                upsertConfig('pagador_api_token', api_token, (err) => err ? reject(err) : resolve());
            });
        }
        console.log('[BOT PAGADOR] Configuración actualizada');
        res.json({ ok: true, mensaje: 'Configuración del bot pagador actualizada' });
    } catch (error) {
        res.status(500).json({ error: 'Error al actualizar config del bot pagador' });
    }
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

app.delete('/api/usuarios/:id', apiAuth, onlyMaster, async (req, res) => {
    const userId = req.params.id;
    // No permitir eliminar el propio usuario
    if (Number(userId) === req.session.user.id) {
        return res.status(400).json({ message: 'No puedes eliminarte a ti mismo.' });
    }
    try {
        const user = await dbGet('SELECT id, username, role FROM usuarios WHERE id = ?', [userId]);
        if (!user) return res.status(404).json({ message: 'Usuario no encontrado.' });

        await dbRun('DELETE FROM usuarios WHERE id = ?', [userId]);
        console.log(`[ADMIN] Usuario "${user.username}" (ID: ${userId}, rol: ${user.role}) eliminado por ${req.session.user.username}`);
        res.json({ message: `Operador "${user.username}" eliminado con éxito.` });
    } catch (e) {
        console.error('Error eliminando usuario:', e.message);
        res.status(500).json({ message: 'Error al eliminar usuario.' });
    }
});

app.post('/api/create-operator', apiAuth, onlyMaster, async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ message: 'Datos incompletos' });

  // Validar username: solo alfanuméricos y guiones, 3-30 caracteres
  const usernameClean = sanitizarTexto(username, 30);
  if (!/^[a-zA-Z0-9_-]{3,30}$/.test(usernameClean)) {
      return res.status(400).json({ message: 'Username debe tener 3-30 caracteres alfanuméricos' });
  }

  // Validar contraseña mínima
  if (password.length < 8) {
      return res.status(400).json({ message: 'La contraseña debe tener al menos 8 caracteres' });
  }

  const hash = await bcrypt.hash(password, 10);
  db.run(`INSERT INTO usuarios(username,password,role) VALUES (?,?,?)`, [usernameClean, hash, 'operador'], (e) => {
      if (e) return res.status(400).json({ message: 'No se pudo crear el operador' });
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

// LEGACY: Endpoints /api/compras desconectados de saldoVesOnline
// Los saldos ahora se manejan SOLO desde el sistema de stock (compras_usdt + compras_ves)
app.post('/api/compras', apiAuth, onlyMaster, (req, res) => {
    return res.status(410).json({ message: 'Sistema legacy desactivado. Usa el Control de Stock para registrar operaciones.' });
});

app.put('/api/compras/:id', apiAuth, onlyMaster, (req, res) => {
    return res.status(410).json({ message: 'Sistema legacy desactivado. Usa el Control de Stock para registrar operaciones.' });
});

app.delete('/api/compras/:id', apiAuth, onlyMaster, (req, res) => {
    return res.status(410).json({ message: 'Sistema legacy desactivado. Usa el Control de Stock para registrar operaciones.' });
});

// =================================================================
// CONTROL DE STOCK MULTI-MONEDA
// =================================================================

// GET /api/stock — consulta de saldos y promedios ponderados
app.get('/api/stock', apiAuth, onlyMaster, async (req, res) => {
    try {
        const saldoClp = Number(await readConfigValue('saldoClpDisponible') || 0);
        const saldoUsdt = Number(await readConfigValue('saldoUsdt') || 0);
        const saldoVes = Number(await readConfigValue('saldoVesOnline') || 0);
        const pppUsdtClp = await getPromedioUsdt();       // CLP por 1 USDT
        const pppVesUsdt = await getPromedioVesEnUsdt();   // VES por 1 USDT

        const pppVesClp = (pppUsdtClp > 0 && pppVesUsdt > 0) ? (pppUsdtClp / pppVesUsdt) : 0;
        const valorUsdtEnClp = saldoUsdt * pppUsdtClp;
        const valorVesEnClp = saldoVes * pppVesClp;
        const capitalTotal = saldoClp + valorUsdtEnClp + valorVesEnClp;

        // Obtener ganancias del dia desde operaciones (sistema legacy)
        const hoy = hoyLocalYYYYMMDD();
        const totalGananciaAcumuladaClp = Number(await readConfigValue('totalGananciaAcumuladaClp') || 0);
        const capitalInicialClp = Number(await readConfigValue('capitalInicialClp') || 0);

        // Ganancias del día: operaciones VES + ventas USDT otros países
        const opsDia = await new Promise((resolve) => {
            db.get(`SELECT
                IFNULL(SUM(monto_clp),0) as clpRecibidoHoy,
                IFNULL(SUM(monto_ves),0) as vesEnviadoHoy,
                IFNULL(SUM(costo_clp),0) as costoTotalHoy,
                COUNT(*) as cantOperaciones
                FROM operaciones WHERE date(fecha)=date(?)`, [hoy], (e, row) => {
                if (e || !row) return resolve({ clpRecibidoHoy: 0, vesEnviadoHoy: 0, costoTotalHoy: 0, cantOperaciones: 0 });
                resolve(row);
            });
        });

        // Ventas USDT del día (otros países)
        const ventasUsdtDia = await dbGet(`SELECT
            IFNULL(SUM(clp),0) as clpRecibidoUsdt,
            IFNULL(SUM(usdt),0) as usdtVendidoHoy,
            COUNT(*) as cantVentas
            FROM ventas_usdt WHERE date(fecha)=date(?)`, [hoy]) || { clpRecibidoUsdt: 0, usdtVendidoHoy: 0, cantVentas: 0 };

        const costoUsdtVendido = ventasUsdtDia.usdtVendidoHoy * pppUsdtClp;
        const gananciaVentasUsdt = ventasUsdtDia.clpRecibidoUsdt - costoUsdtVendido;

        const gananciaBrutaHoy = opsDia.clpRecibidoHoy - opsDia.costoTotalHoy;
        const comisionHoy = opsDia.clpRecibidoHoy * 0.003;
        const gananciaNetaOpsVes = gananciaBrutaHoy - comisionHoy;
        const gananciaNetaHoy = gananciaNetaOpsVes + gananciaVentasUsdt;
        const cantTotalOps = opsDia.cantOperaciones + (ventasUsdtDia.cantVentas || 0);

        res.json({
            saldoClp, saldoUsdt, saldoVes,
            pppUsdtClp,   // ej: 950 CLP = 1 USDT
            pppVesUsdt,   // ej: 85.5 VES = 1 USDT
            pppVesClp,    // ej: 11.11 CLP = 1 VES
            valorUsdtEnClp, valorVesEnClp, capitalTotal,
            // Datos de ganancias
            gananciaNetaHoy,
            gananciaBrutaHoy,
            clpRecibidoHoy: opsDia.clpRecibidoHoy + ventasUsdtDia.clpRecibidoUsdt,
            vesEnviadoHoy: opsDia.vesEnviadoHoy,
            usdtVendidoHoy: ventasUsdtDia.usdtVendidoHoy,
            gananciaVentasUsdt,
            cantOperacionesHoy: cantTotalOps,
            totalGananciaAcumuladaClp,
            capitalInicialClp,
            capitalLegacy: capitalInicialClp + totalGananciaAcumuladaClp
        });
    } catch (error) {
        console.error('Error en GET /api/stock:', error.message);
        res.status(500).json({ error: 'Error al consultar stock' });
    }
});

// =================================================================
// VENTAS USDT — Envíos a otros países
// =================================================================
app.get('/api/ventas-usdt', apiAuth, onlyMaster, async (req, res) => {
    try {
        const ventas = await dbAll(`SELECT id, usuario_id, usdt, clp, pais, fecha, observaciones FROM ventas_usdt ORDER BY fecha DESC, id DESC`);
        res.json(ventas || []);
    } catch (e) {
        console.error('Error en GET /api/ventas-usdt:', e.message);
        res.status(500).json({ error: 'Error al listar ventas' });
    }
});

app.post('/api/ventas-usdt', apiAuth, onlyMaster, async (req, res) => {
    try {
        const { usdt, clp, pais, observaciones } = req.body;
        const usdtNum = Number(usdt || 0);
        const clpNum = Number(clp || 0);
        if (usdtNum <= 0 || clpNum <= 0) return res.status(400).json({ error: 'Los montos deben ser mayores a 0' });
        if (!pais) return res.status(400).json({ error: 'Selecciona un país destino' });

        const saldoUsdtActual = Number(await readConfigValue('saldoUsdt') || 0);
        if (usdtNum > saldoUsdtActual) {
            return res.status(400).json({ error: `Saldo USDT insuficiente. Disponible: ${saldoUsdtActual.toFixed(2)}` });
        }

        const saldoClpActual = Number(await readConfigValue('saldoClpDisponible') || 0);
        const tasa = clpNum / usdtNum;

        // Registrar en tabla ventas_usdt
        await dbRun(
            `INSERT INTO ventas_usdt(usuario_id, usdt, clp, tasa, pais, fecha, observaciones) VALUES (?,?,?,?,?,?,?)`,
            [req.session.user.id, usdtNum, clpNum, tasa, pais, hoyLocalYYYYMMDD(), observaciones || '']
        );

        // Actualizar saldos: USDT baja, CLP sube
        await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) - ? WHERE clave = 'saldoUsdt'`, [usdtNum]);
        await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoClpDisponible'`, [clpNum]);

        // Calcular ganancia
        const pppUsdtClp = await getPromedioUsdt();
        const costoEnClp = usdtNum * pppUsdtClp;
        const ganancia = clpNum - costoEnClp;

        console.log(`[VENTA USDT] ${usdtNum} USDT → ${clpNum} CLP (${pais}) | Ganancia: ${ganancia.toFixed(0)} CLP`);

        res.json({
            mensaje: `Venta USDT registrada (${pais})`,
            detalle: `${usdtNum.toLocaleString('es-CL')} USDT → $${clpNum.toLocaleString('es-CL')} CLP (tasa: ${tasa.toFixed(2)})`,
            pais,
            ganancia: `${ganancia >= 0 ? '+' : ''}${ganancia.toFixed(0)} CLP`,
            saldos: {
                usdt: { antes: saldoUsdtActual, despues: saldoUsdtActual - usdtNum },
                clp: { antes: saldoClpActual, despues: saldoClpActual + clpNum }
            }
        });
    } catch (e) {
        console.error('Error en POST /api/ventas-usdt:', e.message);
        res.status(500).json({ error: 'Error al registrar venta' });
    }
});

app.delete('/api/ventas-usdt/:id', apiAuth, onlyMaster, async (req, res) => {
    try {
        const venta = await dbGet('SELECT * FROM ventas_usdt WHERE id = ?', [req.params.id]);
        if (!venta) return res.status(404).json({ error: 'Venta no encontrada' });

        await dbRun('DELETE FROM ventas_usdt WHERE id = ?', [venta.id]);
        // Revertir saldos: devolver USDT, quitar CLP
        await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoUsdt'`, [venta.usdt]);
        await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) - ? WHERE clave = 'saldoClpDisponible'`, [venta.clp]);

        console.log(`[VENTA USDT BORRADA] +${venta.usdt} USDT, -${venta.clp} CLP revertidos`);
        res.json({ message: 'Venta eliminada y saldos revertidos.' });
    } catch (e) {
        console.error('Error en DELETE /api/ventas-usdt:', e.message);
        res.status(500).json({ error: 'Error al eliminar venta' });
    }
});

// POST /api/stock/operacion — parser de operaciones en lenguaje natural
app.post('/api/stock/operacion', apiAuth, onlyMaster, async (req, res) => {
    try {
        const { mensaje } = req.body;
        if (!mensaje || !mensaje.trim()) return res.status(400).json({ error: 'Escribe una operación' });

        const msg = mensaje.trim().toLowerCase().replace(/,/g, '').replace(/\./g, '');
        // Restaurar puntos decimales: "950000" queda, pero necesitamos manejar decimales
        // Mejor: extraer números del mensaje original
        const msgOriginal = mensaje.trim().replace(/,/g, '');

        let tipo, resultado;

        // Patrón: compré X USDT a tasa Y (calcula CLP = X * Y)
        const matchCompraUsdtTasa = msgOriginal.match(/(?:compr[eéa]|compro)\s+([\d.]+)\s*(?:usdt|USDT)\s+(?:a\s+)?tasa\s+([\d.]+)/i);
        if (matchCompraUsdtTasa) {
            const usdt = Number(matchCompraUsdtTasa[1]);
            const tasa = Number(matchCompraUsdtTasa[2]);
            if (usdt <= 0 || tasa <= 0) return res.status(400).json({ error: 'Los montos deben ser mayores a 0' });
            const clp = usdt * tasa;

            const saldoClpActual = Number(await readConfigValue('saldoClpDisponible') || 0);
            const saldoUsdtActual = Number(await readConfigValue('saldoUsdt') || 0);

            await dbRun(
                `INSERT INTO compras_usdt(usuario_id, clp_invertido, usdt_obtenido, tasa_clp_usdt, fecha, observaciones) VALUES (?,?,?,?,?,?)`,
                [req.session.user.id, clp, usdt, tasa, hoyLocalYYYYMMDD(), msgOriginal]
            );
            await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) - ? WHERE clave = 'saldoClpDisponible'`, [clp]);
            await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoUsdt'`, [usdt]);

            const pppUsdt = await getPromedioUsdt();
            tipo = 'compra_usdt';
            resultado = {
                mensaje: `Compra USDT registrada`,
                detalle: `${clp.toLocaleString('es-CL')} CLP → ${usdt.toLocaleString('es-CL')} USDT (tasa: ${tasa.toFixed(2)} CLP/USDT)`,
                saldos: {
                    clp: { antes: saldoClpActual, despues: saldoClpActual - clp },
                    usdt: { antes: saldoUsdtActual, despues: saldoUsdtActual + usdt },
                },
                pppUsdt: pppUsdt.toFixed(2)
            };
        }

        // Patrón: compré/compre/compra X USDT por Y CLP
        if (!tipo) {
        const matchCompraUsdt = msgOriginal.match(/(?:compr[eéa]|compro)\s+([\d.]+)\s*(?:usdt|USDT)\s+(?:por|a|con)\s+([\d.]+)\s*(?:clp|CLP|pesos)/i);
        if (matchCompraUsdt) {
            const usdt = Number(matchCompraUsdt[1]);
            const clp = Number(matchCompraUsdt[2]);
            if (usdt <= 0 || clp <= 0) return res.status(400).json({ error: 'Los montos deben ser mayores a 0' });

            const tasa = clp / usdt; // CLP por 1 USDT
            const saldoClpActual = Number(await readConfigValue('saldoClpDisponible') || 0);
            const saldoUsdtActual = Number(await readConfigValue('saldoUsdt') || 0);

            await dbRun(
                `INSERT INTO compras_usdt(usuario_id, clp_invertido, usdt_obtenido, tasa_clp_usdt, fecha, observaciones) VALUES (?,?,?,?,?,?)`,
                [req.session.user.id, clp, usdt, tasa, hoyLocalYYYYMMDD(), msgOriginal]
            );
            await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) - ? WHERE clave = 'saldoClpDisponible'`, [clp]);
            await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoUsdt'`, [usdt]);

            const pppUsdt = await getPromedioUsdt();
            tipo = 'compra_usdt';
            resultado = {
                mensaje: `Compra USDT registrada`,
                detalle: `${clp.toLocaleString('es-CL')} CLP → ${usdt.toLocaleString('es-CL')} USDT (tasa: ${tasa.toFixed(2)} CLP/USDT)`,
                saldos: {
                    clp: { antes: saldoClpActual, despues: saldoClpActual - clp },
                    usdt: { antes: saldoUsdtActual, despues: saldoUsdtActual + usdt },
                },
                pppUsdt: pppUsdt.toFixed(2)
            };
        }
        }

        // Patrón: cambié X USDT a tasa Y VES (calcula VES = X * Y)
        if (!tipo) {
            const matchCompraVesTasa = msgOriginal.match(/(?:cambi[eéo]|cambio|compr[eéa]|compro)\s+([\d.]+)\s*(?:usdt|USDT)\s+(?:a\s+)?tasa\s+([\d.]+)\s*(?:ves|VES|bol[ií]vares)?/i);
            if (matchCompraVesTasa) {
                const usdt = Number(matchCompraVesTasa[1]);
                const tasa = Number(matchCompraVesTasa[2]);
                if (usdt <= 0 || tasa <= 0) return res.status(400).json({ error: 'Los montos deben ser mayores a 0' });
                const ves = usdt * tasa;

                const saldoUsdtActual = Number(await readConfigValue('saldoUsdt') || 0);
                if (usdt > saldoUsdtActual) return res.status(400).json({ error: `No tienes suficiente USDT. Stock actual: ${saldoUsdtActual.toFixed(2)}` });

                const saldoVesActual = Number(await readConfigValue('saldoVesOnline') || 0);

                await dbRun(
                    `INSERT INTO compras_ves(usuario_id, usdt_invertido, ves_obtenido, tasa_usdt_ves, fecha, observaciones) VALUES (?,?,?,?,?,?)`,
                    [req.session.user.id, usdt, ves, tasa, hoyLocalYYYYMMDD(), msgOriginal]
                );
                await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) - ? WHERE clave = 'saldoUsdt'`, [usdt]);
                await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoVesOnline'`, [ves]);

                const pppVes = await getPromedioVesEnUsdt();
                tipo = 'compra_ves';
                resultado = {
                    mensaje: `Compra VES registrada`,
                    detalle: `${usdt.toLocaleString('es-CL')} USDT → ${ves.toLocaleString('es-CL')} VES (tasa: ${tasa.toFixed(2)} VES/USDT)`,
                    saldos: {
                        usdt: { antes: saldoUsdtActual, despues: saldoUsdtActual - usdt },
                        ves: { antes: saldoVesActual, despues: saldoVesActual + ves },
                    },
                    pppVesUsdt: pppVes.toFixed(2)
                };
            }
        }

        // Patrón: cambié/cambie/cambio X USDT por Y VES
        if (!tipo) {
            const matchCompraVes = msgOriginal.match(/(?:cambi[eéo]|cambio|compr[eéa]|compro)\s+([\d.]+)\s*(?:usdt|USDT)\s+(?:por|a)\s+([\d.]+)\s*(?:ves|VES|bol[ií]vares)/i);
            if (matchCompraVes) {
                const usdt = Number(matchCompraVes[1]);
                const ves = Number(matchCompraVes[2]);
                if (usdt <= 0 || ves <= 0) return res.status(400).json({ error: 'Los montos deben ser mayores a 0' });

                const saldoUsdtActual = Number(await readConfigValue('saldoUsdt') || 0);
                if (usdt > saldoUsdtActual) return res.status(400).json({ error: `No tienes suficiente USDT. Stock actual: ${saldoUsdtActual.toFixed(2)}` });

                const tasa = ves / usdt; // VES por 1 USDT
                const saldoVesActual = Number(await readConfigValue('saldoVesOnline') || 0);

                await dbRun(
                    `INSERT INTO compras_ves(usuario_id, usdt_invertido, ves_obtenido, tasa_usdt_ves, fecha, observaciones) VALUES (?,?,?,?,?,?)`,
                    [req.session.user.id, usdt, ves, tasa, hoyLocalYYYYMMDD(), msgOriginal]
                );
                await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) - ? WHERE clave = 'saldoUsdt'`, [usdt]);
                await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoVesOnline'`, [ves]);

                const pppVes = await getPromedioVesEnUsdt();
                tipo = 'compra_ves';
                resultado = {
                    mensaje: `Compra VES registrada`,
                    detalle: `${usdt.toLocaleString('es-CL')} USDT → ${ves.toLocaleString('es-CL')} VES (tasa: ${tasa.toFixed(2)} VES/USDT)`,
                    saldos: {
                        usdt: { antes: saldoUsdtActual, despues: saldoUsdtActual - usdt },
                        ves: { antes: saldoVesActual, despues: saldoVesActual + ves },
                    },
                    pppVesUsdt: pppVes.toFixed(2)
                };
            }
        }

        // Patrón: deposité/deposito/ingresé X CLP
        if (!tipo) {
            const matchDeposito = msgOriginal.match(/(?:deposit[eéo]|deposito|ingres[eéo]|ingreso|agreg[eéo]|agrego)\s+([\d.]+)\s*(?:clp|CLP|pesos)/i);
            if (matchDeposito) {
                const clp = Number(matchDeposito[1]);
                if (clp <= 0) return res.status(400).json({ error: 'El monto debe ser mayor a 0' });

                const saldoClpActual = Number(await readConfigValue('saldoClpDisponible') || 0);
                await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoClpDisponible'`, [clp]);
                await dbRun(
                    `INSERT INTO movimientos_stock(usuario_id, tipo, moneda, monto, saldo_antes, saldo_despues, fecha, observaciones) VALUES (?,?,?,?,?,?,?,?)`,
                    [req.session.user.id, 'deposito', 'CLP', clp, saldoClpActual, saldoClpActual + clp, hoyLocalYYYYMMDD(), msgOriginal]
                );

                tipo = 'deposito_clp';
                resultado = {
                    mensaje: `Depósito CLP registrado`,
                    detalle: `+${clp.toLocaleString('es-CL')} CLP`,
                    saldos: {
                        clp: { antes: saldoClpActual, despues: saldoClpActual + clp }
                    }
                };
            }
        }

        // Patrón: ajustar USDT/VES/CLP a X
        if (!tipo) {
            const matchAjuste = msgOriginal.match(/ajust(?:ar|e|o)\s+(usdt|ves|clp)\s+(?:a|en)\s+([\d.]+)/i);
            if (matchAjuste) {
                const moneda = matchAjuste[1].toUpperCase();
                const nuevoSaldo = Number(matchAjuste[2]);

                const claveMap = { 'CLP': 'saldoClpDisponible', 'USDT': 'saldoUsdt', 'VES': 'saldoVesOnline' };
                const clave = claveMap[moneda];
                const saldoAntes = Number(await readConfigValue(clave) || 0);
                await dbRun(`UPDATE configuracion SET valor = ? WHERE clave = ?`, [String(nuevoSaldo), clave]);
                await dbRun(
                    `INSERT INTO movimientos_stock(usuario_id, tipo, moneda, monto, saldo_antes, saldo_despues, fecha, observaciones) VALUES (?,?,?,?,?,?,?,?)`,
                    [req.session.user.id, 'ajuste', moneda, nuevoSaldo, saldoAntes, nuevoSaldo, hoyLocalYYYYMMDD(), msgOriginal]
                );

                tipo = 'ajuste';
                resultado = {
                    mensaje: `Ajuste de ${moneda} registrado`,
                    detalle: `${moneda}: ${saldoAntes.toLocaleString('es-CL')} → ${nuevoSaldo.toLocaleString('es-CL')}`,
                    saldos: { [moneda.toLowerCase()]: { antes: saldoAntes, despues: nuevoSaldo } }
                };
            }
        }

        // Patrón: vendí X USDT por Y CLP [país]
        if (!tipo) {
            const matchVentaUsdt = msgOriginal.match(/(?:vend[ií]|vendo|venta)\s+([\d.]+)\s*(?:usdt|USDT)\s+(?:por|a)\s+([\d.]+)\s*(?:clp|CLP|pesos)\s*(.*)/i);
            if (matchVentaUsdt) {
                const usdt = Number(matchVentaUsdt[1]);
                const clp = Number(matchVentaUsdt[2]);
                const paisRaw = matchVentaUsdt[3] ? matchVentaUsdt[3].trim() : '';
                if (usdt <= 0 || clp <= 0) return res.status(400).json({ error: 'Los montos deben ser mayores a 0' });

                const saldoUsdtActual = Number(await readConfigValue('saldoUsdt') || 0);
                if (usdt > saldoUsdtActual) return res.status(400).json({ error: `Saldo USDT insuficiente. Disponible: ${saldoUsdtActual.toFixed(2)}` });

                const saldoClpActual = Number(await readConfigValue('saldoClpDisponible') || 0);
                const tasa = clp / usdt; // CLP por 1 USDT

                // Mapear país
                const paisMap = {
                    've': 'Venezuela', 'venezuela': 'Venezuela',
                    'co': 'Colombia', 'colombia': 'Colombia', 'col': 'Colombia',
                    'pe': 'Peru', 'peru': 'Peru',
                    'bo': 'Bolivia', 'bolivia': 'Bolivia',
                    'ar': 'Argentina', 'argentina': 'Argentina',
                    'us': 'EEUU', 'usa': 'EEUU', 'eeuu': 'EEUU', 'estados unidos': 'EEUU',
                    'eu': 'Europa', 'europa': 'Europa', 'eur': 'Europa',
                };
                const pais = paisMap[paisRaw.toLowerCase()] || paisRaw || 'No especificado';
                const observacion = `Venta USDT → ${pais} | ${msgOriginal}`;

                await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) - ? WHERE clave = 'saldoUsdt'`, [usdt]);
                await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoClpDisponible'`, [clp]);
                await dbRun(
                    `INSERT INTO movimientos_stock(usuario_id, tipo, moneda, monto, saldo_antes, saldo_despues, fecha, observaciones) VALUES (?,?,?,?,?,?,?,?)`,
                    [req.session.user.id, 'venta_usdt', 'USDT', usdt, saldoUsdtActual, saldoUsdtActual - usdt, hoyLocalYYYYMMDD(), observacion]
                );

                // Calcular ganancia de esta venta
                const pppUsdtClp = await getPromedioUsdt(); // costo promedio de 1 USDT en CLP
                const costoEnClp = usdt * pppUsdtClp;
                const gananciaVenta = clp - costoEnClp;

                tipo = 'venta_usdt';
                resultado = {
                    mensaje: `Venta USDT registrada (${pais})`,
                    detalle: `${usdt.toLocaleString('es-CL')} USDT → ${clp.toLocaleString('es-CL')} CLP (tasa: ${tasa.toFixed(2)} CLP/USDT)`,
                    pais,
                    ganancia: `${gananciaVenta >= 0 ? '+' : ''}${gananciaVenta.toFixed(0)} CLP`,
                    saldos: {
                        usdt: { antes: saldoUsdtActual, despues: saldoUsdtActual - usdt },
                        clp: { antes: saldoClpActual, despues: saldoClpActual + clp }
                    }
                };
            }
        }

        // Patrón: retiré/retiro X CLP/USDT/VES para/por [motivo]
        if (!tipo) {
            const matchRetiro = msgOriginal.match(/(?:retir[eéo]|retiro|saq[uú][eé]|saco|pag[uú][eé]|pago)\s+([\d.]+)\s*(clp|usdt|ves)\s*(?:para|por|de|motivo:?\s*)?\s*(.*)/i);
            if (matchRetiro) {
                const monto = Number(matchRetiro[1]);
                const moneda = matchRetiro[2].toUpperCase();
                const motivo = matchRetiro[3] ? matchRetiro[3].trim() : 'Sin justificación';
                if (monto <= 0) return res.status(400).json({ error: 'El monto debe ser mayor a 0' });

                const claveMap = { 'CLP': 'saldoClpDisponible', 'USDT': 'saldoUsdt', 'VES': 'saldoVesOnline' };
                const clave = claveMap[moneda];
                const saldoActual = Number(await readConfigValue(clave) || 0);

                if (monto > saldoActual) {
                    return res.status(400).json({ error: `Saldo ${moneda} insuficiente. Disponible: ${saldoActual.toLocaleString('es-CL')}` });
                }

                await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) - ? WHERE clave = ?`, [monto, clave]);
                await dbRun(
                    `INSERT INTO movimientos_stock(usuario_id, tipo, moneda, monto, saldo_antes, saldo_despues, fecha, observaciones) VALUES (?,?,?,?,?,?,?,?)`,
                    [req.session.user.id, 'retiro', moneda, monto, saldoActual, saldoActual - monto, hoyLocalYYYYMMDD(), motivo]
                );

                tipo = 'retiro';
                resultado = {
                    mensaje: `Retiro de ${moneda} registrado`,
                    detalle: `-${monto.toLocaleString('es-CL')} ${moneda} | Motivo: ${motivo}`,
                    saldos: { [moneda.toLowerCase()]: { antes: saldoActual, despues: saldoActual - monto } }
                };
            }
        }

        if (!tipo) {
            return res.status(400).json({
                error: 'No entendí la operación. Ejemplos válidos:',
                ejemplos: [
                    'compré 1000 USDT por 950000 CLP',
                    'compré 500 USDT a tasa 932',
                    'cambié 500 USDT por 42500 VES',
                    'cambié 200 USDT a tasa 85.5 VES',
                    'vendí 90 USDT por 100000 CLP Colombia',
                    'vendí 50 USDT por 55000 CLP EEUU',
                    'deposité 2000000 CLP',
                    'retiré 50000 CLP para pago de servicios',
                    'ajustar USDT a 320.5'
                ]
            });
        }

        res.json({ tipo, ...resultado });
    } catch (error) {
        console.error('Error en POST /api/stock/operacion:', error.message);
        res.status(500).json({ error: 'Error procesando la operación' });
    }
});

// GET /api/stock/historial — historial combinado de operaciones
app.get('/api/stock/historial', apiAuth, onlyMaster, async (req, res) => {
    try {
        const comprasUsdt = await dbAll(
            `SELECT id, 'compra_usdt' as tipo, clp_invertido, usdt_obtenido as monto_obtenido, tasa_clp_usdt as tasa, fecha, observaciones FROM compras_usdt ORDER BY fecha DESC, id DESC`
        );
        const comprasVes = await dbAll(
            `SELECT id, 'compra_ves' as tipo, usdt_invertido, ves_obtenido as monto_obtenido, tasa_usdt_ves as tasa, fecha, observaciones FROM compras_ves ORDER BY fecha DESC, id DESC`
        );

        const movimientos = await dbAll(
            `SELECT id, tipo, moneda, monto, saldo_antes, saldo_despues, fecha, observaciones FROM movimientos_stock ORDER BY fecha DESC, id DESC`
        );

        // Combinar y ordenar por fecha DESC
        const historial = [
            ...(comprasUsdt || []).map(c => ({
                id: c.id, tipo: 'CLP → USDT', tipoKey: 'compra_usdt',
                entrada: `${Number(c.monto_obtenido).toLocaleString('es-CL')} USDT`,
                salida: `${Number(c.clp_invertido).toLocaleString('es-CL')} CLP`,
                tasa: `${Number(c.tasa).toFixed(2)} CLP/USDT`,
                fecha: c.fecha, observaciones: c.observaciones
            })),
            ...(comprasVes || []).map(c => ({
                id: c.id, tipo: 'USDT → VES', tipoKey: 'compra_ves',
                entrada: `${Number(c.monto_obtenido).toLocaleString('es-CL')} VES`,
                salida: `${Number(c.usdt_invertido).toLocaleString('es-CL')} USDT`,
                tasa: `${Number(c.tasa).toFixed(2)} VES/USDT`,
                fecha: c.fecha, observaciones: c.observaciones
            })),
            ...(movimientos || []).map(m => {
                let tipoDisplay, entrada, salida;
                if (m.tipo === 'deposito') {
                    tipoDisplay = `Depósito ${m.moneda}`;
                    entrada = `+${Number(m.monto).toLocaleString('es-CL')} ${m.moneda}`;
                    salida = '-';
                } else if (m.tipo === 'retiro') {
                    tipoDisplay = `Retiro ${m.moneda}`;
                    entrada = '-';
                    salida = `-${Number(m.monto).toLocaleString('es-CL')} ${m.moneda}`;
                } else if (m.tipo === 'venta_usdt') {
                    // Extraer país de observaciones: "Venta USDT → Colombia | ..."
                    const paisMatch = m.observaciones ? m.observaciones.match(/→\s*([^|]+)/) : null;
                    const pais = paisMatch ? paisMatch[1].trim() : '';
                    tipoDisplay = `Venta USDT${pais ? ' → ' + pais : ''}`;
                    entrada = `+${Number(m.saldo_antes - m.saldo_despues).toLocaleString('es-CL')} USDT vendido`;
                    salida = `-${Number(m.monto).toLocaleString('es-CL')} USDT`;
                } else {
                    tipoDisplay = `Ajuste ${m.moneda}`;
                    entrada = `${Number(m.saldo_despues).toLocaleString('es-CL')} ${m.moneda}`;
                    salida = `${Number(m.saldo_antes).toLocaleString('es-CL')} ${m.moneda}`;
                }
                return {
                    id: m.id, tipo: tipoDisplay, tipoKey: 'movimiento',
                    entrada, salida,
                    tasa: (m.tipo === 'retiro' || m.tipo === 'venta_usdt') ? (m.observaciones || '-') : '-',
                    fecha: m.fecha, observaciones: m.observaciones
                };
            })
        ].sort((a, b) => b.fecha > a.fecha ? 1 : b.fecha < a.fecha ? -1 : 0);

        res.json(historial);
    } catch (error) {
        console.error('Error en GET /api/stock/historial:', error.message);
        res.status(500).json({ error: 'Error al consultar historial' });
    }
});

// DELETE /api/compras-usdt/:id — borrar compra de USDT y revertir saldos
app.delete('/api/compras-usdt/:id', apiAuth, onlyMaster, async (req, res) => {
    try {
        const compra = await dbGet('SELECT * FROM compras_usdt WHERE id = ?', [req.params.id]);
        if (!compra) return res.status(404).json({ error: 'Compra no encontrada' });

        await dbRun('DELETE FROM compras_usdt WHERE id = ?', [req.params.id]);
        await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoClpDisponible'`, [compra.clp_invertido]);
        await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) - ? WHERE clave = 'saldoUsdt'`, [compra.usdt_obtenido]);

        res.json({ message: 'Compra USDT eliminada y saldos revertidos' });
    } catch (error) {
        console.error('Error borrando compra USDT:', error.message);
        res.status(500).json({ error: 'Error al eliminar' });
    }
});

// DELETE /api/compras-ves/:id — borrar compra de VES y revertir saldos
app.delete('/api/compras-ves/:id', apiAuth, onlyMaster, async (req, res) => {
    try {
        const compra = await dbGet('SELECT * FROM compras_ves WHERE id = ?', [req.params.id]);
        if (!compra) return res.status(404).json({ error: 'Compra no encontrada' });

        await dbRun('DELETE FROM compras_ves WHERE id = ?', [req.params.id]);
        await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = 'saldoUsdt'`, [compra.usdt_invertido]);
        await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) - ? WHERE clave = 'saldoVesOnline'`, [compra.ves_obtenido]);

        res.json({ message: 'Compra VES eliminada y saldos revertidos' });
    } catch (error) {
        console.error('Error borrando compra VES:', error.message);
        res.status(500).json({ error: 'Error al eliminar' });
    }
});

// DELETE /api/movimientos-stock/:id — borrar movimiento y revertir saldo
app.delete('/api/movimientos-stock/:id', apiAuth, onlyMaster, async (req, res) => {
    try {
        const mov = await dbGet('SELECT * FROM movimientos_stock WHERE id = ?', [req.params.id]);
        if (!mov) return res.status(404).json({ error: 'Movimiento no encontrado' });

        const claveMap = { 'CLP': 'saldoClpDisponible', 'USDT': 'saldoUsdt', 'VES': 'saldoVesOnline' };
        const clave = claveMap[mov.moneda];

        if (mov.tipo === 'deposito') {
            // Depósito sumó monto → restar
            await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) - ? WHERE clave = ?`, [mov.monto, clave]);
        } else if (mov.tipo === 'retiro' || mov.tipo === 'venta_usdt') {
            // Retiro/venta restó monto → devolver
            await dbRun(`UPDATE configuracion SET valor = CAST(valor AS REAL) + ? WHERE clave = ?`, [mov.monto, clave]);
        } else if (mov.tipo === 'ajuste') {
            // Ajuste cambió saldo → restaurar saldo anterior
            await dbRun(`UPDATE configuracion SET valor = ? WHERE clave = ?`, [String(mov.saldo_antes), clave]);
        }
        await dbRun('DELETE FROM movimientos_stock WHERE id = ?', [req.params.id]);

        res.json({ message: 'Movimiento eliminado y saldo revertido' });
    } catch (error) {
        console.error('Error borrando movimiento:', error.message);
        res.status(500).json({ error: 'Error al eliminar' });
    }
});

// =================================================================
// INICIO: ENDPOINTS DE ANALYTICS AVANZADO
// =================================================================

// " ENDPOINT 1: Análisis de comportamiento de clientes
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
        const hace30 = new Date(hoy.getTime() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
        const hace60 = new Date(hoy.getTime() - 60 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

        const analisis = [];
        for (const c of clientes) {
            const ultimaOp = c.ultima_operacion ? new Date(c.ultima_operacion) : null;
            const primeraOp = c.primera_operacion ? new Date(c.primera_operacion) : null;
            const diasDesdeUltimo = ultimaOp ? Math.floor((hoy - ultimaOp) / (1000 * 60 * 60 * 24)) : null;

            let frecuencia = 'Sin actividad';
            let tendencia = 'estable';

            if (c.total_operaciones > 0 && primeraOp && ultimaOp) {
                if (c.total_operaciones === 1) {
                    frecuencia = 'Única operación';
                } else {
                    const diasActivo = Math.max(1, Math.floor((ultimaOp - primeraOp) / (1000 * 60 * 60 * 24)));
                    const promedioDias = diasActivo / Math.max(1, c.total_operaciones - 1);

                    if (c.total_operaciones >= 5 && promedioDias <= 2) frecuencia = 'Diario';
                    else if (promedioDias <= 7) frecuencia = 'Semanal';
                    else if (promedioDias <= 30) frecuencia = 'Mensual';
                    else frecuencia = 'Esporádico';
                }

                // Análisis de tendencia con await (fix race condition)
                const recientes = await dbGet(`SELECT COUNT(*) as cnt FROM operaciones WHERE cliente_id = ? AND fecha >= ?`, [c.id, hace30]);
                const anteriores = await dbGet(`SELECT COUNT(*) as cnt FROM operaciones WHERE cliente_id = ? AND fecha >= ? AND fecha < ?`, [c.id, hace60, hace30]);

                if (recientes && anteriores) {
                    if (recientes.cnt > anteriores.cnt * 1.2) tendencia = 'creciente';
                    else if (recientes.cnt < anteriores.cnt * 0.8) tendencia = 'decreciente';
                }
            }

            analisis.push({
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
            });
        }

        res.json(analisis);
    } catch (error) {
        console.error('Error en análisis de comportamiento:', error);
        res.status(500).json({ message: 'Error al analizar comportamiento de clientes' });
    }
});

//  ENDPOINT 2: Alertas y clientes en riesgo
app.get('/api/analytics/clientes/alertas', apiAuth, onlyMaster, async (req, res) => {
    try {
        const alertas = [];
        const hoy = new Date();
        const fechaHoy = hoyLocalYYYYMMDD();
        
        // Clientes inactivos (30-60 dias)
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
                // Retornar alerta existente con accion si existe
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
        
        // Clientes criticos (+60 dias)
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

        // ALERTAS DE DISMINUCION DE FRECUENCIA DESACTIVADAS
        // Las alertas por cambios en la frecuencia de operaciones han sido desactivadas
        // Se mantienen solo alertas de inactividad (30-60 dias) y criticas (+60 dias)

        res.json(alertas);
    } catch (error) {
        console.error('Error en alertas:', error);
        res.status(500).json({ message: 'Error al generar alertas' });
    }
});

// ' ENDPOINT 3: Análisis de clientes nuevos
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

// " ENDPOINT 4: Detalle profundo de un cliente
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
            LIMIT 10
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
            LIMIT 6
        `, [clienteId]);

        res.json({
            cliente,
            estadisticas: stats,
            historial_mensual: porMes,
            ultimas_operaciones: operaciones
        });
    } catch (error) {
        console.error('Error en detalle de cliente:', error);
        res.status(500).json({ message: 'Error al obtener detalle del cliente' });
    }
});

//  ENDPOINT 5: Dashboard de metas
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

//  ENDPOINT 6: Configurar metas
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
 * Obtiene la tasa oficial USD/VES del BCV via bcvapi.tech
 * Se ejecuta al arrancar y cada 12 horas
 */
async function obtenerTasaBCV() {
    try {
        // Usar dolarapi.com (gratis, sin API key, open source)
        const response = await axios.get('https://ve.dolarapi.com/v1/dolares/oficial', {
            timeout: 10000
        });

        const tasa = response.data?.promedio;
        const fechaRaw = response.data?.fechaActualizacion;
        const fecha = fechaRaw ? fechaRaw.split('T')[0] : new Date().toISOString().split('T')[0];

        if (tasa && tasa > 0) {
            await dbRun("UPDATE configuracion SET valor = ? WHERE clave = 'tasaBCV'", [String(tasa)]);
            await dbRun("UPDATE configuracion SET valor = ? WHERE clave = 'tasaBCVFecha'", [fecha]);
            console.log(`Tasa BCV actualizada: ${tasa} VES/USD (${fecha})`);
            return tasa;
        }
    } catch (error) {
        console.error('Error obteniendo tasa BCV:', error.message);
    }
    return null;
}

// =================================================================
// MOTOR DE CONCILIACIÓN AUTOMÁTICA
// =================================================================

function normalizarRut(rut) {
    if (!rut) return '';
    return rut.replace(/[\.\-\s]/g, '').toUpperCase().trim();
}

async function conciliarPedidos() {
    try {
        // Verificar si la conciliación está activa
        const configActiva = await dbGet("SELECT valor FROM configuracion WHERE clave = 'conciliacion_activa'");
        if (configActiva && configActiva.valor === '0') return;
        console.log('[CONCILIACIÓN] Ciclo ejecutado -', new Date().toLocaleString('es-CL'));

        // 1. Buscar transferencias bancarias disponibles (no tomadas ni procesadas)
        const transferencias = await dbAll(
            `SELECT id, n_operacion, rut_origen, monto_numerico, nombre_origen, fecha
             FROM transferencias_banco
             WHERE estado = 'disponible' AND monto_numerico > 0`
        );

        // 2. Buscar pedidos pendientes sin conciliar (SOLO transferencias, NO caja vecina)
        const pedidos = await dbAll(
            `SELECT s.id, s.monto_origen, s.cliente_app_id, s.fecha_solicitud, s.telegram_message_id,
                    s.conciliacion_notificado, c.documento_numero, c.nombre
             FROM solicitudes_transferencia s
             JOIN clientes_app c ON s.cliente_app_id = c.id
             WHERE s.estado = 'pendiente'
               AND s.transferencia_banco_id IS NULL
               AND (s.metodo_pago IS NULL OR s.metodo_pago = 'transferencia_bancaria')
               AND c.documento_numero IS NOT NULL
             ORDER BY s.fecha_solicitud ASC`
        );

        // 2.5 Notificar en Telegram que el bot está monitoreando pedidos nuevos
        if (pedidos && pedidos.length > 0) {
            for (const p of pedidos) {
                if (p.telegram_message_id && !p.conciliacion_notificado) {
                    const minutos = Math.round((Date.now() - new Date(p.fecha_solicitud).getTime()) / 60000);
                    const tiempoRestante = Math.max(10 - minutos, 0);
                    try {
                        // Obtener datos del pedido original para mantener la info
                        const pedidoCompleto = await dbGet(
                            `SELECT s.*, b.nombre_completo, b.banco, b.numero_cuenta, b.documento_numero as benef_doc,
                                    b.tipo_cuenta, b.telefono as benef_tel, c.nombre as cliente_nombre, c.telefono as cliente_tel
                             FROM solicitudes_transferencia s
                             LEFT JOIN beneficiarios b ON s.beneficiario_id = b.id
                             LEFT JOIN clientes_app c ON s.cliente_app_id = c.id
                             WHERE s.id = ?`, [p.id]
                        );

                        if (pedidoCompleto) {
                            const cuenta = pedidoCompleto.numero_cuenta || 'No disponible';
                            const cedula = pedidoCompleto.benef_doc || 'No disponible';
                            const nombre = pedidoCompleto.nombre_completo || 'No disponible';
                            const banco = pedidoCompleto.banco || 'No disponible';

                            await editarMensajeTelegram(
                                p.telegram_message_id,
                                `🤖 <b>PEDIDO #${p.id} - MONITOREANDO</b>\n\n` +
                                `👤 ${p.nombre}\n` +
                                `💰 $${Math.round(p.monto_origen).toLocaleString('es-CL')} CLP\n` +
                                `🆔 RUT: ${p.documento_numero}\n\n` +
                                `🏦 ${nombre} - ${banco}\n` +
                                `💳 ${cuenta}\n` +
                                `🪪 ${cedula}\n\n` +
                                `⏳ Bot buscando transferencia...\n` +
                                `⏰ Tiempo restante: ${tiempoRestante} min\n` +
                                `🔄 Auto-cancela si no se detecta en 10 min`,
                                [
                                    [
                                        { text: `💳 ${cuenta}`, copy_text: { text: cuenta } },
                                        { text: `🪪 ${cedula}`, copy_text: { text: cedula } }
                                    ],
                                    [
                                        { text: `👤 ${nombre.substring(0, 15)}`, copy_text: { text: nombre } },
                                        { text: `🏦 ${banco.substring(0, 12)}`, copy_text: { text: banco } }
                                    ],
                                    [
                                        { text: '✅ COMPLETADO', callback_data: `completar_${p.id}` },
                                        { text: '❌ CANCELAR', callback_data: `cancelar_${p.id}` }
                                    ]
                                ]
                            );
                        }
                    } catch (e) { console.error('[CONCILIACIÓN] Error notificando monitoreo:', e.message); }

                    await dbRun('UPDATE solicitudes_transferencia SET conciliacion_notificado = 1 WHERE id = ?', [p.id]);
                }
            }
        }

        let conciliados = 0;

        // 3. Conciliar: match por RUT + monto exacto
        if (transferencias && transferencias.length > 0 && pedidos && pedidos.length > 0) {
            for (const transf of transferencias) {
                const rutTransf = normalizarRut(transf.rut_origen);
                if (!rutTransf) continue;

                const pedidoMatch = pedidos.find(p => {
                    const rutPedido = normalizarRut(p.documento_numero);
                    return rutPedido === rutTransf && Math.round(p.monto_origen) === transf.monto_numerico;
                });

                if (!pedidoMatch) continue;

                // Vincular transferencia con pedido y tomarlo como operador automático
                await dbRun(
                    `UPDATE solicitudes_transferencia
                     SET estado = 'procesando',
                         transferencia_banco_id = ?,
                         referencia = ?,
                         tomado_por_nombre = 'Bot Conciliación',
                         fecha_tomado = ?
                     WHERE id = ?`,
                    [transf.id, transf.n_operacion, new Date().toISOString(), pedidoMatch.id]
                );

                await dbRun(
                    `UPDATE transferencias_banco SET estado = 'procesado' WHERE id = ?`,
                    [transf.id]
                );

                // Remover pedido conciliado de la lista
                const idx = pedidos.indexOf(pedidoMatch);
                if (idx > -1) pedidos.splice(idx, 1);

                conciliados++;

                console.log(`[CONCILIACIÓN] Pedido #${pedidoMatch.id} conciliado con transferencia N°${transf.n_operacion} (RUT: ${transf.rut_origen}, $${transf.monto_numerico})`);

                try {
                    // Editar mensaje original de Telegram (quitar botones, mostrar que fue tomado)
                    const solicitudData = await dbGet('SELECT telegram_message_id FROM solicitudes_transferencia WHERE id = ?', [pedidoMatch.id]);
                    if (solicitudData && solicitudData.telegram_message_id) {
                        await editarMensajeTelegram(
                            solicitudData.telegram_message_id,
                            `🤖 <b>CONCILIADO AUTOMÁTICAMENTE</b>\n\n` +
                            `📋 Pedido #${pedidoMatch.id}\n` +
                            `👤 ${pedidoMatch.nombre}\n` +
                            `💰 Monto: $${Math.round(pedidoMatch.monto_origen).toLocaleString('es-CL')} CLP\n\n` +
                            `🔗 Transferencia N°${transf.n_operacion}\n` +
                            `📥 Tomado por: <b>Bot Conciliación</b>\n` +
                            `⏰ ${new Date().toLocaleString('es-VE', { timeZone: 'America/Caracas' })}`,
                            [] // Sin botones
                        );
                    }
                    // Enviar notificación adicional
                    await enviarNotificacionTelegram(
                        `🤖 <b>Conciliación Automática</b>\n\n` +
                        `📋 Pedido #${pedidoMatch.id} → Transferencia N°${transf.n_operacion}\n` +
                        `👤 ${transf.nombre_origen}\n` +
                        `🆔 RUT: ${transf.rut_origen}\n` +
                        `💰 Monto: $${transf.monto_numerico.toLocaleString('es-CL')}\n\n` +
                        `📥 Pedido <b>tomado</b> por Bot Conciliación\n` +
                        `✅ Estado: <b>procesando</b>`
                    );
                } catch (e) { console.error('[CONCILIACIÓN] Error Telegram:', e.message); }

                // Enviar al bot pagador de Bancamiga
                try {
                    await enviarAlBotPagador(pedidoMatch.id);
                } catch (e) { console.error('[CONCILIACIÓN] Error enviando al bot pagador:', e.message); }
            }

            if (conciliados > 0) {
                console.log(`[CONCILIACIÓN] ${conciliados} pedido(s) conciliado(s) automáticamente`);
            }
        }

        // 4. Auto-cancelar pedidos de transferencia sin match después de 10 minutos
        const pedidosVencidos = await dbAll(
            `SELECT s.id, s.monto_origen, s.fecha_solicitud, s.telegram_message_id, c.nombre, c.documento_numero
             FROM solicitudes_transferencia s
             JOIN clientes_app c ON s.cliente_app_id = c.id
             WHERE s.estado = 'pendiente'
               AND s.transferencia_banco_id IS NULL
               AND (s.metodo_pago IS NULL OR s.metodo_pago = 'transferencia_bancaria')
               AND strftime('%s', replace(replace(s.fecha_solicitud, 'T', ' '), 'Z', '')) < strftime('%s', datetime('now', '-10 minutes'))`
        );
        console.log(`[CONCILIACIÓN] Pedidos vencidos encontrados: ${pedidosVencidos ? pedidosVencidos.length : 0}`);

        if (pedidosVencidos && pedidosVencidos.length > 0) {
            for (const p of pedidosVencidos) {
                await dbRun(
                    `UPDATE solicitudes_transferencia
                     SET estado = 'cancelada',
                         notas_operador = 'Cancelado automáticamente: transferencia no detectada en 10 min'
                     WHERE id = ?`,
                    [p.id]
                );

                console.log(`[CONCILIACIÓN] Pedido #${p.id} cancelado automáticamente (sin transferencia en 10 min) - ${p.nombre} $${p.monto_origen}`);

                try {
                    // Editar mensaje original de Telegram (quitar botones)
                    if (p.telegram_message_id) {
                        await editarMensajeTelegram(
                            p.telegram_message_id,
                            `❌ <b>CANCELADO AUTOMÁTICAMENTE</b>\n\n` +
                            `📋 Pedido #${p.id}\n` +
                            `👤 ${p.nombre}\n` +
                            `💰 Monto: $${Math.round(p.monto_origen).toLocaleString('es-CL')} CLP\n\n` +
                            `⏰ Transferencia no detectada en 10 minutos\n` +
                            `🕐 ${new Date().toLocaleString('es-VE', { timeZone: 'America/Caracas' })}`,
                            [] // Sin botones
                        );
                    }
                    // Enviar notificación adicional
                    await enviarNotificacionTelegram(
                        `⏰ <b>Pedido Auto-Cancelado</b>\n\n` +
                        `📋 Pedido #${p.id}\n` +
                        `👤 ${p.nombre}\n` +
                        `🆔 RUT: ${p.documento_numero}\n` +
                        `💰 Monto: $${Math.round(p.monto_origen).toLocaleString('es-CL')}\n\n` +
                        `❌ Transferencia no detectada en 10 minutos`
                    );
                } catch (e) {}
            }

            console.log(`[CONCILIACIÓN] ${pedidosVencidos.length} pedido(s) cancelado(s) por timeout`);
        }
    } catch (error) {
        console.error('[CONCILIACIÓN] Error:', error.message);
    }
}

/**
 * Obtiene la mejor tasa de venta de USDT por VES (Bancamiga)
 * @returns {Promise<number>} - Precio en VES por 1 USDT
 */
async function obtenerTasaVentaVES() {
    try {
        const anuncios = await consultarBinanceP2P('VES', 'SELL', ['Bancamiga'], 200000);
        
        if (!anuncios || anuncios.length === 0) {
            throw new Error('No se encontraron ofertas de venta USDT por VES con Bancamiga');
        }

        // Filtrar por monto mínimo y ordenar por precio (más alto primero = mejor para vender)
        const ofertasValidas = anuncios
            .filter(ad => {
                const minLimit = parseFloat(ad.adv?.minSingleTransAmount || 0);
                return minLimit <= 200000;
            })
            .sort((a, b) => parseFloat(b.adv?.price || 0) - parseFloat(a.adv?.price || 0));

        if (ofertasValidas.length === 0) {
            throw new Error('No hay ofertas válidas con monto mínimo <= 200,000 VES');
        }

        const mejorOferta = ofertasValidas[0];
        const precio = parseFloat(mejorOferta.adv?.price || 0);

        console.log(`... Mejor oferta VES: ${precio} VES/USDT (Bancamiga)`);
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

        console.log(`... Mejor oferta COP: ${precio} COP/USDT (Bancolombia/Nequi)`);
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

        console.log(`... Mejor oferta PEN: ${precio} PEN/USDT (BCP/Yape)`);
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

        console.log(`... Mejor oferta BOB: ${precio} BOB/USDT (BancoGanadero/BancoEconomico)`);
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

        console.log(`... Mejor oferta ARS: ${precio} ARS/USDT (MercadoPago/Brubank/LemonCash)`);
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

        console.log(`... Mejor oferta CLP: ${precio} CLP/USDT`);
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
        console.log('"" Consultando tasas P2P VES/CLP...');

        // 1. Obtener tasas P2P
        const [tasa_ves_p2p, tasa_clp_p2p] = await Promise.all([
            obtenerTasaVentaVES(),
            obtenerTasaCompraCLP()
        ]);

        // 2. Calcular tasa base CLP †' VES
        const tasa_base_clp_ves = tasa_ves_p2p / tasa_clp_p2p;

        // 3. Calcular tasas ajustadas
        const tasa_menos_5 = tasa_base_clp_ves * (1 - 0.05);
        const tasa_menos_4_5 = tasa_base_clp_ves * (1 - 0.045);
        const tasa_menos_4 = tasa_base_clp_ves * (1 - 0.04);

        // 4. Truncar a 4 decimales SIN redondear
        const truncar = (num) => Math.floor(num * 10000) / 10000;

        const response = {
            tasa_ves_p2p: truncar(tasa_ves_p2p),
            tasa_clp_p2p: truncar(tasa_clp_p2p),
            tasa_base_clp_ves: truncar(tasa_base_clp_ves),
            tasas_ajustadas: {
                tasa_menos_5: truncar(tasa_menos_5),
                tasa_menos_4_5: truncar(tasa_menos_4_5),
                tasa_menos_4: truncar(tasa_menos_4)
            },
            metadata: {
                fuente: 'Binance P2P',
                banco_ves: 'Bancamiga',
                min_ves: 200000,
                timestamp: new Date().toISOString()
            }
        };

        console.log('... Tasas P2P calculadas exitosamente');
        res.json(response);
    } catch (error) {
        console.error(' Error en endpoint /api/p2p/tasas-ves-clp:', error.message);
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
        console.log('"" Consultando tasas P2P COP/CLP...');

        // 1. Obtener tasas P2P
        const [tasa_cop_p2p, tasa_clp_p2p] = await Promise.all([
            obtenerTasaVentaCOP(),
            obtenerTasaCompraCLP()
        ]);

        // 2. Calcular tasa base CLP †' COP
        const tasa_base_clp_cop = tasa_cop_p2p / tasa_clp_p2p;

        // 3. Calcular tasas ajustadas
        const tasa_menos_5 = tasa_base_clp_cop * (1 - 0.05);
        const tasa_menos_4_5 = tasa_base_clp_cop * (1 - 0.045);
        const tasa_menos_4 = tasa_base_clp_cop * (1 - 0.04);

        // 4. Truncar a 4 decimales SIN redondear
        const truncar = (num) => Math.floor(num * 10000) / 10000;

        const response = {
            tasa_cop_p2p: truncar(tasa_cop_p2p),
            tasa_clp_p2p: truncar(tasa_clp_p2p),
            tasa_base_clp_cop: truncar(tasa_base_clp_cop),
            tasas_ajustadas: {
                tasa_menos_5: truncar(tasa_menos_5),
                tasa_menos_4_5: truncar(tasa_menos_4_5),
                tasa_menos_4: truncar(tasa_menos_4)
            },
            metadata: {
                fuente: 'Binance P2P',
                bancos_cop: 'Bancolombia, Nequi',
                min_cop: 40000,
                timestamp: new Date().toISOString()
            }
        };

        console.log('... Tasas P2P COP/CLP calculadas exitosamente');
        res.json(response);
    } catch (error) {
        console.error(' Error en endpoint /api/p2p/tasas-cop-clp:', error.message);
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
        console.log('"" Consultando tasas P2P PEN/CLP...');

        // 1. Obtener tasas P2P
        const [tasa_pen_p2p, tasa_clp_p2p] = await Promise.all([
            obtenerTasaVentaPEN(),
            obtenerTasaCompraCLP()
        ]);

        // 2. Calcular tasa base CLP †' PEN
        const tasa_base_clp_pen = tasa_pen_p2p / tasa_clp_p2p;

        // 3. Calcular tasas ajustadas
        const tasa_menos_5 = tasa_base_clp_pen * (1 - 0.05);
        const tasa_menos_4_5 = tasa_base_clp_pen * (1 - 0.045);
        const tasa_menos_4 = tasa_base_clp_pen * (1 - 0.04);

        // 4. Truncar a 6 decimales SIN redondear (PEN tiene valores más pequeños)
        const truncar = (num) => Math.floor(num * 1000000) / 1000000;

        const response = {
            tasa_pen_p2p: truncar(tasa_pen_p2p),
            tasa_clp_p2p: truncar(tasa_clp_p2p),
            tasa_base_clp_pen: truncar(tasa_base_clp_pen),
            tasas_ajustadas: {
                tasa_menos_5: truncar(tasa_menos_5),
                tasa_menos_4_5: truncar(tasa_menos_4_5),
                tasa_menos_4: truncar(tasa_menos_4)
            },
            metadata: {
                fuente: 'Binance P2P',
                bancos_pen: 'BCP, Yape',
                min_pen: 30,
                timestamp: new Date().toISOString()
            }
        };

        console.log('... Tasas P2P PEN/CLP calculadas exitosamente');
        res.json(response);
    } catch (error) {
        console.error(' Error en endpoint /api/p2p/tasas-pen-clp:', error.message);
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
        console.log('"" Consultando tasas P2P BOB/CLP...');

        // 1. Obtener tasas P2P
        const [tasa_bob_p2p, tasa_clp_p2p] = await Promise.all([
            obtenerTasaVentaBOB(),
            obtenerTasaCompraCLP()
        ]);

        // 2. Calcular tasa base CLP †' BOB
        const tasa_base_clp_bob = tasa_bob_p2p / tasa_clp_p2p;

        // 3. Calcular tasas ajustadas
        const tasa_menos_5 = tasa_base_clp_bob * (1 - 0.05);
        const tasa_menos_4_5 = tasa_base_clp_bob * (1 - 0.045);
        const tasa_menos_4 = tasa_base_clp_bob * (1 - 0.04);

        // 4. Truncar a 5 decimales SIN redondear (BOB requiere más precisión)
        const truncar = (num) => Math.floor(num * 100000) / 100000;

        const response = {
            tasa_bob_p2p: truncar(tasa_bob_p2p),
            tasa_clp_p2p: truncar(tasa_clp_p2p),
            tasa_base_clp_bob: truncar(tasa_base_clp_bob),
            tasas_ajustadas: {
                tasa_menos_5: truncar(tasa_menos_5),
                tasa_menos_4_5: truncar(tasa_menos_4_5),
                tasa_menos_4: truncar(tasa_menos_4)
            },
            metadata: {
                fuente: 'Binance P2P',
                bancos_bob: 'Banco Ganadero, Banco Economico',
                min_bob: 100,
                timestamp: new Date().toISOString()
            }
        };

        console.log('... Tasas P2P BOB/CLP calculadas exitosamente');
        res.json(response);
    } catch (error) {
        console.error(' Error en endpoint /api/p2p/tasas-bob-clp:', error.message);
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
        console.log('"" Consultando tasas P2P ARS/CLP...');

        // 1. Obtener tasas P2P
        const [tasa_ars_p2p, tasa_clp_p2p] = await Promise.all([
            obtenerTasaVentaARS(),
            obtenerTasaCompraCLP()
        ]);

        // 2. Calcular tasa base CLP †' ARS
        const tasa_base_clp_ars = tasa_ars_p2p / tasa_clp_p2p;

        // 3. Calcular tasas ajustadas
        const tasa_menos_5 = tasa_base_clp_ars * (1 - 0.05);
        const tasa_menos_4_5 = tasa_base_clp_ars * (1 - 0.045);
        const tasa_menos_4 = tasa_base_clp_ars * (1 - 0.04);

        // 4. Truncar a 4 decimales SIN redondear
        const truncar = (num) => Math.floor(num * 10000) / 10000;

        const response = {
            tasa_ars_p2p: truncar(tasa_ars_p2p),
            tasa_clp_p2p: truncar(tasa_clp_p2p),
            tasa_base_clp_ars: truncar(tasa_base_clp_ars),
            tasas_ajustadas: {
                tasa_menos_5: truncar(tasa_menos_5),
                tasa_menos_4_5: truncar(tasa_menos_4_5),
                tasa_menos_4: truncar(tasa_menos_4)
            },
            metadata: {
                fuente: 'Binance P2P',
                bancos_ars: 'MercadoPago, Brubank, LemonCash',
                min_ars: 15000,
                timestamp: new Date().toISOString()
            }
        };

        console.log('... Tasas P2P ARS/CLP calculadas exitosamente');
        res.json(response);
    } catch (error) {
        console.error(' Error en endpoint /api/p2p/tasas-ars-clp:', error.message);
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
            
            // Registrar actividad de tarea completada
            const timestamp = new Date().toISOString();
            await dbRun(`
                INSERT INTO actividad_operadores(usuario_id, tipo_actividad, timestamp, metadata)
                VALUES (?, 'tarea', ?, ?)
            `, [req.session.user.id, timestamp, JSON.stringify({
                tarea_id: tareaId
            })]);
        }
        
        res.json({ message: 'Tarea actualizada exitosamente' });
    } catch (error) {
        console.error('Error actualizando tarea:', error);
        res.status(500).json({ message: 'Error al actualizar tarea' });
    }
});

// Resolver tarea automáticamente con el agente
app.post('/api/tareas/:id/resolver', apiAuth, async (req, res) => {
    const tareaId = req.params.id;
    const userId = req.session.user.id;
    
    try {
        // Obtener datos de la tarea
        const tarea = await dbGet(`
            SELECT t.*, a.cliente_id, a.tipo as tipo_alerta, a.dias_inactivo, a.ultima_operacion,
                   c.nombre as cliente_nombre
            FROM tareas t
            LEFT JOIN alertas a ON a.tarea_id = t.id
            LEFT JOIN clientes c ON c.id = a.cliente_id
            WHERE t.id = ?
        `, [tareaId]);
        
        if (!tarea) {
            return res.status(404).json({ message: 'Tarea no encontrada' });
        }
        
        // Verificar si ya fue resuelta
        if (tarea.resolucion_agente === 'automatica' && tarea.mensaje_generado) {
            return res.json({
                success: true,
                ya_resuelta: true,
                resolucion_agente: tarea.resolucion_agente,
                mensaje_generado: tarea.mensaje_generado,
                metadata: tarea.metadata ? JSON.parse(tarea.metadata) : null,
                accion_requerida: tarea.accion_requerida
            });
        }
        
        // Extraer días de inactividad
        const matchDias = tarea.descripcion ? tarea.descripcion.match(/(\d+)\s*d[ií]as?/i) : null;
        const diasInactivo = tarea.dias_inactivo || (matchDias ? parseInt(matchDias[1]) : 0);
        
        // Obtener tasa VES/CLP desde sistema de stock unificado
        const tasaVesPorClpCalc = await getTasaVesPorClp();
        const ultimaCompra = tasaVesPorClpCalc > 0 ? { tasa_clp_ves: tasaVesPorClpCalc, fecha: hoyLocalYYYYMMDD() } : null;

        if (!ultimaCompra || !ultimaCompra.tasa_clp_ves) {
            // Resolución ASISTIDA - Sin historial de compras
            await dbRun(`
                UPDATE tareas
                SET resolucion_agente = 'asistida',
                    accion_requerida = 'registrar_compra_usdt',
                    observaciones = 'No hay historial de compras en el sistema de stock para calcular tasa promocional',
                    estado = 'en_progreso'
                WHERE id = ?
            `, [tareaId]);

            return res.json({
                success: false,
                resolucion_agente: 'asistida',
                problema: 'sin_historial_compras',
                mensaje: 'No se puede resolver automáticamente porque no hay historial de compras en el stock. Registra compras en /stock.html'
            });
        }
        
        // Determinar estrategia según tipo de alerta y días
        let tipoEstrategia = '';
        let descuentoPorcentaje = 0;
        let mensajeGenerado = '';
        
        if (tarea.tipo_alerta === 'critico' || diasInactivo > 60) {
            tipoEstrategia = 'critico_reactivacion';
            descuentoPorcentaje = 2.0;
        } else if (tarea.tipo_alerta === 'disminucion' || (tarea.descripcion && tarea.descripcion.toLowerCase().includes('reducción'))) {
            tipoEstrategia = 'reduccion_actividad';
            descuentoPorcentaje = 3.3;  // Cambiado de 4.0 a 3.3%
        } else if (diasInactivo >= 45) {
            tipoEstrategia = 'inactivo_promocion';
            descuentoPorcentaje = 3.3;
        } else if (diasInactivo >= 30) {
            tipoEstrategia = 'inactivo_recordatorio';
            descuentoPorcentaje = 0;
        }
        
        // Calcular tasa promocional
        const tasaOriginal = ultimaCompra.tasa_clp_ves;
        let tasaPromocional = null;
        
        if (descuentoPorcentaje > 0) {
            const descuento = tasaOriginal * (descuentoPorcentaje / 100);
            tasaPromocional = parseFloat((tasaOriginal - descuento).toFixed(4));
        }
        
        // Generar mensaje con IA (OpenAI) usando helper optimizado
        const nombreCliente = tarea.cliente_nombre || 'Cliente';

        const resultIA = await openaiHelper.generateTaskMessage({
            nombreCliente,
            diasInactivo,
            tasaPromocional,
            tipoEstrategia
        });

        if (resultIA.success) {
            mensajeGenerado = resultIA.message;
            console.log(`   ✅ Mensaje IA generado. Tokens: ${resultIA.usage.inputTokens} in + ${resultIA.usage.outputTokens} out | Costo: $${resultIA.usage.cost.toFixed(6)}`);
        } else {
            mensajeGenerado = resultIA.message; // Ya trae el fallback automatico
            console.warn(`   ⚠️ Usando mensaje fallback. Error: ${resultIA.error}`);
        }

        // Preparar metadata
        const metadata = {
            tasa_original: tasaOriginal,
            tasa_promocional: tasaPromocional,
            descuento_porcentaje: descuentoPorcentaje,
            dias_inactivo: diasInactivo,
            tipo_estrategia: tipoEstrategia,
            fecha_ultima_compra: ultimaCompra.fecha,
            cliente_id: tarea.cliente_id,
            resuelto_por: userId,
            fecha_resolucion: hoyLocalYYYYMMDD()
        };
        
        // Actualizar tarea con resolución automática
        await dbRun(`
            UPDATE tareas
            SET resolucion_agente = 'automatica',
                mensaje_generado = ?,
                accion_requerida = 'enviar_whatsapp',
                metadata = ?,
                estado = 'en_progreso'
            WHERE id = ?
        `, [mensajeGenerado, JSON.stringify(metadata), tareaId]);
        
        res.json({
            success: true,
            resolucion_agente: 'automatica',
            cliente_nombre: nombreCliente,
            mensaje_generado: mensajeGenerado,
            metadata: metadata,
            accion_requerida: 'enviar_whatsapp'
        });
        
    } catch (error) {
        console.error('Error resolviendo tarea:', error);
        res.status(500).json({ message: 'Error al resolver tarea: ' + error.message });
    }
});

// Confirmar envío de mensaje
app.post('/api/tareas/:id/confirmar-envio', apiAuth, async (req, res) => {
    const tareaId = req.params.id;
    const { respuesta_cliente } = req.body;
    const fechaHoy = hoyLocalYYYYMMDD();
    
    try {
        // Obtener tarea y cliente asociado
        const tarea = await dbGet(`
            SELECT t.*, a.cliente_id
            FROM tareas t
            LEFT JOIN alertas a ON a.tarea_id = t.id
            WHERE t.id = ?
        `, [tareaId]);
        
        if (!tarea) {
            return res.status(404).json({ message: 'Tarea no encontrada' });
        }
        
        // Marcar tarea como completada
        await dbRun(`
            UPDATE tareas 
            SET estado = 'completada',
                fecha_completada = ?,
                fecha_mensaje_enviado = ?,
                respuesta_cliente = ?
            WHERE id = ?
        `, [fechaHoy, fechaHoy, respuesta_cliente || null, tareaId]);
        
        // Registrar actividad de mensaje enviado
        const timestamp = new Date().toISOString();
        await dbRun(`
            INSERT INTO actividad_operadores(usuario_id, tipo_actividad, timestamp, metadata)
            VALUES (?, 'mensaje', ?, ?)
        `, [req.session.user.id, timestamp, JSON.stringify({
            tarea_id: tareaId,
            cliente_id: tarea.cliente_id
        })]);
        
        // Actualizar alerta
        if (tarea.cliente_id) {
            await dbRun(`
                UPDATE alertas 
                SET accion_realizada = 'mensaje_enviado',
                    fecha_accion = ?
                WHERE cliente_id = ? AND activa = 1
            `, [fechaHoy, tarea.cliente_id]);
        }
        
        // Crear notificación para el master
        const operadorNombre = req.session.user.username || 'Operador';
        await dbRun(`
            INSERT INTO notificaciones(usuario_id, tipo, titulo, mensaje, fecha_creacion, tarea_id)
            VALUES (1, 'tarea', 'Tarea completada', ?, ?, ?)
        `, [
            `${operadorNombre} completó: ${tarea.titulo}`,
            fechaHoy,
            tareaId
        ]);
        
        res.json({
            success: true,
            message: 'Mensaje enviado confirmado. Tarea completada exitosamente.'
        });
        
    } catch (error) {
        console.error('Error confirmando envío:', error);
        res.status(500).json({ message: 'Error al confirmar envío' });
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

// Borrado masivo de tareas
app.post('/api/tareas/bulk-delete', apiAuth, onlyMaster, async (req, res) => {
    try {
        const { ids, deleteAll, filtroEstado } = req.body;

        if (deleteAll) {
            // Borrar todas (o filtradas por estado)
            let whereClause = '';
            const params = [];
            if (filtroEstado && filtroEstado !== 'todos') {
                whereClause = ' WHERE estado = ?';
                params.push(filtroEstado);
            }

            const countRow = await dbGet(`SELECT COUNT(*) as total FROM tareas${whereClause}`, params);
            const total = countRow ? countRow.total : 0;

            await dbRun(`DELETE FROM notificaciones WHERE tarea_id IN (SELECT id FROM tareas${whereClause})`, params);
            await dbRun(`DELETE FROM tareas${whereClause}`, params);

            console.log(`[TAREAS] Borrado masivo: ${total} tareas eliminadas (filtro: ${filtroEstado || 'todos'})`);
            res.json({ message: `${total} tareas eliminadas.`, total });
        } else if (ids && Array.isArray(ids) && ids.length > 0) {
            // Borrar por IDs específicos
            const placeholders = ids.map(() => '?').join(',');
            await dbRun(`DELETE FROM notificaciones WHERE tarea_id IN (${placeholders})`, ids);
            await dbRun(`DELETE FROM tareas WHERE id IN (${placeholders})`, ids);

            console.log(`[TAREAS] Borrado masivo: ${ids.length} tareas eliminadas por IDs`);
            res.json({ message: `${ids.length} tareas eliminadas.`, total: ids.length });
        } else {
            res.status(400).json({ message: 'Especifica IDs o deleteAll: true' });
        }
    } catch (e) {
        console.error('Error en borrado masivo de tareas:', e.message);
        res.status(500).json({ message: 'Error al eliminar tareas.' });
    }
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
        
        console.log('" === DEBUG: Generando tareas automáticas ===');
        console.log(`"... Fecha hoy: ${fechaHoy}`);
        
        // Primero verificar TODAS las alertas activas
        const todasAlertas = await dbAll(`SELECT id, cliente_id, tipo, accion_realizada, tarea_id FROM alertas WHERE activa = 1`);
        console.log(`" Total alertas activas: ${todasAlertas.length}`);
        
        const conMensaje = todasAlertas.filter(a => a.accion_realizada === 'mensaje_enviado');
        const conPromocion = todasAlertas.filter(a => a.accion_realizada === 'promocion_enviada');
        const sinAccion = todasAlertas.filter(a => !a.accion_realizada || a.accion_realizada === '');
        
        console.log(`... Con mensaje_enviado: ${conMensaje.length}`);
        console.log(`... Con promocion_enviada: ${conPromocion.length}`);
        console.log(` Sin acción (NULL o vacío): ${sinAccion.length}`);
        
        // Obtener alertas activas SIN accion realizada (sin mensaje_enviado ni promocion_enviada)
        // Permitir crear tarea SOLO si: 1) sin tarea vinculada, 2) tarea eliminada, 3) tarea completada/cancelada
        // NO crear si ya existe una tarea pendiente o en_progreso
        const alertasSinResolver = await dbAll(`
            SELECT a.*
            FROM alertas a
            WHERE a.activa = 1
            AND (a.accion_realizada IS NULL OR a.accion_realizada = '')
            AND (
                a.tarea_id IS NULL
                OR NOT EXISTS (SELECT 1 FROM tareas t WHERE t.id = a.tarea_id)
                OR EXISTS (
                    SELECT 1 FROM tareas t
                    WHERE t.id = a.tarea_id
                    AND t.estado IN ('completada', 'cancelada')
                )
            )
        `);
        
        console.log(` Alertas que cumplen condiciones para generar tareas: ${alertasSinResolver.length}`);

        // Verificar estado de tareas vinculadas para debugging
        for (const alerta of alertasSinResolver.slice(0, 5)) {
            if (alerta.tarea_id) {
                const tareaVinculada = await dbGet(`SELECT id, estado, fecha_creacion FROM tareas WHERE id = ?`, [alerta.tarea_id]);
                if (tareaVinculada) {
                    console.log(`   Alerta ${alerta.id} -> Tarea ${tareaVinculada.id} (${tareaVinculada.estado}, ${tareaVinculada.fecha_creacion})`);
                } else {
                    console.log(`   Alerta ${alerta.id} -> Tarea ${alerta.tarea_id} (eliminada)`);
                }
            } else {
                console.log(`   Alerta ${alerta.id} -> Sin tarea vinculada`);
            }
        }

        // Verificar si hay alguna con accion_realizada que no deberia estar
        const errorAccion = alertasSinResolver.filter(a => a.accion_realizada && a.accion_realizada !== '');
        if (errorAccion.length > 0) {
            console.log(` ERROR: ${errorAccion.length} alertas con accion_realizada pasaron el filtro:`);
            errorAccion.slice(0, 5).forEach(a => {
                console.log(`   - ID ${a.id}: accion_realizada="${a.accion_realizada}"`);
            });
        }
        
        if (alertasSinResolver.length === 0) {
            console.log('... No hay alertas pendientes para crear tareas');
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
            
            // Obtener datos del cliente y RECALCULAR días de inactividad en tiempo real
            const cliente = await dbGet(`
                SELECT c.nombre, MAX(o.fecha) as ultima_operacion,
                       CAST(julianday('now') - julianday(
                           CASE 
                               WHEN o.fecha LIKE '__-__-____' THEN substr(o.fecha, 7, 4) || '-' || substr(o.fecha, 4, 2) || '-' || substr(o.fecha, 1, 2)
                               ELSE o.fecha
                           END
                       ) AS INTEGER) as dias_reales
                FROM clientes c
                LEFT JOIN operaciones o ON c.id = o.cliente_id
                WHERE c.id = ?
                GROUP BY c.id
            `, [alerta.cliente_id]);
            
            console.log(`" Cliente ID ${alerta.cliente_id} (${cliente?.nombre}): última op ${cliente?.ultima_operacion}, días reales: ${cliente?.dias_reales}`);
            
            // Si el cliente ya no cumple el criterio de inactividad, saltar
            const diasInactivo = cliente?.dias_reales || 0;
            if (alerta.tipo === 'inactivo' && (diasInactivo < 30 || diasInactivo > 60)) {
                console.log(`⏭️ Saltando alerta ${alerta.id}: cliente ya no cumple criterio inactivo (${diasInactivo} días)`);
                continue;
            }
            if (alerta.tipo === 'critico' && diasInactivo <= 60) {
                console.log(`⏭️ Saltando alerta ${alerta.id}: cliente ya no cumple criterio crítico (${diasInactivo} días)`);
                continue;
            }
            
            // Para reducción de frecuencia, verificar AHORA si realmente hay reducción
            if (alerta.tipo === 'disminucion') {
                const hace30 = new Date(new Date().getTime() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
                const hace60 = new Date(new Date().getTime() - 60 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
                
                const recientes = await dbGet(`SELECT COUNT(*) as cnt FROM operaciones WHERE cliente_id = ? AND fecha >= ?`, [alerta.cliente_id, hace30]);
                const anteriores = await dbGet(`SELECT COUNT(*) as cnt FROM operaciones WHERE cliente_id = ? AND fecha >= ? AND fecha < ?`, [alerta.cliente_id, hace60, hace30]);
                
                // Si ya NO hay reducción significativa, saltar
                if (anteriores.cnt < 3 || recientes.cnt >= anteriores.cnt * 0.5) {
                    console.log(`⏭️ Saltando alerta ${alerta.id}: cliente ya no tiene reducción significativa (${anteriores.cnt}†'${recientes.cnt} ops)`);
                    continue;
                }
            }
            
            // Determinar prioridad según días REALES de inactividad
            let prioridad = 'normal';
            if (diasInactivo > 60) prioridad = 'urgente';
            else if (diasInactivo >= 45) prioridad = 'alta';
            
            // ANTES de crear nueva tarea, cancelar SOLO tareas PENDIENTES antiguas del mismo cliente
            // NO cancelar tareas en_progreso (el operador esta trabajando en ellas)
            const canceladas = await dbRun(`
                UPDATE tareas
                SET estado = 'cancelada',
                    observaciones = 'Tarea obsoleta - reemplazada por nueva tarea automatica'
                WHERE cliente_id = ?
                AND tipo = 'automatica'
                AND estado = 'pendiente'
                AND fecha_creacion < ?
            `, [alerta.cliente_id, fechaHoy]);

            if (canceladas.changes > 0) {
                console.log(`   Canceladas ${canceladas.changes} tareas obsoletas del cliente ${alerta.cliente_id}`);
            }
            
            // Crear tarea con días REALES
            const titulo = `Reactivar cliente: ${cliente ? cliente.nombre : 'Desconocido'}`;
            const descripcion = `${alerta.tipo === 'inactivo' ? 'Cliente inactivo' : alerta.tipo === 'critico' ? 'Cliente crítico' : 'Disminución de frecuencia'} - ${diasInactivo ? `${diasInactivo} días sin actividad` : 'Reducción de operaciones'}. ltima operación: ${cliente?.ultima_operacion || 'N/A'}`;
            
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
                            console.log(`... Notificación #${notif.id} marcada como leída automáticamente (cliente "${nombreCliente}" ya completo)`);
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
        const systemContext = ` PROMPT SISTEMA - ASISTENTE INTERNO DE OPERACIONES Y SUPERVISOR SUAVE (DEFIORACLE.CL)

Eres el Asistente Interno de Operaciones y Supervisor Suave de la empresa de remesas DefiOracle.cl.

'‰ Solo hablas con operadores y usuarios master del sistema.
Nunca conversas directamente con el cliente final.

Tu trabajo es ayudar, supervisar suavemente y mejorar el rendimiento de los operadores.

USUARIO ACTUAL: "${username}" con rol de "${userRole}".

1. INFORMACI'N DE LA EMPRESA

Nombre comercial: DefiOracle.cl
Razón social: DEFI ORACLE SPA
Rubro: Empresa de remesas y cambio de divisas, usando cripto (USDT) como puente.
Ubicación: Santiago de Chile, comuna de Las Condes.
Ámbito: Envía dinero desde Chile (CLP) hacia varios países (principalmente Venezuela, pero también Colombia, Perú, Argentina, República Dominicana, Europa y EE.UU.).

DATOS BANCARIOS OFICIALES (cuenta CLP):
Banco: BancoEstado - Chequera Electrónica
Nombre: DEFI ORACLE SPA
N.º de cuenta: 316-7-032793-3
RUT: 77.354.262-7

Horario de atención: 08:00-21:00 hrs, todos los días.

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
- ‰ 5.000 CLP: ${contextData.tasas_actuales.VES_nivel1} VES por 1 CLP
- ‰ 100.000 CLP: ${contextData.tasas_actuales.VES_nivel2} VES por 1 CLP
- ‰ 250.000 CLP: ${contextData.tasas_actuales.VES_nivel3} VES por 1 CLP

Estas son las tasas que los operadores ofrecen a los clientes finales.

OTROS PAÍSES (COP, PEN, BOB, ARS):
- Usa tasas basadas en Binance P2P ajustadas con margen

PROMOCIONES POR BAJA ACTIVIDAD (solo Venezuela):
- Cuando el sistema genere alerta de cliente inactivo/reducción de envíos (‰45 días)
- Tasa promo = ltima tasa de compra USDT registrada en "Historial de Compras" - 3.3%
- Ejemplo: Si última compra fue a 0.393651 VES/CLP, la tasa promocional es 0.393651 - (0.393651 - 0.033) = 0.3806 VES/CLP
- Genera mensaje personalizado con nombre del cliente y tasa promocional calculada

4. ASISTENTE DE CONVERSACI'N

Cuando el operador te escriba:
- Entiende la intención (conversión, proceso, datos bancarios, tiempos, promo)
- Genera respuesta clara, amigable, semiformal
- Lista para copiar y pegar en WhatsApp

DATOS BANCARIOS - Cuando se pidan, envía SIEMPRE:
"Te dejo los datos de nuestra cuenta en Chile:
Banco: BancoEstado - Chequera Electrónica
Nombre: DEFI ORACLE SPA
N.º de cuenta: 316-7-032793-3
RUT: 77.354.262-7

Después de hacer el pago, que el cliente envíe el comprobante por WhatsApp para procesar su envío ˜‰."

5. SUPERVISI'N DE DATOS DE CLIENTES Y ACCIONES COMO AGENTE

Cliente "completo" = nombre, rut, email, telefono

️ IMPORTANTE: NO guardamos ni solicitamos datos bancarios de clientes. Solo validamos: RUT, email, teléfono.

Si falta información, informa al operador de forma conversacional y sugiere actualizar los datos.

- MODO AGENTE AUT'NOMO CON FUNCTION CALLING:

Tienes acceso REAL a funciones para consultar la base de datos. OpenAI decide AUTOMÁTICAMENTE cuándo llamarlas según el contexto de la pregunta.

FUNCIONES DISPONIBLES (llamadas automáticamente por ti):

1️ƒ **buscar_cliente(nombre)**
   - Cuándo usarla: Cuando pregunten sobre un cliente específico, si actualizaron datos, verificar información, etc.
   - Ejemplos de preguntas:
     * "¿ya actualizaron a Cris?"
     * "datos de María"
     * "tiene el cliente Juan todos los datos?"
     * "verificar si Cris tiene email"
   - Retorna: {encontrado, nombre, rut, email, telefono, datos_completos, faltan: [array con 'RUT', 'Email', 'Teléfono']}
   - Razonamiento: Si preguntan "¿ya actualizaron a Cris?", T decides llamar buscar_cliente("Cris"), recibes los datos actuales, y respondes si están completos o no
   - CRÍTICO: Si el cliente tiene datos_completos=true, NO menciones que faltan datos. Si datos_completos=false, menciona SOLO lo que está en faltan[]

2️ƒ **listar_operaciones_dia(limite)**
   - Cuándo usarla: Cuando pregunten sobre operaciones de hoy, envíos realizados, última operación
   - Ejemplos:
     * "¿cuántas operaciones llevamos hoy?"
     * "muéstrame las últimas operaciones"
     * "qué envíos se hicieron hoy"
   - Retorna: {total, operaciones: [{numero_recibo, cliente, monto_clp, monto_ves, tasa, operador, hora}]}

3️ƒ **consultar_rendimiento()**
   - Cuándo usarla: Cuando el operador pregunte sobre su desempeño, estadísticas, productividad
   - Ejemplos:
     * "cómo voy este mes?"
     * "mi rendimiento"
     * "cuántas operaciones he hecho?"
   - Retorna: {total_operaciones, total_procesado_clp, ganancia_total_clp, ganancia_promedio_clp}

4️ƒ **listar_clientes_incompletos(limite)**
   - Cuándo usarla: Cuando pregunten sobre clientes pendientes de actualizar
   - Ejemplos:
     * "¿qué clientes faltan actualizar?"
     * "clientes incompletos"
     * "quién necesita completar datos?"
   - Retorna: {total, clientes: [{nombre, faltan: [array]}]}

5️ƒ **buscar_operaciones_cliente(nombre_cliente)**
   - Cuándo usarla: Cuando pregunten sobre el historial de un cliente específico
   - Ejemplos:
     * "cuántas operaciones tiene Cris?"
     * "historial de envíos de María"
     * "ha enviado Juan anteriormente?"
   - Retorna: {total, operaciones: [{numero_recibo, monto_clp, monto_ves, fecha}]}

6️ƒ **calcular_conversion_moneda(monto, moneda_origen, moneda_destino)**
   - Cuándo usarla: Cuando pregunten sobre conversiones entre monedas, cuánto transferir, tasas de cambio
   - Ejemplos:
     * "¿cuánto debo transferir en CLP para que lleguen 40.000 COP?"
     * "convertir 100.000 CLP a VES"
     * "cuántos dólares son 500.000 pesos chilenos?"
     * "equivalencia entre pesos chilenos y colombianos"
     * "cuál es la tasa CLP a COP"

7️ƒ **consultar_tareas(incluir_completadas)**
   - Cuándo usarla: Cuando pregunten sobre tareas pendientes, trabajo asignado, qué hacer
   - Ejemplos:
     * "¿tengo tareas pendientes?"
     * "qué tareas tengo hoy?"
     * "mis asignaciones"
     * "qué debo hacer?"
     * "tareas"
   - Retorna: {total, tareas: [{titulo, descripcion, prioridad, estado, fecha_vencimiento, vencida, dias_restantes}]}
   - IMPORTANTE: Si el mensaje proactivo mencionó tareas, SIEMPRE llama esta función

8️ƒ **obtener_estadisticas_clientes()**
   - Cuándo usarla: Cuando pregunten por el total de clientes, estadísticas generales
   - Ejemplos:
     * "¿cuántos clientes tenemos?"
     * "total de clientes registrados"
     * "estadísticas de clientes"
   - Retorna: {total_clientes, clientes_completos, clientes_incompletos, porcentaje_completos}

9️ƒ **analizar_tarea_cliente_inactivo(nombre_cliente, descripcion_tarea)**
   - Cuándo usarla: Cuando el operador pida ayuda con una tarea de cliente inactivo o reducción de actividad
   - Ejemplos:
     * "¿qué hago con esta tarea de [cliente]?"
     * "ayúdame con el cliente inactivo [nombre]"
     * "¿qué mensaje envío a [cliente]?"
     * Operador menciona tarea de: "cliente inactivo por X días", "reducción de actividad", "riesgo alto"
   - Función INTELIGENTE que:
     ... Analiza los días de inactividad
     ... Determina si debe enviar recordatorio (30-44 días) o promoción (45+ días)
     ... Calcula tasa promocional automáticamente (3.3% descuento sobre LTIMA TASA DE COMPRA USDT del historial de compras)
     ... Genera mensaje personalizado listo para copiar y enviar
   - Aplica a: "Cliente inactivo", "Reducción de actividad", "Riesgo alto"
   - IMPORTANTE: La tasa promocional se calcula desde el "Historial de Compras" (última compra de USDT), NO desde Binance P2P
   - Retorna: {tipo_accion, dias_inactivo, tasa_original, tasa_promocional, mensaje_sugerido}

   - Monedas soportadas: CLP (Chile), COP (Colombia), VES (Venezuela), USD (Dólares), ARS (Argentina), PEN (Perú), BRL (Brasil), MXN (México), EUR (Euro), UYU (Uruguay)
   - Retorna: {monto_origen, moneda_origen, nombre_moneda_origen, monto_convertido, moneda_destino, nombre_moneda_destino, tasa_cambio, formula}

   
   " F"RMULAS DE CONVERSI'N (IMPORTANTE):
   
   Para convertir DESDE moneda A HACIA moneda B:
   Monto en B = Monto en A - Tasa(A†'B)
   
   Ejemplos prácticos:
   
   ... "¿Cuántos COP son 100.000 CLP?"
   †' Llamas: calcular_conversion_moneda(100000, "CLP", "COP")
   †' Tasa CLP†'COP = 4 (porque 1 CLP = 4 COP)
   †' Resultado: 100.000 - 4 = 400.000 COP
   
   ... "¿Cuántos CLP necesito transferir para que lleguen 40.000 COP?"
   †' Usuario pregunta: Cuántos CLP †' 40.000 COP (quiere saber el origen)
   †' Llamas: calcular_conversion_moneda(40000, "COP", "CLP")
   †' Tasa COP†'CLP = 0.25 (porque 1 COP = 0.25 CLP)
   †' Resultado: 40.000 - 0.25 = 10.000 CLP
   †' Respondes: "Para que lleguen 40.000 COP, debes transferir 10.000 CLP"
   
   ... "¿Cuántos VES recibe el cliente por 50.000 CLP?"
   †' Llamas: calcular_conversion_moneda(50000, "CLP", "VES")
   †' Resultado basado en tasa actual
   
   ️ IMPORTANTE - INTERPRETACI'N DE PREGUNTAS:
   
   Cuando pregunten "¿cuánto debo transferir para que lleguen X [moneda destino]?":
   - El usuario TIENE moneda destino conocida (X unidades)
   - El usuario NECESITA saber cuánta moneda origen enviar
   - Llamas: calcular_conversion_moneda(X, "moneda_destino", "moneda_origen")
   
   Cuando pregunten "¿cuánto llega si envío X [moneda origen]?":
   - El usuario TIENE moneda origen conocida (X unidades)
   - El usuario NECESITA saber cuánto llega en moneda destino
   - Llamas: calcular_conversion_moneda(X, "moneda_origen", "moneda_destino")

RAZONAMIENTO AUT'NOMO:

... T DECIDES qué función llamar según el contexto de la pregunta
... OpenAI analiza la pregunta y elige la función apropiada automáticamente
... NO necesitas que el usuario use palabras exactas
... Entiendes intención: "¿ya está listo Cris?" †' buscar_cliente("Cris") †' revisar datos_completos

EJEMPLOS DE RAZONAMIENTO:

Pregunta: "¿ya actualizaron a ese cliente Cris?"
†' T razonas: 'Necesito buscar si Cris existe y si sus datos están completos"
†' Llamas: buscar_cliente("Cris")
†' Recibes: {encontrado: true, nombre: "Cris", rut: "12345", email: "cris@mail.com", telefono: "987654", datos_completos: true, faltan: []}
†' Respondes: "Sí, Cris ya está completo .... Tiene RUT, email y teléfono registrados."

Pregunta: "tiene datos el cliente que se llama María?"
†' Llamas: buscar_cliente("María")
†' Respondes según lo que encuentres

Pregunta: "cuánto he trabajado este mes?"
†' Llamas: consultar_rendimiento()
†' Respondes con las estadísticas

IMPORTANTE:

... Llamas funciones AUTOMÁTICAMENTE cuando detectas la necesidad
... NO pidas permiso para consultar - simplemente hazlo
... Presenta los resultados de forma conversacional y amigable
... Si no encuentras datos, dilo claramente: 'No encontré cliente con ese nombre"
... NO inventes información - usa SOLO lo que las funciones retornan

6. GESTI'N DE TAREAS Y RENDIMIENTO

Revisa tareas pendientes o vencidas
Como supervisor suave, pregunta sin regañar
Sugiere actualización de tareas según respuesta del operador

Rendimiento: Usa /api/mi-rendimiento para explicar métricas del mes

7. CONTEXTO CONVERSACIONAL Y NOTIFICACIONES

Mantén el contexto de la conversación. Si el operador te preguntó algo anteriormente, recuérdalo.

"" IMPORTANTE - NOTIFICACIONES PROACTIVAS (OBLIGATORIO):

️ REGLA ABSOLUTA - VERIFICA PRIMERO:
ANTES de responder cualquier cosa, REVISA si contextData.notificaciones_pendientes tiene contenido.

Si contextData.notificaciones_pendientes existe y NO está vacío:
-  DEBES mencionarlas INMEDIATAMENTE en tu respuesta
-  NO respondas nada más sin mencionarlas primero
- ... Menciónalas ANTES de responder cualquier otra cosa

CUÁNDO MENCIONAR NOTIFICACIONES:
- ... SIEMPRE que contextData.notificaciones_pendientes tenga datos
- ... Especialmente cuando el usuario te salude ("hola", "buenos días", "qué hay", etc.)
- ... Cuando pregunten "tengo notificaciones?", "qué hay pendiente", "tareas", "alertas"
-  NUNCA digas 'No hay notificaciones" si contextData.notificaciones_pendientes tiene elementos

EJEMPLO VERIFICACI'N:
Usuario: "hola"
T piensas: ¿Hay algo en contextData.notificaciones_pendientes?
- SI HAY: Mencionar PRIMERO las notificaciones
- NO HAY: Saludo normal

C"MO MENCIONAR NOTIFICACIONES:
- Ejemplo BUENO: "¡Hola! '‹ Mira, hay un tema: el cliente Craus hizo un envío pero le faltan RUT, email y teléfono. ¿Lo revisamos?"
- Ejemplo MALO: 'Notificación #1: Cliente Craus tiene datos incompletos..."
- Si hay varias (2-3), menciónalas: "Hay un par de cosas: 1) Craus necesita datos, 2) María también..."
- Si hay muchas (>3): "Tienes 5 notificaciones. Las más importantes: Craus y María necesitan actualizar datos"

FORMATO DE RESPUESTA CON NOTIFICACIONES:
1. Saludo breve
2. ⭐ MENCIONA LAS NOTIFICACIONES (palabra clave: "pendiente", "falta", "incompleto", etc.)
3. Pregunta si quiere más detalles

Ejemplo completo cuando preguntan "tengo notificaciones?":
"Sí! Tienes 1 notificación pendiente: el cliente Craus hizo una operación pero le faltan datos (RUT, email, teléfono). ¿Quieres que busque más info?"

 NUNCA digas 'No hay notificaciones" si contextData.notificaciones_pendientes tiene contenido

CRÍTICO - SOBRE CONSULTAS DE DATOS ESPECÍFICOS:
- SI te piden datos de un cliente específico, revisa si hay información en contextData.cliente_consultado
- Si contextData.cliente_consultado existe, muestra esos datos de forma conversacional y clara
- Si NO existe cliente_consultado pero te piden datos, sugiere verificar el nombre del cliente
- NUNCA inventes datos como RUT, email, teléfono
- Solo usa la información real que viene en contextData

CUANDO MUESTRES DATOS DE UN CLIENTE:
- Formato conversacional, NO listados robóticos
- Ejemplo BUENO: "Cris está registrado desde [fecha]. Tiene RUT: xxx, email: xxx, teléfono: xxx. Todo completo ..."
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

️ IMPORTANTE - LEE SIEMPRE ESTOS CONTEXTOS PRIMERO:

"‹ **1. MENSAJES PROACTIVOS** (contextData.mensajes_proactivos):
- Estos mensajes contienen información ESPECÍFICA ya detectada por el sistema
- Nombres exactos de clientes, detalles precisos de alertas
- Cuando el usuario responda a un mensaje proactivo, USA LA INFORMACI'N DEL MENSAJE
- NO llames funciones genéricas si el mensaje proactivo ya tiene los detalles
- Ejemplo: Si dice "Cristia Jose, Craus y 1 más", menciona ESOS nombres exactos

"" **2. NOTIFICACIONES PENDIENTES** (contextData.notificaciones_pendientes):
- Alertas del sistema de notificaciones normales
- Menciónalas cuando existan, especialmente al saludar
- Palabra clave para mencionar: "pendiente", "falta", "incompleto"

... **3. TAREAS PENDIENTES** (contextData.tareas_pendientes):
- Lista de tareas asignadas al usuario
- Total disponible en: contextData.total_tareas_pendientes
- Cuando pregunten por tareas, VERIFICA PRIMERO si ya están en el contexto
- Si contextData.tareas_pendientes tiene datos, úsalos directamente
- Solo llama a consultar_tareas() si necesitas actualizar o filtrar

 **AYUDA CON TAREAS - FUNCI'N INTELIGENTE**:

Cuando el operador tenga tareas y pida ayuda:
- **Detecta tipo de tarea**: "Cliente inactivo por X días", "Reducción de actividad", etc.
- **Ofrece ayuda automáticamente**: "¿Quieres que te ayude a resolver esta tarea?"
- **Usa analizar_tarea_cliente_inactivo()** para generar mensajes automáticos

Ejemplo de flujo:
Operador: "Tengo una tarea de andrez hernandez, cliente inactivo por 71 días"
Tú: "¡Claro! Voy a analizar esta tarea y generar un mensaje para andrez..."
†' Llamas: analizar_tarea_cliente_inactivo("andrez hernandez", "Cliente inactivo por 71 días")
†' Recibes: tasa promocional calculada + mensaje listo
†' Respondes: "Aquí está el mensaje para andrez: [mensaje generado]. La tasa promocional es [X] VES. ¿Lo envío?"

Tipos de tareas que puedes resolver:
1. **Cliente inactivo 30-44 días**: Mensaje de recordatorio/cercanía (sin promoción)
2. **Cliente inactivo 45+ días**: Mensaje con promoción (tasa + 3.3% descuento)
3. **Reducción de actividad**: Mensaje con promoción (tasa + 3.3% descuento)

" **PRIORIDAD DE LECTURA**:
1. PRIMERO: Lee mensajes_proactivos (información más específica)
2. SEGUNDO: Lee notificaciones_pendientes
3. TERCERO: Lee tareas_pendientes
4. LTIMO: Llama funciones solo si necesitas datos adicionales

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
                const notifIds = contextData.notificaciones_pendientes.map(n => parseInt(n.id)).filter(id => !isNaN(id));
                const placeholders = notifIds.map(() => '?').join(',');
                db.run(
                    `UPDATE notificaciones SET leida = 1 WHERE id IN (${placeholders})`,
                    notifIds,
                    (err) => {
                        if (!err) {
                            console.log(`... ${notifIds.length} notificación(es) marcada(s) como leída(s) (chatbot las mencionó en su respuesta)`);
                        }
                    }
                );
            } else {
                console.log(`"️ Notificaciones NO marcadas como leídas - el chatbot no las mencionó en esta respuesta`);
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
            VES_descripcion: "Tasas de VENTA a clientes (‰5K, ‰100K, ‰250K CLP). Estas son las que ofrecemos.",
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

// ' TASAS DE CAMBIO P2P (Base: CLP)
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
    
    // Convertir: Origen †' CLP †' Destino
    return tasaOrigenACLP / tasaDestinoACLP;
}

// - FUNCIONES DISPONIBLES PARA EL AGENTE (Function Calling)
const agentFunctions = [
    {
        name: "calcular_conversion_moneda",
        description: "Calcula conversiones entre monedas del P2P. salo cuando pregunten: '¿cuánto debo transferir para que lleguen X pesos colombianos?', 'convertir X a otra moneda', 'cuál es la tasa', 'equivalencia entre monedas', etc. Monedas soportadas: CLP (Chile), COP (Colombia), VES (Venezuela), USD, ARS (Argentina), PEN (Perú), BRL (Brasil), MXN (México), EUR, UYU (Uruguay).",
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
                    description: "Numero maximo de operaciones a listar (por defecto 10)"
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
                    description: "Numero maximo de clientes a listar (por defecto 10)"
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
        description: "Consulta las tareas asignadas al operador. SALO SIEMPRE cuando pregunten: '¿tengo tareas?', 'mis tareas pendientes', 'qué debo hacer hoy', 'tareas', 'pendientes', 'asignaciones', 'trabajo pendiente', etc. Esta función muestra tareas activas, su prioridad, estado y fecha de vencimiento.",
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
        description: "Analiza una tarea de cliente inactivo y genera una sugerencia de mensaje personalizado. sala cuando el operador pida ayuda con una tarea de: 'cliente inactivo', 'reducción de actividad', 'riesgo alto', o cuando pregunten '¿qué hago con esta tarea?', 'ayúdame con este cliente', '¿qué mensaje envío?'",
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
        name: "resolver_tarea",
        description: "Resuelve automáticamente una tarea generando mensaje, calculando promoción y preparando todo para que el operador solo confirme el envío. sala cuando: 1) Se crea una tarea nueva automática, 2) El operador pregunta sobre una tarea asignada, 3) Necesitas preparar el mensaje de forma proactiva. Esta función analiza la tarea, obtiene datos del cliente, calcula tasa promocional y genera mensaje listo para copiar.",
        parameters: {
            type: "object",
            properties: {
                tarea_id: {
                    type: "number",
                    description: "ID de la tarea a resolver"
                },
                confirmar_envio: {
                    type: "boolean",
                    description: "true si el operador confirma que envió el mensaje al cliente (marca tarea completada)"
                }
            },
            required: ["tarea_id"]
        }
    }
];

// Función para generar respuestas del chatbot con Function Calling
async function generateChatbotResponse(userMessage, systemContext, userRole, username, contextData, historial = [], userId = null) {
    const messageLower = userMessage.toLowerCase();
    
    // SOLO respuestas ultra-rápidas de datos bancarios (se usan mucho)
    if (messageLower === 'datos bancarios' || messageLower === 'cuenta bancaria' || messageLower === 'datos banco') {
        return ` **Datos Bancarios DefiOracle.cl:**\n\nBanco: BancoEstado - Chequera Electrónica\nNombre: DEFI ORACLE SPA\nCuenta: 316-7-032793-3\nRUT: 77.354.262-7\n\n... Listo para copiar y pegar.`;
    }
    
    // Para todo lo demás, usar OpenAI con Function Calling
    try {
        // Usar variable de entorno OPENAI_API_KEY, o fallback a la key hardcodeada
        const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
        
        // Validar que hay API key
        if (!OPENAI_API_KEY || OPENAI_API_KEY === '' || OPENAI_API_KEY.includes('your-api-key-here')) {
            console.error(' No se encontró API key de OpenAI válida');
            return ' Lo siento, el chatbot no está configurado correctamente. Por favor contacta al administrador para configurar la API key de OpenAI.';
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
        
        // Primera llamada a OpenAI con function calling usando helper optimizado
        const resultChatbot = await openaiHelper.chatbotWithFunctions(messages, agentFunctions);

        if (!resultChatbot.success) {
            console.error(' Error chatbot OpenAI:', resultChatbot.error);

            // Error especifico de API key
            if (resultChatbot.errorCode === 'invalid_api_key') {
                return ` **Configuración pendiente**\n\nLo siento, la API key de OpenAI no está configurada correctamente.\n\n**Administrador:** Configure la variable de entorno \`OPENAI_API_KEY\` en Render con una key válida de https://platform.openai.com/api-keys`;
            }

            // Respuesta generica humanizada
            return `Entiendo tu consulta, ${username}. Como asistente de DefiOracle.cl puedo ayudarte con conversiones, datos bancarios, tareas, y más. ¿Podrías darme más detalles de lo que necesitas?\n\n_Nota: El servicio de IA está experimentando problemas técnicos._`;
        }

        let responseMessage = resultChatbot.message;
        
        // Si OpenAI decidió llamar una función
        if (responseMessage.function_call) {
            const functionName = responseMessage.function_call.name;
            const functionArgs = JSON.parse(responseMessage.function_call.arguments);
            
            console.log(`- Agente llamando función: ${functionName} con args:`, functionArgs);
            
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
                                mensaje: ` La moneda "${moneda_origen}" no está soportada. Monedas disponibles: CLP, COP, VES, USD, ARS, PEN, BRL, MXN, EUR, UYU` 
                            });
                            return;
                        }
                        
                        if (!TASAS_CAMBIO_P2P[monedaDestinoUpper]) {
                            resolve({ 
                                error: true, 
                                mensaje: ` La moneda "${moneda_destino}" no está soportada. Monedas disponibles: CLP, COP, VES, USD, ARS, PEN, BRL, MXN, EUR, UYU` 
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
                            formula: `${monto} ${monedaOrigenUpper} - ${Math.round(tasa * 10000) / 10000} = ${Math.round(montoConvertido * 100) / 100} ${monedaDestinoUpper}`
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
                                    resolve({ total: 0, mensaje: 'No hay operaciones registradas hoy' });
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
                                    resolve({ total_operaciones: 0, mensaje: 'No hay operaciones este mes' });
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
                                    resolve({ total: 0, mensaje: 'No tienes tareas pendientes asignadas' });
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
                        
                        // Obtener tasa VES/CLP desde sistema de stock unificado
                        getTasaVesPorClp().then(tasaVesPorClpMsg => {
                                let tasaOriginal = null;
                                let tasaPromocional = null;
                                let mensajeSugerido = '';
                                let fechaCompra = null;

                                // Calcular tasa promocional si hay datos en el stock
                                if (tasaVesPorClpMsg > 0) {
                                    tasaOriginal = tasaVesPorClpMsg;
                                    fechaCompra = hoyLocalYYYYMMDD();
                                    // Aplicar 3.3% de DESCUENTO sobre la tasa de compra
                                    const descuento = tasaOriginal * 0.033;
                                    tasaPromocional = parseFloat((tasaOriginal - descuento).toFixed(4));
                                }

                                // Generar mensaje según tipo de acción
                                if (tipoAccion === 'inactivo_recordatorio') {
                                    mensajeSugerido = `Hola ${nombreCliente}! '‹\n\nHemos notado que hace ${diasInactivo} días no realizas una operación con nosotros. ˜\n\nTe esperamos pronto, siempre estamos atentos a tus operaciones. ¡Gracias por ser un cliente constante de DefiOracle! ‡‡‡‡`;

                                } else if (requierePromocion && tasaPromocional) {
                                    if (tipoAccion === 'reduccion_actividad') {
                                        mensajeSugerido = `Hola ${nombreCliente}! '‹\n\nHemos notado que últimamente has reducido tu actividad con nosotros. ˜\n\nNo queremos que te vayas, así que tenemos una tasa especial solo para ti: ${tasaPromocional.toFixed(3)} VES por cada CLP '\n\n¡Aprovecha esta oferta! Estamos disponibles 08:00-21:00 todos los días. ‡‡‡‡`;
                                    } else {
                                        mensajeSugerido = `Hola ${nombreCliente}! '‹\n\nTe extrañamos! Hace tiempo que no haces una operación con nosotros. ˜\n\nPorque nos importa tu regreso, tenemos una tasa de regalo especial para ti: ${tasaPromocional.toFixed(3)} VES por cada CLP '\n\n¡Esperamos verte pronto! Disponibles 08:00-21:00 todos los días. ‡‡‡‡`;
                                    }
                                } else if (requierePromocion && !tasaPromocional) {
                                    mensajeSugerido = `No se pudo calcular la tasa promocional porque no hay datos en el stock.\n\nSugerencia: Registra compras de USDT y cambios USDT→VES en /stock.html para calcular tasas automaticamente.`;
                                }
                                
                                resolve({
                                    cliente: nombreCliente,
                                    tipo_accion: tipoAccion,
                                    dias_inactivo: diasInactivo,
                                    requiere_promocion: requierePromocion,
                                    tasa_original: tasaOriginal ? tasaOriginal.toFixed(4) : null,
                                    tasa_promocional: tasaPromocional ? tasaPromocional.toFixed(4) : null,
                                    descuento_aplicado: requierePromocion ? '+3.3%' : 'No aplica',
                                    fecha_ultima_compra: fechaCompra,
                                    mensaje_sugerido: mensajeSugerido
                                });
                        });
                    });
                    break;

                case 'resolver_tarea':
                    functionResult = await new Promise(async (resolve) => {
                        const tareaId = functionArgs.tarea_id;
                        const confirmarEnvio = functionArgs.confirmar_envio || false;
                        
                        try {
                            // 1. Obtener datos de la tarea
                            const tarea = await dbGet(`
                                SELECT t.*, a.cliente_id, a.tipo as tipo_alerta, a.dias_inactivo, a.ultima_operacion,
                                       c.nombre as cliente_nombre
                                FROM tareas t
                                LEFT JOIN alertas a ON a.tarea_id = t.id
                                LEFT JOIN clientes c ON c.id = a.cliente_id
                                WHERE t.id = ?
                            `, [tareaId]);
                            
                            if (!tarea) {
                                resolve({ error: true, mensaje: 'Tarea no encontrada' });
                                return;
                            }
                            
                            // Si solo está confirmando envío
                            if (confirmarEnvio) {
                                const fechaHoy = hoyLocalYYYYMMDD();
                                
                                // Marcar tarea como completada
                                await dbRun(`
                                    UPDATE tareas 
                                    SET estado = 'completada',
                                        fecha_completada = ?,
                                        fecha_mensaje_enviado = ?
                                    WHERE id = ?
                                `, [fechaHoy, fechaHoy, tareaId]);
                                
                                // Actualizar alerta
                                if (tarea.cliente_id) {
                                    await dbRun(`
                                        UPDATE alertas 
                                        SET accion_realizada = 'mensaje_enviado',
                                            fecha_accion = ?
                                        WHERE cliente_id = ? AND activa = 1
                                    `, [fechaHoy, tarea.cliente_id]);
                                }
                                
                                resolve({
                                    success: true,
                                    mensaje: `... Tarea completada exitosamente. Mensaje enviado a ${tarea.cliente_nombre}.`
                                });
                                return;
                            }
                            
                            // 2. Verificar si ya fue resuelta
                            if (tarea.resolucion_agente === 'automatica' && tarea.mensaje_generado) {
                                resolve({
                                    success: true,
                                    ya_resuelta: true,
                                    resolucion_agente: tarea.resolucion_agente,
                                    mensaje_generado: tarea.mensaje_generado,
                                    metadata: tarea.metadata ? JSON.parse(tarea.metadata) : null,
                                    accion_requerida: tarea.accion_requerida,
                                    mensaje: `Esta tarea ya fue resuelta automáticamente. El mensaje está listo para copiar y enviar.`
                                });
                                return;
                            }
                            
                            // 3. Extraer días de inactividad
                            const matchDias = tarea.descripcion ? tarea.descripcion.match(/(\d+)\s*d[ií]as?/i) : null;
                            const diasInactivo = tarea.dias_inactivo || (matchDias ? parseInt(matchDias[1]) : 0);
                            
                            // 4. Obtener tasa VES/CLP desde sistema de stock unificado
                            const tasaVesPorClpCalc2 = await getTasaVesPorClp();
                            const ultimaCompra = tasaVesPorClpCalc2 > 0 ? { tasa_clp_ves: tasaVesPorClpCalc2, fecha: hoyLocalYYYYMMDD() } : null;

                            if (!ultimaCompra || !ultimaCompra.tasa_clp_ves) {
                                // Resolución ASISTIDA - Sin historial de compras
                                await dbRun(`
                                    UPDATE tareas
                                    SET resolucion_agente = 'asistida',
                                        accion_requerida = 'registrar_compra_usdt',
                                        observaciones = 'No hay datos en el sistema de stock para calcular tasa promocional',
                                        estado = 'en_progreso'
                                    WHERE id = ?
                                `, [tareaId]);

                                resolve({
                                    success: false,
                                    resolucion_agente: 'asistida',
                                    problema: 'sin_historial_compras',
                                    mensaje: `No se puede resolver automaticamente porque no hay datos en el stock.\n\n**Accion requerida:** Registra compras de USDT y cambios USDT→VES en /stock.html para poder calcular tasas promocionales.`
                                });
                                return;
                            }
                            
                            // 5. Determinar estrategia según tipo de alerta y días
                            let tipoEstrategia = '';
                            let descuentoPorcentaje = 0;
                            let mensajeGenerado = '';
                            
                            if (tarea.tipo_alerta === 'critico' || diasInactivo > 60) {
                                // Cliente CRÍTICO: 2% descuento
                                tipoEstrategia = 'critico_reactivacion';
                                descuentoPorcentaje = 2.0;
                                
                            } else if (tarea.tipo_alerta === 'disminucion' || tarea.descripcion.toLowerCase().includes('reducción')) {
                                // Reducción de actividad: 3.3% descuento
                                tipoEstrategia = 'reduccion_actividad';
                                descuentoPorcentaje = 3.3;
                                
                            } else if (diasInactivo >= 45) {
                                // Inactivo 45-60 días: 3.3% descuento
                                tipoEstrategia = 'inactivo_promocion';
                                descuentoPorcentaje = 3.3;
                                
                            } else if (diasInactivo >= 30) {
                                // Inactivo 30-44 días: Solo recordatorio (SIN promoción)
                                tipoEstrategia = 'inactivo_recordatorio';
                                descuentoPorcentaje = 0;
                            }
                            
                            // 6. Calcular tasa promocional
                            const tasaOriginal = ultimaCompra.tasa_clp_ves;
                            let tasaPromocional = null;
                            
                            if (descuentoPorcentaje > 0) {
                                const descuento = tasaOriginal * (descuentoPorcentaje / 100);
                                tasaPromocional = parseFloat((tasaOriginal - descuento).toFixed(4));
                            }
                            
                            // 7. Generar mensaje con OpenAI usando helper optimizado
                            const nombreCliente = tarea.cliente_nombre || 'Cliente';

                            const resultIA = await openaiHelper.generateTaskMessage({
                                nombreCliente,
                                diasInactivo,
                                tasaPromocional,
                                tipoEstrategia
                            });

                            if (resultIA.success) {
                                mensajeGenerado = resultIA.message;
                                console.log(`   ✅ Mensaje IA (chatbot) generado. Tokens: ${resultIA.usage.inputTokens} in + ${resultIA.usage.outputTokens} out | Costo: $${resultIA.usage.cost.toFixed(6)}`);
                            } else {
                                mensajeGenerado = resultIA.message; // Fallback automatico
                                console.warn(`   ⚠️ Usando mensaje fallback (chatbot). Error: ${resultIA.error}`);
                            }
                            
                            // 8. Preparar metadata
                            const metadata = {
                                tasa_original: tasaOriginal,
                                tasa_promocional: tasaPromocional,
                                descuento_porcentaje: descuentoPorcentaje,
                                dias_inactivo: diasInactivo,
                                tipo_estrategia: tipoEstrategia,
                                fecha_ultima_compra: ultimaCompra.fecha,
                                cliente_id: tarea.cliente_id
                            };
                            
                            // 9. Actualizar tarea con resolución automática
                            await dbRun(`
                                UPDATE tareas
                                SET resolucion_agente = 'automatica',
                                    mensaje_generado = ?,
                                    accion_requerida = 'enviar_whatsapp',
                                    metadata = ?,
                                    estado = 'en_progreso'
                                WHERE id = ?
                            `, [mensajeGenerado, JSON.stringify(metadata), tareaId]);
                            
                            resolve({
                                success: true,
                                resolucion_agente: 'automatica',
                                cliente_nombre: nombreCliente,
                                mensaje_generado: mensajeGenerado,
                                metadata: metadata,
                                accion_requerida: 'enviar_whatsapp',
                                mensaje: `... Tarea resuelta automáticamente para ${nombreCliente}.\n\n"‹ Mensaje listo para copiar y enviar por WhatsApp.`
                            });
                            
                        } catch (error) {
                            console.error('Error resolviendo tarea:', error);
                            resolve({
                                error: true,
                                mensaje: 'Error al resolver la tarea: ' + error.message
                            });
                        }
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
            
            // Segunda llamada a OpenAI para que genere respuesta final con los datos (sin functions)
            const resultFinal = await openaiHelper.callOpenAI({
                model: 'gpt-4o-mini',
                messages: messages,
                maxTokens: 500,
                temperature: 0.8,
                useCache: false
            });

            return resultFinal.message.content;
        }
        
        // Si no llamó ninguna función, retornar respuesta directa
        return responseMessage.content;
        
    } catch (error) {
        console.error(' Error API OpenAI:', error.response?.data || error.message);
        
        // Si el error es de API key inválida, dar mensaje específico
        if (error.response?.data?.error?.code === 'invalid_api_key') {
            return ` **Configuración pendiente**\n\nLo siento, la API key de OpenAI no está configurada correctamente.\n\n**Administrador:** Configure la variable de entorno \`OPENAI_API_KEY\` en Render con una key válida de https://platform.openai.com/api-keys`;
        }
        
        // Si falla OpenAI por otro motivo, respuesta genérica humanizada
        return `Entiendo tu consulta, ${username}. Como asistente de DefiOracle.cl puedo ayudarte con conversiones, datos bancarios, tareas, y más. ¿Podrías darme más detalles de lo que necesitas?\n\n_Nota: El servicio de IA está experimentando problemas técnicos._`;
    }
}

// =====================================================
// ENDPOINT: Estadisticas de uso de OpenAI
// =====================================================
app.get('/api/openai/stats', apiAuth, onlyMaster, (req, res) => {
    try {
        const stats = openaiHelper.getStats();
        res.json({
            success: true,
            stats: stats,
            mensaje: 'Estadisticas de uso de OpenAI obtenidas exitosamente'
        });
    } catch (error) {
        console.error('Error obteniendo stats de OpenAI:', error);
        res.status(500).json({
            error: true,
            mensaje: 'Error al obtener estadisticas'
        });
    }
});

// Endpoint para resetear estadisticas de OpenAI (solo master)
app.post('/api/openai/stats/reset', apiAuth, onlyMaster, (req, res) => {
    try {
        openaiHelper.resetStats();
        res.json({
            success: true,
            mensaje: 'Estadisticas reseteadas exitosamente'
        });
    } catch (error) {
        console.error('Error reseteando stats:', error);
        res.status(500).json({
            error: true,
            mensaje: 'Error al resetear estadisticas'
        });
    }
});

// Endpoint para limpiar cache de OpenAI (solo master)
app.post('/api/openai/cache/clear', apiAuth, onlyMaster, (req, res) => {
    try {
        openaiHelper.clearCache();
        res.json({
            success: true,
            mensaje: 'Cache limpiado exitosamente'
        });
    } catch (error) {
        console.error('Error limpiando cache:', error);
        res.status(500).json({
            error: true,
            mensaje: 'Error al limpiar cache'
        });
    }
});

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
                            mensaje: `' Operación #${op.numero_recibo || op.id} - ${op.cliente_nombre} - ${op.monto_clp} CLP (${op.operador})`,
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
                            mensaje: `"" ${not.titulo} - ${not.username} - ${not.leida ? 'Leída' : 'No leída'}`,
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
                            mensaje: `️ ${alerta.tipo} - ${alerta.cliente_nombre} - Severidad: ${alerta.severidad}`,
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
                            mensaje: `"‹ Cliente "${cliente.nombre}" - Faltan: ${faltantes.join(', ')}`,
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
// - SISTEMA DE MONITOREO PROACTIVO DEL CHATBOT
// =================================================================

async function generarMensajesProactivos() {
    console.log('" Ejecutando monitoreo proactivo...');
    
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
            // Obtener fecha actual en zona horaria de Venezuela (UTC-4)
            const ahora = new Date();
            const fechaVenezuela = new Date(ahora.toLocaleString('en-US', { timeZone: 'America/Caracas' }));
            const hoyStr = fechaVenezuela.toISOString().split('T')[0]; // YYYY-MM-DD en hora local de Venezuela
            
            console.log(`⏰ Verificando mensajes para ${usuario.username} - Fecha local Venezuela: ${hoyStr}`);

            // 1️ƒ CELEBRACI'N - Operaciones del día
            const operacionesHoy = await new Promise((resolve) => {
                db.all(`
                    SELECT COUNT(*) as total, SUM(monto_clp) as volumen
                    FROM operaciones
                    WHERE usuario_id = ? AND DATE(fecha) = ?
                `, [usuario.id, hoyStr], (err, rows) => {
                    if (err || !rows || !rows[0]) return resolve(null);
                    resolve(rows[0]);
                });
            });
            console.log(`" ${usuario.username} - Operaciones hoy (${hoyStr}):`, operacionesHoy);

            if (operacionesHoy && operacionesHoy.total >= 5) {
                mensajesGenerados.push({
                    tipo: 'celebracion',
                    mensaje: `‰ ¡Vas genial hoy! Ya llevas ${operacionesHoy.total} operaciones y has procesado $${Math.round(operacionesHoy.volumen).toLocaleString()} CLP. ¡Sigue así!`,
                    prioridad: 'normal',
                    contexto: JSON.stringify({ operaciones: operacionesHoy.total, volumen: operacionesHoy.volumen })
                });
                console.log(`... Agregado mensaje: celebracion`);
            }

            // 2️ƒ RECORDATORIO - Tareas pendientes urgentes
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

            // 3️ƒ ALERTA - Clientes con datos incompletos que operaron recientemente
            const hace7Dias = new Date(fechaVenezuela);
            hace7Dias.setDate(hace7Dias.getDate() - 7);
            const fecha7DiasStr = hace7Dias.toISOString().split('T')[0];
            
            const clientesIncompletos = await new Promise((resolve) => {
                db.all(`
                    SELECT DISTINCT c.nombre, c.id
                    FROM clientes c
                    JOIN operaciones o ON c.id = o.cliente_id
                    WHERE o.usuario_id = ?
                    AND DATE(o.fecha) >= ?
                    AND (c.rut IS NULL OR c.rut = '' OR c.email IS NULL OR c.email = '' OR c.telefono IS NULL OR c.telefono = '')
                    LIMIT 3
                `, [usuario.id, fecha7DiasStr], (err, rows) => {
                    if (err || !rows) return resolve([]);
                    resolve(rows);
                });
            });

            if (clientesIncompletos.length > 0) {
                const nombres = clientesIncompletos.map(c => c.nombre).slice(0, 2).join(', ');
                const resto = clientesIncompletos.length > 2 ? ` y ${clientesIncompletos.length - 2} más` : '';
                mensajesGenerados.push({
                    tipo: 'alerta',
                    mensaje: `️ Ojo: ${nombres}${resto} operaron esta semana pero les faltan datos. ¿Los actualizamos?`,
                    prioridad: 'normal',
                    contexto: JSON.stringify({ clientes: clientesIncompletos.map(c => c.nombre) })
                });
            }

            // 4️ƒ SUGERENCIA - Clientes con datos completos que operaron recientemente
            const clientesCompletosRecientes = await new Promise((resolve) => {
                db.all(`
                    SELECT DISTINCT c.nombre, c.id
                    FROM clientes c
                    JOIN operaciones o ON c.id = o.cliente_id
                    WHERE o.usuario_id = ?
                    AND DATE(o.fecha) >= ?
                    AND c.rut IS NOT NULL AND c.rut != ''
                    AND c.email IS NOT NULL AND c.email != ''
                    AND c.telefono IS NOT NULL AND c.telefono != ''
                    LIMIT 3
                `, [usuario.id, fecha7DiasStr], (err, rows) => {
                    if (err || !rows) return resolve([]);
                    resolve(rows);
                });
            });
            console.log(`... ${usuario.username} - Clientes completos recientes:`, clientesCompletosRecientes.length);

            if (clientesCompletosRecientes.length > 0) {
                mensajesGenerados.push({
                    tipo: 'sugerencia',
                    mensaje: `... ¡Genial! ${clientesCompletosRecientes[0].nombre} ya tiene todos los datos completos. Un cliente menos en pendientes `,
                    prioridad: 'baja',
                    contexto: JSON.stringify({ cliente: clientesCompletosRecientes[0].nombre })
                });
                console.log(`... Agregado mensaje: sugerencia`);
            }

            // 5️ƒ INFORMATIVO - Rendimiento semanal
            const esLunes = fechaVenezuela.getDay() === 1; // 0 = Domingo, 1 = Lunes
            if (esLunes && fechaVenezuela.getHours() >= 9 && fechaVenezuela.getHours() <= 10) {
                const rendimientoSemanal = await new Promise((resolve) => {
                    db.get(`
                        SELECT COUNT(*) as ops, SUM(monto_clp) as volumen
                        FROM operaciones
                        WHERE usuario_id = ?
                        AND DATE(fecha) >= ?
                    `, [usuario.id, fecha7DiasStr], (err, row) => {
                        if (err || !row) return resolve(null);
                        resolve(row);
                    });
                });

                if (rendimientoSemanal && rendimientoSemanal.ops > 0) {
                    mensajesGenerados.push({
                        tipo: 'informativo',
                        mensaje: `" Resumen semanal: ${rendimientoSemanal.ops} operaciones, volumen de $${Math.round(rendimientoSemanal.volumen).toLocaleString()} CLP. ¡Buen trabajo!`,
                        prioridad: 'baja',
                        contexto: JSON.stringify({ ops: rendimientoSemanal.ops, volumen: rendimientoSemanal.volumen })
                    });
                }
            }

            // Guardar mensajes generados en la base de datos
            console.log(`"‹ Usuario ${usuario.username}: ${mensajesGenerados.length} mensajes candidatos`);
            for (const msg of mensajesGenerados) {
                // Verificar que no exista un mensaje similar reciente (últimas 6 horas)
                // IMPORTANTE: Incluir mensajes mostrados para evitar repetir el mismo mensaje
                const mensajeDuplicado = await new Promise((resolve) => {
                    db.get(`
                        SELECT id, mostrado FROM chatbot_mensajes_proactivos
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
                        `, [usuario.id, msg.tipo, msg.mensaje, msg.contexto, msg.prioridad, fechaVenezuela.toISOString()],
                        (err) => {
                            if (!err) {
                                console.log(`' Mensaje proactivo generado para ${usuario.username}: ${msg.tipo}`);
                            } else {
                                console.error(` Error guardando mensaje ${msg.tipo}:`, err.message);
                            }
                            resolve();
                        });
                    });
                } else {
                    console.log(`⏭️ Mensaje tipo "${msg.tipo}" ya existe (ID ${mensajeDuplicado.id}, mostrado: ${mensajeDuplicado.mostrado ? 'sí' : 'no'}), omitiendo...`);
                }
            }
        }
    } catch (error) {
        console.error(' Error en monitoreo proactivo:', error);
    }
}

// Función para limpiar mensajes antiguos (más de 24 horas o mostrados hace más de 6 horas)
async function limpiarMensajesAntiguos() {
    try {
        const ahora = new Date();
        const fechaVenezuela = new Date(ahora.toLocaleString('en-US', { timeZone: 'America/Caracas' }));
        const hace24Horas = new Date(fechaVenezuela);
        hace24Horas.setHours(hace24Horas.getHours() - 24);
        const fecha24HorasStr = hace24Horas.toISOString();

        // Solo eliminar mensajes:
        // 1. Más antiguos de 24 horas (sin importar si están mostrados o no)
        // 2. Mostrados hace más de 6 horas (para evitar duplicados durante ese período)
        db.run(`
            DELETE FROM chatbot_mensajes_proactivos
            WHERE fecha_creacion < ?
            OR (mostrado = 1 AND datetime(fecha_mostrado) < datetime('now', '-6 hours'))
        `, [fecha24HorasStr], function(err) {
            if (err) {
                console.error(' Error limpiando mensajes antiguos:', err);
            } else if (this.changes > 0) {
                console.log(` Limpiados ${this.changes} mensajes antiguos/mostrados`);
            }
        });
    } catch (error) {
        console.error(' Error en limpieza de mensajes:', error);
    }
}

// Endpoint para obtener mensajes proactivos
app.get('/api/chatbot/mensajes-proactivos', apiAuth, (req, res) => {
    const userId = req.session.user.id;
    console.log(`" GET /api/chatbot/mensajes-proactivos - userId: ${userId}`);
    
    // Limpiar mensajes antiguos antes de consultar
    limpiarMensajesAntiguos();
    
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
        
        console.log(`" Mensajes encontrados para userId ${userId}:`, mensajes.length);
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

// Endpoint para limpiar manualmente todos los mensajes (solo master)
app.post('/api/chatbot/mensajes-proactivos/limpiar', apiAuth, (req, res) => {
    if (req.session.user.role !== 'master') {
        return res.status(403).json({ error: 'No autorizado' });
    }
    
    limpiarMensajesAntiguos();
    res.json({ success: true, message: 'Limpieza iniciada' });
});

// Endpoint para ver todos los mensajes en la BD (debug - solo master)
app.get('/api/chatbot/mensajes-proactivos/debug', apiAuth, (req, res) => {
    if (req.session.user.role !== 'master') {
        return res.status(403).json({ error: 'No autorizado' });
    }
    
    db.all(`
        SELECT m.*, u.username 
        FROM chatbot_mensajes_proactivos m
        JOIN usuarios u ON m.usuario_id = u.id
        ORDER BY m.fecha_creacion DESC
        LIMIT 50
    `, [], (err, mensajes) => {
        if (err) {
            return res.status(500).json({ error: err.message });
        }
        res.json({ mensajes });
    });
});

// Ejecutar monitoreo cada 30 segundos (para pruebas - cambiar a 10 min en producción)
const INTERVALO_MONITOREO = 30 * 1000; // 30 segundos
const INTERVALO_LIMPIEZA = 60 * 60 * 1000; // 1 hora
const INTERVALO_GENERACION_TAREAS = 24 * 60 * 60 * 1000; // 24 horas
let intervaloMonitoreo = null;
let intervaloLimpieza = null;
let intervaloGeneracionTareas = null;

/**
 * Genera tareas automáticamente desde alertas pendientes
 * Distribuye equitativamente entre operadores
 */
async function generarTareasAutomaticas() {
    try {
        console.log('"‹ Generando tareas automáticas desde alertas...');
        const fechaHoy = hoyLocalYYYYMMDD();
        
        // Obtener alertas activas SIN acción realizada (sin mensaje_enviado ni promocion_enviada)
        // Permitir reasignar si: 1) sin tarea, 2) tarea eliminada, 3) tarea cancelada, 4) tarea de días anteriores
        const alertasSinResolver = await dbAll(`
            SELECT a.* 
            FROM alertas a
            WHERE a.activa = 1 
            AND a.accion_realizada IS NULL
            AND (
                a.tarea_id IS NULL 
                OR NOT EXISTS (SELECT 1 FROM tareas t WHERE t.id = a.tarea_id)
                OR EXISTS (
                    SELECT 1 FROM tareas t 
                    WHERE t.id = a.tarea_id 
                    AND (t.estado = 'cancelada' OR t.fecha_creacion < ?)
                )
            )
        `, [fechaHoy]);
        
        if (alertasSinResolver.length === 0) {
            console.log('... No hay alertas pendientes para crear tareas');
            return;
        }
        
        // Obtener operadores disponibles (excluir master)
        const operadores = await dbAll(`
            SELECT id, username FROM usuarios WHERE role != 'master' ORDER BY id
        `);
        
        if (operadores.length === 0) {
            console.log('️ No hay operadores disponibles para asignar tareas');
            return;
        }
        
        let indiceOperador = 0;
        let tareasCreadas = 0;
        
        // Distribuir alertas equitativamente
        for (const alerta of alertasSinResolver) {
            // Seleccionar operador por rotación
            const operador = operadores[indiceOperador];
            indiceOperador = (indiceOperador + 1) % operadores.length;
            
            // Obtener datos del cliente y RECALCULAR días de inactividad en tiempo real
            const cliente = await dbGet(`
                SELECT c.nombre, MAX(o.fecha) as ultima_operacion,
                       CAST(julianday('now') - julianday(
                           CASE 
                               WHEN o.fecha LIKE '__-__-____' THEN substr(o.fecha, 7, 4) || '-' || substr(o.fecha, 4, 2) || '-' || substr(o.fecha, 1, 2)
                               ELSE o.fecha
                           END
                       ) AS INTEGER) as dias_reales
                FROM clientes c
                LEFT JOIN operaciones o ON c.id = o.cliente_id
                WHERE c.id = ?
                GROUP BY c.id
            `, [alerta.cliente_id]);
            
            // Validar en tiempo real si aún cumple criterio
            const diasInactivo = cliente?.dias_reales || 0;
            if (alerta.tipo === 'inactivo' && (diasInactivo < 30 || diasInactivo > 60)) {
                continue; // Saltar si ya no cumple
            }
            if (alerta.tipo === 'critico' && diasInactivo <= 60) {
                continue; // Saltar si ya no cumple (debe ser más de 60 días)
            }
            
            // Para reducción de frecuencia, verificar AHORA
            if (alerta.tipo === 'disminucion') {
                const hace30 = new Date(new Date().getTime() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
                const hace60 = new Date(new Date().getTime() - 60 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
                
                const recientes = await dbGet(`SELECT COUNT(*) as cnt FROM operaciones WHERE cliente_id = ? AND fecha >= ?`, [alerta.cliente_id, hace30]);
                const anteriores = await dbGet(`SELECT COUNT(*) as cnt FROM operaciones WHERE cliente_id = ? AND fecha >= ? AND fecha < ?`, [alerta.cliente_id, hace60, hace30]);
                
                if (anteriores.cnt < 3 || recientes.cnt >= anteriores.cnt * 0.5) {
                    continue; // Saltar si ya no hay reducción
                }
            }
            
            // Determinar prioridad según días REALES de inactividad
            let prioridad = 'normal';
            if (diasInactivo > 60) prioridad = 'urgente';
            else if (diasInactivo >= 45) prioridad = 'alta';
            
            // ANTES de crear nueva tarea, cancelar SOLO tareas PENDIENTES antiguas del mismo cliente
            // NO cancelar tareas en_progreso (el operador esta trabajando en ellas)
            const canceladas = await dbRun(`
                UPDATE tareas
                SET estado = 'cancelada',
                    observaciones = 'Tarea obsoleta - reemplazada por nueva tarea automatica'
                WHERE cliente_id = ?
                AND tipo = 'automatica'
                AND estado = 'pendiente'
                AND fecha_creacion < ?
            `, [alerta.cliente_id, fechaHoy]);

            if (canceladas.changes > 0) {
                console.log(`   Canceladas ${canceladas.changes} tareas obsoletas del cliente ${alerta.cliente_id}`);
            }
            
            // Crear tarea con días REALES
            const titulo = `Reactivar cliente: ${cliente ? cliente.nombre : 'Desconocido'}`;
            const descripcion = `${alerta.tipo === 'inactivo' ? 'Cliente inactivo' : alerta.tipo === 'critico' ? 'Cliente crítico' : 'Disminución de frecuencia'} - ${diasInactivo ? `${diasInactivo} días sin actividad` : 'Reducción de operaciones'}. ltima operación: ${cliente?.ultima_operacion || 'N/A'}`;
            
            const resultTarea = await dbRun(`
                INSERT INTO tareas(titulo, descripcion, tipo, prioridad, asignado_a, creado_por, fecha_creacion, tipo_alerta, cliente_id, cliente_nombre)
                VALUES (?, ?, 'automatica', ?, ?, 1, ?, ?, ?, ?)
            `, [titulo, descripcion, prioridad, operador.id, fechaHoy, alerta.tipo, alerta.cliente_id, cliente ? cliente.nombre : 'Desconocido']);
            
            // Vincular tarea con alerta
            await dbRun(`
                UPDATE alertas SET tarea_id = ? WHERE id = ?
            `, [resultTarea.lastID, alerta.id]);
            
            // Crear notificación para el operador
            await dbRun(`
                INSERT INTO notificaciones(usuario_id, tipo, titulo, mensaje, fecha_creacion, tarea_id)
                VALUES (?, 'tarea', 'Nueva tarea asignada automáticamente', ?, ?, ?)
            `, [operador.id, titulo, fechaHoy, resultTarea.lastID]);
            
            tareasCreadas++;
        }
        
        console.log(`... ${tareasCreadas} tareas creadas y distribuidas entre ${operadores.length} operadores`);
    } catch (error) {
        console.error(' Error generando tareas automáticas:', error);
    }
}

function iniciarMonitoreoProactivo() {
    // Ejecutar inmediatamente
    setTimeout(generarMensajesProactivos, 3000); // 3 segundos después del inicio
    
    // Luego cada 30 segundos
    intervaloMonitoreo = setInterval(generarMensajesProactivos, INTERVALO_MONITOREO);
    console.log('- Sistema de monitoreo proactivo iniciado (cada 30 segundos)');
    
    // Limpieza de mensajes antiguos cada hora
    intervaloLimpieza = setInterval(limpiarMensajesAntiguos, INTERVALO_LIMPIEZA);
    console.log(' Sistema de limpieza de mensajes iniciado (cada 1 hora)');
    
    // Generar tareas automáticas cada 24 horas
    setTimeout(generarTareasAutomaticas, 10000); // Primera ejecución 10 segundos después del inicio
    intervaloGeneracionTareas = setInterval(generarTareasAutomaticas, INTERVALO_GENERACION_TAREAS);
    console.log('"‹ Sistema de generación automática de tareas iniciado (cada 24 horas)');
    
    // Verificar bonos de referidos expirados cada 6 horas
    setTimeout(verificarBonosExpirados, 30000);
    setInterval(verificarBonosExpirados, 6 * 60 * 60 * 1000);
    console.log('&#127873; Sistema de verificación de bonos de referidos iniciado (cada 6 horas)');
}

// Funcion para verificar bonos de referidos expirados
async function verificarBonosExpirados() {
    try {
        const ahora = new Date().toISOString();
        
        // Marcar como expirados los bonos que pasaron su fecha limite
        const result = await dbRun(`
            UPDATE referidos_bonos 
            SET estado = 'expirado' 
            WHERE estado = 'pendiente' AND fecha_limite < ?
        `, [ahora]);
        
        if (result.changes > 0) {
            console.log(`... ${result.changes} bonos de referidos marcados como expirados`);
        }
    } catch (error) {
        console.error('Error verificando bonos expirados:', error);
    }
}

// =================================================================
// FIN: SISTEMA DE MONITOREO PROACTIVO
// =================================================================

// =================================================================
// "" SISTEMA DE MONITOREO DE TASAS P2P vs MANUALES
// =================================================================

let alertaTasasPendiente = null; // { timestamp, tasa_manual, tasa_p2p, notificado, timeout_id }
let intervaloMonitoreoTasas = null;

/**
 * Compara las tasas manuales vs P2P y genera alertas si es necesario
 */
async function monitorearTasasVES() {
    try {
        console.log('Verificando tasas VES: Manual vs Binance P2P...');

        // 1. Obtener tasa manual configurada (nivel 3 = 250.000 CLP)
        const tasaManual = await readConfigValue('tasaNivel3');
        
        if (!tasaManual || tasaManual === 0) {
            console.log('️ No hay tasa manual configurada (tasaNivel3), omitiendo monitoreo');
            return;
        }

        // 2. Consultar tasas P2P de Binance
        let tasaP2PAjustada = 0;
        let tasa_base_clp_ves = 0;
        try {
            const [tasa_ves_p2p, tasa_clp_p2p] = await Promise.all([
                obtenerTasaVentaVES(),
                obtenerTasaCompraCLP()
            ]);

            tasa_base_clp_ves = tasa_ves_p2p / tasa_clp_p2p;
            tasaP2PAjustada = tasa_base_clp_ves * (1 - 0.04); // -4% para 250K CLP
            
            console.log(`" Tasa Manual (250K): ${tasaManual.toFixed(4)} VES/CLP`);
            console.log(`" Tasa P2P Ajustada -4%: ${tasaP2PAjustada.toFixed(4)} VES/CLP`);
        } catch (error) {
            console.error(' Error consultando Binance P2P:', error.message);
            return;
        }

        // 3. Obtener margen de tolerancia configurado (por defecto 2%)
        let margenTolerancia = await readConfigValue('margenToleranciaAlertas');
        if (!margenTolerancia || margenTolerancia === 0) {
            margenTolerancia = 2.0; // Margen por defecto: 2%
        }
        
        // 4. COMPARAR: Si tasa manual > tasa P2P (estamos cobrando MÁS de lo debido)
        const diferencia = tasaManual - tasaP2PAjustada;
        const porcentajeDiferencia = (diferencia / tasaP2PAjustada) * 100;

        console.log(`" Diferencia: ${porcentajeDiferencia.toFixed(2)}% | Margen tolerancia: ${margenTolerancia}%`);

        // Solo alertar si la diferencia supera el margen de tolerancia
        if (tasaManual > tasaP2PAjustada && porcentajeDiferencia > margenTolerancia) {
            console.log(` ALERTA: Tasa manual es ${porcentajeDiferencia.toFixed(2)}% MAYOR que P2P (supera margen de ${margenTolerancia}%)`);
            
            // Verificar si ya existe una alerta pendiente
            if (alertaTasasPendiente) {
                const tiempoTranscurrido = Date.now() - alertaTasasPendiente.timestamp;
                const minutosTranscurridos = Math.floor(tiempoTranscurrido / 60000);
                
                console.log(`⏳ Alerta existente. Tiempo transcurrido: ${minutosTranscurridos} minutos`);
                
                // Si ya pasaron 15 minutos y el Master no actualizó
                if (minutosTranscurridos >= 15) {
                    console.log('"" 15 minutos transcurridos. Actualizando las 3 tasas automáticamente...');
                    await actualizarTasasAutomaticamente(alertaTasasPendiente.tasa_base);
                    
                    // Limpiar alerta
                    if (alertaTasasPendiente.timeout_id) {
                        clearTimeout(alertaTasasPendiente.timeout_id);
                    }
                    alertaTasasPendiente = null;
                }
            } else {
                // Nueva alerta - Notificar a todos los usuarios
                console.log('"" Generando nueva alerta de tasas...');
                await generarAlertaTasas(tasaManual, tasaP2PAjustada, diferencia, porcentajeDiferencia);
                
                // Programar actualización automática de las 3 tasas en 15 minutos
                const timeoutId = setTimeout(async () => {
                    console.log('⏰ Timeout de 15 minutos alcanzado. Actualizando las 3 tasas...');
                    await actualizarTasasAutomaticamente(alertaTasasPendiente.tasa_base);
                    alertaTasasPendiente = null;
                }, 15 * 60 * 1000); // 15 minutos
                
                alertaTasasPendiente = {
                    timestamp: Date.now(),
                    tasa_manual: tasaManual,
                    tasa_p2p: tasaP2PAjustada,
                    tasa_base: tasa_base_clp_ves, //  Guardamos la tasa base para reutilizarla
                    notificado: true,
                    timeout_id: timeoutId
                };
            }
        } else if (tasaManual > tasaP2PAjustada && porcentajeDiferencia <= margenTolerancia) {
            // Diferencia dentro del margen de tolerancia - no alertar
            console.log(`... Diferencia ${porcentajeDiferencia.toFixed(2)}% dentro del margen de tolerancia (${margenTolerancia}%) - No se alerta`);
            
            // Si había alerta pendiente, cancelarla
            if (alertaTasasPendiente) {
                console.log('... Diferencia ahora dentro del margen. Cancelando alerta...');
                if (alertaTasasPendiente.timeout_id) {
                    clearTimeout(alertaTasasPendiente.timeout_id);
                }
                alertaTasasPendiente = null;
            }
        } else {
            console.log(`... Tasas OK: Manual (${tasaManual.toFixed(4)}) <= P2P (${tasaP2PAjustada.toFixed(4)})`);
            
            // Si había alerta pendiente pero las tasas ya se corrigieron, cancelarla
            if (alertaTasasPendiente) {
                console.log('... Tasas corregidas por el Master. Cancelando alerta...');
                if (alertaTasasPendiente.timeout_id) {
                    clearTimeout(alertaTasasPendiente.timeout_id);
                }
                alertaTasasPendiente = null;
            }
        }
    } catch (error) {
        console.error(' Error en monitoreo de tasas:', error);
    }
}

/**
 * Genera alerta para TODOS los usuarios (especialmente Master)
 */
async function generarAlertaTasas(tasaManual, tasaP2P, diferencia, porcentaje) {
    try {
        const fechaHoy = new Date().toISOString();
        
        // Obtener TODOS los usuarios (Master y operadores)
        const usuarios = await dbAll('SELECT id, username, role FROM usuarios');
        
        console.log(`" Generando alertas de tasas para ${usuarios.length} usuario(s)...`);
        
        for (const usuario of usuarios) {
            let mensaje = '';
            let prioridad = 'urgente';
            
            if (usuario.role === 'master') {
                // Mensaje para Master - más técnico y con acción requerida
                mensaje = `⚠️ **ALERTA DE TASAS - ACCIÓN REQUERIDA**\n\n` +
                         `📊 **Tasa Manual (250K CLP):** ${tasaManual.toFixed(4)} VES/CLP\n` +
                         `📊 **Tasa Binance P2P (-4%):** ${tasaP2P.toFixed(4)} VES/CLP\n` +
                         `⚠️ **Diferencia:** +${diferencia.toFixed(4)} VES/CLP (${porcentaje.toFixed(2)}% más alta)\n\n` +
                         `🚨 **Nuestra tasa está MÁS ALTA que el mercado - POSIBLES PÉRDIDAS**\n\n` +
                         `**ACCIÓN URGENTE:** Actualiza las tasas manualmente en /admin.html\n\n` +
                         `⏰ **Si no actualizas en 15 minutos:**\n` +
                         `   El sistema actualizará automáticamente las 3 tasas (5K, 100K, 250K)\n` +
                         `   basándose en las tasas actuales de Binance P2P.`;
            } else {
                // Mensaje para operadores - informativo y directivo
                mensaje = `⚠️ **ALERTA: Tasas MÁS ALTAS que mercado**\n\n` +
                         `📊 Nuestra tasa: ${tasaManual.toFixed(4)} VES/CLP\n` +
                         `📊 Mercado P2P: ${tasaP2P.toFixed(4)} VES/CLP\n` +
                         `⚠️ Diferencia: +${diferencia.toFixed(4)} VES/CLP (${porcentaje.toFixed(2)}% más alta)\n\n` +
                         `🚨 **Estamos dando más de lo necesario - posibles pérdidas.**\n\n` +
                         `**⚡ ACCIÓN INMEDIATA:**\n` +
                         `   📞 Informa al Master AHORA\n` +
                         `   📱 Contacta vía WhatsApp/llamada si es necesario\n\n` +
                         `⏰ En 15 minutos el sistema actualizará las tasas automáticamente.`;
            }
            
            // Crear notificación en la BD
            await dbRun(`
                INSERT INTO notificaciones(usuario_id, tipo, titulo, mensaje, fecha_creacion, leida)
                VALUES (?, 'alerta', 'Alerta de Tasas VES', ?, ?, 0)
            `, [usuario.id, mensaje, fechaHoy]);
            
            // Crear mensaje proactivo para el chatbot
            await dbRun(`
                INSERT INTO chatbot_mensajes_proactivos(usuario_id, tipo, mensaje, prioridad, fecha_creacion, mostrado, contexto)
                VALUES (?, 'alerta', ?, 'urgente', ?, 0, ?)
            `, [
                usuario.id,
                mensaje,
                fechaHoy,
                JSON.stringify({ 
                    tasa_manual: tasaManual, 
                    tasa_p2p: tasaP2P, 
                    diferencia: diferencia,
                    porcentaje: porcentaje
                })
            ]);
            
            console.log(`... Alerta de tasas enviada a: ${usuario.username} (${usuario.role})`);
        }
        
    } catch (error) {
        console.error(' Error generando alerta de tasas:', error);
    }
}

/**
 * Actualiza las 3 tasas automáticamente usando la tasa base ya calculada
 * @param {number} tasa_base_clp_ves - Tasa base VES/CLP de Binance P2P ya calculada
 */
async function actualizarTasasAutomaticamente(tasa_base_clp_ves) {
    try {
        console.log('"" Actualizando las 3 tasas automáticamente...');
        console.log(`" Usando tasa base P2P: ${tasa_base_clp_ves.toFixed(4)} VES/CLP (calculada previamente)`);
        
        // Función auxiliar para truncar (NO redondear) a exactamente 4 decimales
        const truncarA4Decimales = (num) => {
            return Math.floor(num * 10000) / 10000;
        };
        
        // 1. Calcular las 3 tasas con sus respectivos ajustes y truncar a 4 decimales
        const tasa_nivel1 = truncarA4Decimales(tasa_base_clp_ves * (1 - 0.05));  // -5% para 5K CLP
        const tasa_nivel2 = truncarA4Decimales(tasa_base_clp_ves * (1 - 0.045)); // -4.5% para 100K CLP
        const tasa_nivel3 = truncarA4Decimales(tasa_base_clp_ves * (1 - 0.04));  // -4% para 250K CLP
        
        // 2. Actualizar las 3 tasas en la base de datos
        await dbRun(`INSERT OR REPLACE INTO configuracion(clave, valor) VALUES ('tasaNivel1', ?)`, [tasa_nivel1.toString()]);
        await dbRun(`INSERT OR REPLACE INTO configuracion(clave, valor) VALUES ('tasaNivel2', ?)`, [tasa_nivel2.toString()]);
        await dbRun(`INSERT OR REPLACE INTO configuracion(clave, valor) VALUES ('tasaNivel3', ?)`, [tasa_nivel3.toString()]);
        
        console.log(`... Tasas actualizadas en BD (truncadas a 4 decimales SIN redondear):`);
        console.log(`   - Nivel 1 (5K CLP, -5%): ${tasa_nivel1.toFixed(4)} VES/CLP`);
        console.log(`   - Nivel 2 (100K CLP, -4.5%): ${tasa_nivel2.toFixed(4)} VES/CLP`);
        console.log(`   - Nivel 3 (250K CLP, -4%): ${tasa_nivel3.toFixed(4)} VES/CLP`);
        
        // 5. Notificar a todos los usuarios que las tasas fueron actualizadas
        const usuarios = await dbAll('SELECT id, username, role FROM usuarios');
        const fechaHoy = new Date().toISOString();
        
        for (const usuario of usuarios) {
            let mensaje = '';
            
            if (usuario.role === 'master') {
                // Mensaje para Master
                mensaje = `... **Tasas actualizadas automáticamente**\n\n` +
                         `⏰ El tiempo de espera de 15 minutos expiró.\n` +
                         `- El sistema actualizó las 3 tasas según Binance P2P:\n\n` +
                         `" Nivel 1 (5K CLP): ${tasa_nivel1.toFixed(4)} VES/CLP\n` +
                         `" Nivel 2 (100K CLP): ${tasa_nivel2.toFixed(4)} VES/CLP\n` +
                         `" Nivel 3 (250K CLP): ${tasa_nivel3.toFixed(4)} VES/CLP\n\n` +
                         `... Las nuevas tasas ya están disponibles en el sistema.\n` +
                         `"‹ Puedes verificarlas en /admin.html`;
            } else {
                // Mensaje para Operadores
                mensaje = `... **TASAS ACTUALIZADAS AUTOMÁTICAMENTE**\n\n` +
                         `- El sistema actualizó las tasas según mercado P2P:\n\n` +
                         `" Nivel 1 (5K CLP): ${tasa_nivel1.toFixed(4)} VES/CLP\n` +
                         `" Nivel 2 (100K CLP): ${tasa_nivel2.toFixed(4)} VES/CLP\n` +
                         `" Nivel 3 (250K CLP): ${tasa_nivel3.toFixed(4)} VES/CLP\n\n` +
                         `... Las nuevas tasas ya están activas en el sistema.\n` +
                         `"‹ El Master fue notificado de la actualización.`;
            }
            
            await dbRun(`
                INSERT INTO notificaciones(usuario_id, tipo, titulo, mensaje, fecha_creacion, leida)
                VALUES (?, 'sistema', 'Tasas Actualizadas', ?, ?, 0)
            `, [usuario.id, mensaje, fechaHoy]);
            
            await dbRun(`
                INSERT INTO chatbot_mensajes_proactivos(usuario_id, tipo, mensaje, prioridad, fecha_creacion, mostrado)
                VALUES (?, 'informativo', ?, 'normal', ?, 0)
            `, [usuario.id, mensaje, fechaHoy]);
        }
        
    } catch (error) {
        console.error(' Error actualizando tasas automáticamente:', error);
    }
}

// Función generarMensajeWhatsApp eliminada - el sistema NO envía WhatsApp automáticamente
// Los operadores pueden comunicar cambios de tasas manualmente cuando lo consideren necesario

/**
 * Endpoint manual para forzar verificación de tasas (solo Master)
 */
app.post('/api/monitoreo/verificar-tasas', apiAuth, onlyMaster, async (req, res) => {
    try {
        await monitorearTasasVES();
        res.json({ 
            message: 'Verificación de tasas ejecutada',
            alerta_activa: !!alertaTasasPendiente,
            detalles: alertaTasasPendiente
        });
    } catch (error) {
        res.status(500).json({ message: 'Error verificando tasas', error: error.message });
    }
});

/**
 * Endpoint de diagnóstico completo (solo Master)
 */
app.get('/api/monitoreo/diagnostico-tasas', apiAuth, onlyMaster, async (req, res) => {
    try {
        const tasaManual = await readConfigValue('tasaNivel3');
        const margenTolerancia = await readConfigValue('margenToleranciaAlertas') || 2.0;
        
        let tasaP2PAjustada = null;
        let tasa_base = null;
        let errorP2P = null;
        
        try {
            const [tasa_ves_p2p, tasa_clp_p2p] = await Promise.all([
                obtenerTasaVentaVES(),
                obtenerTasaCompraCLP()
            ]);
            tasa_base = tasa_ves_p2p / tasa_clp_p2p;
            tasaP2PAjustada = tasa_base * (1 - 0.04);
        } catch (error) {
            errorP2P = error.message;
        }
        
        const diferencia = tasaManual && tasaP2PAjustada ? tasaManual - tasaP2PAjustada : null;
        const porcentajeDiferencia = diferencia && tasaP2PAjustada ? (diferencia / tasaP2PAjustada) * 100 : null;
        
        res.json({
            monitoreo_activo: !!intervaloMonitoreoTasas,
            alerta_pendiente: !!alertaTasasPendiente,
            configuracion: {
                tasa_manual_250k: tasaManual,
                margen_tolerancia_porcentaje: margenTolerancia
            },
            tasas_p2p: {
                tasa_base_binance: tasa_base,
                tasa_ajustada_menos_4: tasaP2PAjustada,
                error: errorP2P
            },
            comparacion: {
                diferencia_absoluta: diferencia,
                diferencia_porcentaje: porcentajeDiferencia,
                supera_margen: porcentajeDiferencia ? porcentajeDiferencia > margenTolerancia : false,
                deberia_alertar: tasaManual && tasaP2PAjustada && tasaManual > tasaP2PAjustada && porcentajeDiferencia > margenTolerancia
            },
            alerta_actual: alertaTasasPendiente ? {
                timestamp: new Date(alertaTasasPendiente.timestamp).toISOString(),
                minutos_transcurridos: Math.floor((Date.now() - alertaTasasPendiente.timestamp) / 60000),
                tasa_manual: alertaTasasPendiente.tasa_manual,
                tasa_p2p: alertaTasasPendiente.tasa_p2p,
                tasa_base_guardada: alertaTasasPendiente.tasa_base
            } : null
        });
    } catch (error) {
        res.status(500).json({ message: 'Error en diagnóstico', error: error.message });
    }
});

/**
 * Endpoint para cancelar alerta de tasas manualmente (solo Master)
 */
app.post('/api/monitoreo/cancelar-alerta-tasas', apiAuth, onlyMaster, (req, res) => {
    if (alertaTasasPendiente) {
        if (alertaTasasPendiente.timeout_id) {
            clearTimeout(alertaTasasPendiente.timeout_id);
        }
        alertaTasasPendiente = null;
        res.json({ message: 'Alerta de tasas cancelada' });
    } else {
        res.json({ message: 'No hay alerta activa' });
    }
});

/**
 * Endpoint para obtener estado del monitoreo de tasas
 */
app.get('/api/monitoreo/estado-tasas', apiAuth, onlyMaster, async (req, res) => {
    try {
        const tasaManual = await readConfigValue('tasaNivel3');
        
        let tasaP2P = null;
        try {
            const [tasa_ves_p2p, tasa_clp_p2p] = await Promise.all([
                obtenerTasaVentaVES(),
                obtenerTasaCompraCLP()
            ]);
            const tasa_base = tasa_ves_p2p / tasa_clp_p2p;
            tasaP2P = tasa_base * (1 - 0.04);
        } catch (error) {
            // Ignorar error
        }
        
        res.json({
            monitoreo_activo: !!intervaloMonitoreoTasas,
            alerta_activa: !!alertaTasasPendiente,
            tasa_manual: tasaManual,
            tasa_p2p_actual: tasaP2P,
            diferencia: tasaP2P ? (tasaManual - tasaP2P) : null,
            detalles_alerta: alertaTasasPendiente ? {
                tiempo_transcurrido_minutos: Math.floor((Date.now() - alertaTasasPendiente.timestamp) / 60000),
                tiempo_restante_minutos: 15 - Math.floor((Date.now() - alertaTasasPendiente.timestamp) / 60000)
            } : null
        });
    } catch (error) {
        res.status(500).json({ message: 'Error obteniendo estado', error: error.message });
    }
});

/**
 * Iniciar monitoreo de tasas (cada 2 minutos para respuesta más rápida)
 */
function iniciarMonitoreoTasas() {
            configurarWebhookTelegram();    // Configurar webhook Telegram
    // Ejecutar verificación inicial después de 10 segundos
    setTimeout(monitorearTasasVES, 10000);
    
    // Luego verificar cada 2 minutos (más frecuente que antes)
    intervaloMonitoreoTasas = setInterval(monitorearTasasVES, 2 * 60 * 1000);
    
    console.log('Sistema de monitoreo de tasas P2P iniciado (cada 2 minutos)');
}

// =================================================================
// FIN: SISTEMA DE MONITOREO DE TASAS
// =================================================================

// =================================================================
// SISTEMA DE N"MINA
// =================================================================

// Obtener periodo actual o crear uno nuevo
app.get('/api/nomina/periodo-actual', apiAuth, onlyMaster, async (req, res) => {
  try {
    const hoy = new Date();
    const anio = hoy.getFullYear();
    const mes = hoy.getMonth() + 1;
    const dia = hoy.getDate();
    const quincena = dia <= 15 ? 1 : 2;

    // Buscar periodo existente
    let periodo = await dbGet(
      'SELECT * FROM periodos_pago WHERE anio = ? AND mes = ? AND quincena = ?',
      [anio, mes, quincena]
    );

    // Si no existe, crear uno nuevo
    if (!periodo) {
      const fecha_inicio = quincena === 1 ? `${anio}-${mes.toString().padStart(2, '0')}-01` : `${anio}-${mes.toString().padStart(2, '0')}-16`;
      const fecha_fin = quincena === 1 ? `${anio}-${mes.toString().padStart(2, '0')}-15` : `${anio}-${mes.toString().padStart(2, '0')}-${new Date(anio, mes, 0).getDate()}`;

      const result = await dbRun(
        'INSERT INTO periodos_pago (anio, mes, quincena, fecha_inicio, fecha_fin) VALUES (?, ?, ?, ?, ?)',
        [anio, mes, quincena, fecha_inicio, fecha_fin]
      );

      periodo = await dbGet('SELECT * FROM periodos_pago WHERE id = ?', [result.lastID]);
    }

    res.json(periodo);
  } catch (error) {
    console.error('Error obteniendo periodo actual:', error);
    res.status(500).json({ error: 'Error obteniendo periodo actual' });
  }
});

// Obtener todos los periodos
app.get('/api/nomina/periodos', apiAuth, onlyMaster, async (req, res) => {
  try {
    const periodos = await dbAll(
      'SELECT * FROM periodos_pago ORDER BY anio DESC, mes DESC, quincena DESC'
    );
    res.json(periodos);
  } catch (error) {
    console.error('Error obteniendo periodos:', error);
    res.status(500).json({ error: 'Error obteniendo periodos' });
  }
});

// Calcular nómina para un periodo
app.post('/api/nomina/calcular/:periodoId', apiAuth, onlyMaster, async (req, res) => {
  try {
    const { periodoId } = req.params;
    
    // Obtener el periodo
    const periodo = await dbGet('SELECT * FROM periodos_pago WHERE id = ?', [periodoId]);
    if (!periodo) {
      return res.status(404).json({ error: 'Periodo no encontrado' });
    }

    // Obtener todos los operadores (role != 'master')
    const operadores = await dbAll("SELECT * FROM usuarios WHERE role != 'master'");

    const resultados = [];

    for (const operador of operadores) {
      // 1. CALCULAR HORAS TRABAJADAS (mismo algoritmo que monitoreo)
      // Obtener todas las actividades del período
      const actividades = await dbAll(`
        SELECT fecha, timestamp
        FROM actividad_operadores
        WHERE usuario_id = ?
        AND fecha BETWEEN ? AND ?
        AND tipo_actividad IN ('login', 'heartbeat', 'operacion', 'tarea', 'mensaje')
        ORDER BY timestamp ASC
      `, [operador.id, periodo.fecha_inicio, periodo.fecha_fin]);

      // Calcular horas con gaps de máximo 30 minutos (igual que monitoreo)
      let horasOnline = 0;
      let sesionInicio = null;
      let ultimaActividad = null;
      const UMBRAL_MINUTOS = 30;

      for (const act of actividades) {
        const timestamp = new Date(act.timestamp);
        
        if (!sesionInicio) {
          sesionInicio = timestamp;
          ultimaActividad = timestamp;
        } else {
          const diffMinutos = (timestamp - ultimaActividad) / (1000 * 60);
          
          if (diffMinutos > UMBRAL_MINUTOS) {
            // Gap > 30 min: cerrar sesión anterior e iniciar nueva
            horasOnline += (ultimaActividad - sesionInicio) / (1000 * 60 * 60);
            sesionInicio = timestamp;
          }
          
          ultimaActividad = timestamp;
        }
      }

      // Cerrar última sesión si existe
      if (sesionInicio && ultimaActividad) {
        horasOnline += (ultimaActividad - sesionInicio) / (1000 * 60 * 60);
      }

      const horas_trabajadas = Math.round(horasOnline * 100) / 100; // 2 decimales

      // 2. CALCULAR MILLONES COMISIONABLES (desde operaciones en CLP)
      const millonesResult = await dbGet(`
        SELECT COALESCE(SUM(monto_clp / 1000000.0), 0) as millones_comisionables
        FROM operaciones
        WHERE usuario_id = ?
        AND DATE(fecha) BETWEEN ? AND ?
      `, [operador.id, periodo.fecha_inicio, periodo.fecha_fin]);

      const millones_comisionables = millonesResult.millones_comisionables || 0;

      // 3. CONTAR DOMINGOS TRABAJADOS
      const domingosResult = await dbGet(`
        SELECT COUNT(DISTINCT fecha) as domingos
        FROM actividad_operadores
        WHERE usuario_id = ?
        AND fecha BETWEEN ? AND ?
        AND CAST(strftime('%w', fecha) AS INTEGER) = 0
      `, [operador.id, periodo.fecha_inicio, periodo.fecha_fin]);

      const domingos_trabajados = domingosResult.domingos || 0;

      // 4. CALCULAR PAGOS
      // Sistema quincenal: 135 horas - $0.94/hora = $127 USD base
      const TASA_POR_HORA = 0.94;
      const HORAS_MAXIMAS_QUINCENA = 135;
      const horas_a_pagar = Math.min(horas_trabajadas, HORAS_MAXIMAS_QUINCENA);
      const sueldo_base = horas_a_pagar * TASA_POR_HORA; // Pago por horas trabajadas hasta el tope
      
      const bono_atencion_rapida = 0; // Se agrega manualmente desde el botón Bonos
      const bono_asistencia = horas_trabajadas >= 80 ? 15.00 : 0; // $15 quincenal por cumplir 80 horas
      const comision_ventas = millones_comisionables * 2.00; // $2 por millón CLP
      const bono_domingos = domingos_trabajados * 8.00; // $8 por domingo
      const bonos_extra = 0; // Se pueden agregar manualmente después

      const total_pagar = sueldo_base + bono_atencion_rapida + bono_asistencia + comision_ventas + bono_domingos + bonos_extra;

      // 6. INSERTAR O ACTUALIZAR N"MINA
      await dbRun(`
        INSERT INTO nomina (
          periodo_id, usuario_id, sueldo_base, horas_trabajadas,
          bono_asistencia, bono_atencion_rapida, comision_ventas, millones_comisionables,
          bono_domingos, domingos_trabajados, bonos_extra, total_pagar, actualizado_en
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(periodo_id, usuario_id) DO UPDATE SET
          sueldo_base = excluded.sueldo_base,
          horas_trabajadas = excluded.horas_trabajadas,
          bono_asistencia = excluded.bono_asistencia,
          bono_atencion_rapida = excluded.bono_atencion_rapida,
          comision_ventas = excluded.comision_ventas,
          millones_comisionables = excluded.millones_comisionables,
          bono_domingos = excluded.bono_domingos,
          domingos_trabajados = excluded.domingos_trabajados,
          total_pagar = excluded.total_pagar,
          actualizado_en = CURRENT_TIMESTAMP
      `, [
        periodoId, operador.id, sueldo_base, horas_trabajadas,
        bono_asistencia, bono_atencion_rapida, comision_ventas, millones_comisionables,
        bono_domingos, domingos_trabajados, bonos_extra, total_pagar
      ]);

      resultados.push({
        operador: operador.username,
        horas_trabajadas,
        millones_comisionables,
        domingos_trabajados,
        total_pagar
      });
    }

    res.json({ mensaje: 'Nómina calculada exitosamente', resultados });
  } catch (error) {
    console.error('Error calculando nómina:', error);
    res.status(500).json({ error: 'Error calculando nómina' });
  }
});

// Obtener nómina de un periodo
app.get('/api/nomina/periodo/:periodoId', apiAuth, onlyMaster, async (req, res) => {
  try {
    const { periodoId } = req.params;
    
    const nominas = await dbAll(`
      SELECT 
        n.*,
        u.username,
        p.anio,
        p.mes,
        p.quincena,
        p.fecha_inicio,
        p.fecha_fin,
        p.estado as estado_periodo
      FROM nomina n
      JOIN usuarios u ON n.usuario_id = u.id
      JOIN periodos_pago p ON n.periodo_id = p.id
      WHERE n.periodo_id = ?
      ORDER BY n.total_pagar DESC
    `, [periodoId]);

    res.json(nominas);
  } catch (error) {
    console.error('Error obteniendo nómina:', error);
    res.status(500).json({ error: 'Error obteniendo nómina' });
  }
});

// Limpiar datos de actividad incorrectos (solo master)
app.delete('/api/actividad/limpiar', apiAuth, onlyMaster, async (req, res) => {
  try {
    const { fecha_inicio, fecha_fin } = req.query;
    
    if (!fecha_inicio || !fecha_fin) {
      return res.status(400).json({ error: 'Se requieren fecha_inicio y fecha_fin' });
    }
    
    const result = await dbRun(`
      DELETE FROM actividad_operadores
      WHERE fecha BETWEEN ? AND ?
    `, [fecha_inicio, fecha_fin]);
    
    res.json({ 
      mensaje: 'Actividad limpiada exitosamente',
      registros_eliminados: result.changes 
    });
  } catch (error) {
    console.error('Error limpiando actividad:', error);
    res.status(500).json({ error: 'Error limpiando actividad' });
  }
});

// Actualizar bonos extras de un operador
app.put('/api/nomina/:nominaId/bonos', apiAuth, onlyMaster, async (req, res) => {
  try {
    const { nominaId } = req.params;
    const { bono_atencion_rapida, bonos_extra, nota_bonos } = req.body;

    // Obtener la nómina actual
    const nomina = await dbGet('SELECT * FROM nomina WHERE id = ?', [nominaId]);
    if (!nomina) {
      return res.status(404).json({ error: 'Nómina no encontrada' });
    }

    // Recalcular el total
    const nuevo_bono_atencion = parseFloat(bono_atencion_rapida || 0);
    const nuevo_total = nomina.sueldo_base + nomina.bono_asistencia + nuevo_bono_atencion + 
                        nomina.comision_ventas + nomina.bono_domingos + parseFloat(bonos_extra || 0);

    await dbRun(
      'UPDATE nomina SET bono_atencion_rapida = ?, bonos_extra = ?, nota_bonos = ?, total_pagar = ?, actualizado_en = CURRENT_TIMESTAMP WHERE id = ?',
      [nuevo_bono_atencion, bonos_extra || 0, nota_bonos || null, nuevo_total, nominaId]
    );

    res.json({ mensaje: 'Bonos actualizados exitosamente' });
  } catch (error) {
    console.error('Error actualizando bonos:', error);
    res.status(500).json({ error: 'Error actualizando bonos' });
  }
});

// Cerrar un periodo (no se podrá modificar después)
app.post('/api/nomina/periodo/:periodoId/cerrar', apiAuth, onlyMaster, async (req, res) => {
  try {
    const { periodoId } = req.params;

    await dbRun(
      'UPDATE periodos_pago SET estado = ?, fecha_cierre = CURRENT_TIMESTAMP WHERE id = ?',
      ['cerrado', periodoId]
    );

    res.json({ mensaje: 'Periodo cerrado exitosamente' });
  } catch (error) {
    console.error('Error cerrando periodo:', error);
    res.status(500).json({ error: 'Error cerrando periodo' });
  }
});

// Generar periodos futuros automáticamente
app.post('/api/nomina/generar-periodos', apiAuth, onlyMaster, async (req, res) => {
  try {
    const { meses } = req.body;

    if (!meses || meses < 1 || meses > 24) {
      return res.status(400).json({ error: 'Número de meses inválido (1-24)' });
    }

    const hoy = new Date();
    let creados = 0;
    let existentes = 0;

    // Generar periodos desde el mes actual hasta N meses en el futuro
    for (let i = 0; i < meses; i++) {
      const fecha = new Date(hoy.getFullYear(), hoy.getMonth() + i, 1);
      const anio = fecha.getFullYear();
      const mes = fecha.getMonth() + 1;

      // Crear ambas quincenas para cada mes
      for (let quincena = 1; quincena <= 2; quincena++) {
        // Verificar si ya existe
        const existente = await dbGet(
          'SELECT id FROM periodos_pago WHERE anio = ? AND mes = ? AND quincena = ?',
          [anio, mes, quincena]
        );

        if (existente) {
          existentes++;
          continue;
        }

        // Calcular fechas de inicio y fin
        const fecha_inicio = quincena === 1
          ? `${anio}-${mes.toString().padStart(2, '0')}-01`
          : `${anio}-${mes.toString().padStart(2, '0')}-16`;

        const fecha_fin = quincena === 1
          ? `${anio}-${mes.toString().padStart(2, '0')}-15`
          : `${anio}-${mes.toString().padStart(2, '0')}-${new Date(anio, mes, 0).getDate()}`;

        // Crear el periodo
        await dbRun(
          'INSERT INTO periodos_pago (anio, mes, quincena, fecha_inicio, fecha_fin) VALUES (?, ?, ?, ?, ?)',
          [anio, mes, quincena, fecha_inicio, fecha_fin]
        );

        creados++;
      }
    }

    res.json({
      mensaje: `Periodos generados exitosamente`,
      creados,
      existentes,
      total: creados + existentes
    });
  } catch (error) {
    console.error('Error generando periodos:', error);
    res.status(500).json({ error: 'Error generando periodos' });
  }
});

// Marcar periodo como pagado
app.post('/api/nomina/periodo/:periodoId/pagar', apiAuth, onlyMaster, async (req, res) => {
  try {
    const { periodoId } = req.params;

    await dbRun(
      'UPDATE periodos_pago SET estado = ?, fecha_pago = CURRENT_TIMESTAMP WHERE id = ?',
      ['pagado', periodoId]
    );

    res.json({ mensaje: 'Periodo marcado como pagado' });
  } catch (error) {
    console.error('Error marcando periodo como pagado:', error);
    res.status(500).json({ error: 'Error marcando periodo como pagado' });
  }
});

// Registrar atención rápida (llamar desde endpoint de operaciones/mensajes)
async function registrarAtencionRapida(usuario_id, cliente_id, tipo, tiempo_respuesta_minutos) {
  try {
    if (tiempo_respuesta_minutos <= 5) {
      const fecha = hoyLocalYYYYMMDD();
      await dbRun(
        'INSERT INTO atencion_rapida (usuario_id, cliente_id, tipo, fecha, tiempo_respuesta_minutos) VALUES (?, ?, ?, ?, ?)',
        [usuario_id, cliente_id, tipo, fecha, tiempo_respuesta_minutos]
      );
    }
  } catch (error) {
    console.error('Error registrando atención rápida:', error);
  }
}

// =================================================================
// FIN: SISTEMA DE N"MINA
// =================================================================

// =================================================================
// INICIO: APP CLIENTE M"VIL - AUTENTICACI'N Y ENDPOINTS
// =================================================================

// Función para generar token simple
function generarTokenCliente() {
    return crypto.randomBytes(32).toString('hex');
}

// Función para verificar token de Google usando google-auth-library
const { OAuth2Client } = require('google-auth-library');
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || '951549639267-4c4928hcndqkc6j2cf95gafce48o1i28.apps.googleusercontent.com';
const googleClient = new OAuth2Client(GOOGLE_CLIENT_ID);

async function verificarGoogleToken(credential) {
    try {
        const ticket = await googleClient.verifyIdToken({
            idToken: credential,
            audience: GOOGLE_CLIENT_ID,
        });
        const payload = ticket.getPayload();

        if (!payload.email_verified) {
            throw new Error('Email de Google no verificado');
        }

        return {
            google_id: payload.sub,
            email: payload.email,
            nombre: payload.name,
            foto_url: payload.picture,
            email_verificado: payload.email_verified
        };
    } catch (error) {
        console.error('Error verificando token de Google:', error);
        throw error;
    }
}

// Duración de token de cliente: 7 días
const TOKEN_EXPIRACION_MS = 7 * 24 * 60 * 60 * 1000;

// Middleware para autenticación de clientes de la app
const clienteAuth = async (req, res, next) => {
    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'Token no proporcionado' });
    }

    const token = authHeader.split(' ')[1];

    try {
        const cliente = await dbGet(
            'SELECT * FROM clientes_app WHERE token_sesion = ? AND activo = 1',
            [token]
        );

        if (!cliente) {
            return res.status(401).json({ error: 'Token inválido o sesión expirada' });
        }

        // Verificar expiración del token
        if (cliente.token_expiracion && new Date(cliente.token_expiracion) < new Date()) {
            await dbRun('UPDATE clientes_app SET token_sesion = NULL, token_expiracion = NULL WHERE id = ?', [cliente.id]);
            return res.status(401).json({ error: 'Sesión expirada. Inicia sesión de nuevo.' });
        }

        req.clienteApp = cliente;
        req.clienteId = cliente.id;
        next();
    } catch (error) {
        console.error('Error en autenticación de cliente:', error);
        res.status(500).json({ error: 'Error de autenticación' });
    }
};

// Servir archivos estáticos de la app cliente con headers de caché optimizados
app.use('/app-cliente', express.static(path.join(__dirname, 'app-cliente'), { setHeaders: setStaticCacheHeaders }));
app.get('/favicon.ico', (req, res) => {
    res.sendFile(path.join(__dirname, 'app-cliente', 'assets', 'defioracle-logo.png'));
});

// POST /api/cliente/auth/google - Autenticación con Google
app.post('/api/cliente/auth/google', async (req, res) => {
    try {
        const { credential } = req.body;
        
        if (!credential) {
            return res.status(400).json({ error: 'Credencial de Google no proporcionada' });
        }
        
        // Verificar el token de Google
        const googleUser = await verificarGoogleToken(credential);
        
        if (!googleUser.email_verificado) {
            return res.status(400).json({ error: 'El email de Google no está verificado' });
        }
        
        // Buscar si el usuario ya existe
        let cliente = await dbGet(
            'SELECT * FROM clientes_app WHERE google_id = ?',
            [googleUser.google_id]
        );
        
        const token = generarTokenCliente();
        const ahora = new Date().toISOString();
        const tokenExpiracion = new Date(Date.now() + TOKEN_EXPIRACION_MS).toISOString();
        let nuevoUsuario = false;

        if (cliente) {
            // Usuario existente - actualizar token y último acceso
            await dbRun(
                'UPDATE clientes_app SET token_sesion = ?, token_expiracion = ?, ultimo_acceso = ? WHERE id = ?',
                [token, tokenExpiracion, ahora, cliente.id]
            );
            cliente = await dbGet('SELECT * FROM clientes_app WHERE id = ?', [cliente.id]);
        } else {
            // Usuario nuevo - crear cuenta
            nuevoUsuario = true;
            const result = await dbRun(
                `INSERT INTO clientes_app (google_id, email, nombre, foto_url, token_sesion, token_expiracion, fecha_registro, ultimo_acceso)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
                [googleUser.google_id, googleUser.email, googleUser.nombre, googleUser.foto_url, token, tokenExpiracion, ahora, ahora]
            );
            cliente = await dbGet('SELECT * FROM clientes_app WHERE id = ?', [result.lastID]);
        }
        
        res.json({
            mensaje: nuevoUsuario ? 'Cuenta creada exitosamente' : 'Inicio de sesión exitoso',
            token: token,
            nuevoUsuario: nuevoUsuario,
            usuario: {
                id: cliente.id,
                email: cliente.email,
                nombre: cliente.nombre,
                foto_url: cliente.foto_url,
                registroCompleto: cliente.registro_completo === 1
            }
        });
        
    } catch (error) {
        console.error('Error en login con Google:', error);
        res.status(500).json({ error: 'Error al procesar autenticación de Google' });
    }
});

// GET /api/cliente/auth/verificar - Verificar sesión activa
app.get('/api/cliente/auth/verificar', clienteAuth, (req, res) => {
    res.json({
        valido: true,
        registroCompleto: req.clienteApp.registro_completo === 1,
        usuario: {
            id: req.clienteApp.id,
            email: req.clienteApp.email,
            nombre: req.clienteApp.nombre,
            foto_url: req.clienteApp.foto_url
        }
    });
});

// POST /api/cliente/auth/logout - Cerrar sesión
app.post('/api/cliente/auth/logout', clienteAuth, async (req, res) => {
    try {
        await dbRun(
            'UPDATE clientes_app SET token_sesion = NULL WHERE id = ?',
            [req.clienteApp.id]
        );
        res.json({ mensaje: 'Sesión cerrada exitosamente' });
    } catch (error) {
        console.error('Error en logout:', error);
        res.status(500).json({ error: 'Error al cerrar sesión' });
    }
});

// PUT /api/cliente/perfil - Actualizar perfil del cliente
app.put('/api/cliente/perfil', clienteAuth, async (req, res) => {
    try {
        let { nombre, telefono, documento_tipo, documento_numero, pais, ciudad, direccion, fecha_nacimiento } = req.body;

        // Validaciones básicas
        if (!nombre || !telefono || !documento_tipo || !documento_numero || !pais) {
            return res.status(400).json({ error: 'Faltan campos obligatorios: nombre, teléfono, tipo de documento, número de documento y país' });
        }

        // Formatear RUT chileno al guardar: XX.XXX.XXX-X
        if (documento_tipo === 'rut' && documento_numero) {
            let limpio = documento_numero.replace(/[^0-9kK]/g, '');
            if (limpio.length > 1 && limpio.length <= 9) {
                const cuerpo = limpio.slice(0, -1);
                const dv = limpio.slice(-1).toUpperCase();
                let formateado = '';
                let j = 0;
                for (let i = cuerpo.length - 1; i >= 0; i--) {
                    formateado = cuerpo[i] + formateado;
                    j++;
                    if (j % 3 === 0 && i > 0) formateado = '.' + formateado;
                }
                documento_numero = formateado + '-' + dv;
            }
        }

        // Verificar que el documento no esté registrado por otro usuario
        const existeDocumento = await dbGet(
            'SELECT id FROM clientes_app WHERE documento_tipo = ? AND documento_numero = ? AND id != ?',
            [documento_tipo, documento_numero, req.clienteApp.id]
        );
        
        if (existeDocumento) {
            return res.status(400).json({ error: 'Este documento ya está registrado' });
        }
        
        await dbRun(
            `UPDATE clientes_app SET 
                nombre = ?, 
                telefono = ?, 
                documento_tipo = ?, 
                documento_numero = ?, 
                pais = ?, 
                ciudad = ?, 
                direccion = ?, 
                fecha_nacimiento = ?,
                registro_completo = 1
             WHERE id = ?`,
            [nombre, telefono, documento_tipo, documento_numero, pais, ciudad, direccion, fecha_nacimiento, req.clienteApp.id]
        );
        
        const clienteActualizado = await dbGet('SELECT * FROM clientes_app WHERE id = ?', [req.clienteApp.id]);
        
        res.json({
            mensaje: 'Perfil actualizado exitosamente',
            usuario: {
                id: clienteActualizado.id,
                email: clienteActualizado.email,
                nombre: clienteActualizado.nombre,
                foto_url: clienteActualizado.foto_url,
                telefono: clienteActualizado.telefono,
                documento_tipo: clienteActualizado.documento_tipo,
                documento_numero: clienteActualizado.documento_numero,
                pais: clienteActualizado.pais,
                ciudad: clienteActualizado.ciudad,
                direccion: clienteActualizado.direccion,
                fecha_nacimiento: clienteActualizado.fecha_nacimiento,
                registroCompleto: true
            }
        });
    } catch (error) {
        console.error('Error actualizando perfil:', error);
        res.status(500).json({ error: 'Error al actualizar perfil' });
    }
});

// GET /api/cliente/perfil - Obtener perfil del cliente
app.get('/api/cliente/perfil', clienteAuth, (req, res) => {
    const c = req.clienteApp;
    res.json({
        id: c.id,
        email: c.email,
        nombre: c.nombre,
        foto_url: c.foto_url,
        telefono: c.telefono,
        documento_tipo: c.documento_tipo,
        documento_numero: c.documento_numero,
        pais: c.pais,
        ciudad: c.ciudad,
        direccion: c.direccion,
        fecha_nacimiento: c.fecha_nacimiento,
        registroCompleto: c.registro_completo === 1,
        fechaRegistro: c.fecha_registro
    });
});

// GET /api/cliente/estadisticas - Obtener estadisticas del cliente
app.get('/api/cliente/estadisticas', clienteAuth, async (req, res) => {
    try {
        const clienteId = req.clienteApp.id;
        
        // Contar total de envios completados
        const stats = await dbGet(`
            SELECT 
                COUNT(*) as total_envios,
                COALESCE(SUM(monto_origen), 0) as total_enviado
            FROM solicitudes_transferencia 
            WHERE cliente_app_id = ? AND estado = 'completada'
        `, [clienteId]);
        
        res.json({
            total_envios: stats?.total_envios || 0,
            total_enviado: stats?.total_enviado || 0
        });
    } catch (error) {
        console.error('Error obteniendo estadisticas:', error);
        res.status(500).json({ error: 'Error al obtener estadisticas' });
    }
});

// =================================================================
// APP CLIENTE - SISTEMA DE REFERIDOS
// =================================================================

// Funcion para generar codigo de referido unico
function generarCodigoReferido(nombre) {
    const prefijo = nombre.substring(0, 3).toUpperCase().replace(/[^A-Z]/g, 'X');
    const random = crypto.randomBytes(3).toString('hex').toUpperCase();
    return `${prefijo}${random}`;
}

// GET /api/cliente/referidos/mi-codigo - Obtener o generar mi codigo de referido
app.get('/api/cliente/referidos/mi-codigo', clienteAuth, async (req, res) => {
    try {
        const clienteId = req.clienteApp.id;
        let cliente = await dbGet('SELECT codigo_referido, nombre FROM clientes_app WHERE id = ?', [clienteId]);
        
        // Si no tiene codigo, generarlo
        if (!cliente.codigo_referido) {
            let codigoUnico = false;
            let intentos = 0;
            let nuevoCodigo;
            
            while (!codigoUnico && intentos < 10) {
                nuevoCodigo = generarCodigoReferido(cliente.nombre || 'USR');
                const existe = await dbGet('SELECT id FROM clientes_app WHERE codigo_referido = ?', [nuevoCodigo]);
                if (!existe) {
                    codigoUnico = true;
                }
                intentos++;
            }
            
            if (!codigoUnico) {
                return res.status(500).json({ error: 'No se pudo generar codigo unico' });
            }
            
            await dbRun('UPDATE clientes_app SET codigo_referido = ? WHERE id = ?', [nuevoCodigo, clienteId]);
            cliente.codigo_referido = nuevoCodigo;
        }
        
        res.json({
            codigo: cliente.codigo_referido,
            link: `https://defioracle.com/registro?ref=${cliente.codigo_referido}`
        });
    } catch (error) {
        console.error('Error obteniendo codigo referido:', error);
        res.status(500).json({ error: 'Error al obtener codigo de referido' });
    }
});

// POST /api/cliente/referidos/aplicar - Aplicar codigo de referido (usuario nuevo)
app.post('/api/cliente/referidos/aplicar', clienteAuth, async (req, res) => {
    try {
        const { codigo } = req.body;
        const clienteId = req.clienteApp.id;
        
        if (!codigo) {
            return res.status(400).json({ error: 'Codigo de referido requerido' });
        }
        
        // Verificar que el usuario no tenga ya un referidor
        const cliente = await dbGet('SELECT referido_por, fecha_registro FROM clientes_app WHERE id = ?', [clienteId]);
        if (cliente.referido_por) {
            return res.status(400).json({ error: 'Ya tienes un codigo de referido aplicado' });
        }
        
        // Buscar el referidor por codigo
        const referidor = await dbGet('SELECT id, nombre FROM clientes_app WHERE codigo_referido = ? AND activo = 1', [codigo.toUpperCase()]);
        if (!referidor) {
            return res.status(404).json({ error: 'Codigo de referido no valido' });
        }
        
        // No puede referirse a si mismo
        if (referidor.id === clienteId) {
            return res.status(400).json({ error: 'No puedes usar tu propio codigo' });
        }
        
        const ahora = new Date().toISOString();
        
        // Calcular fecha limite (45 dias desde ahora)
        const fechaLimite = new Date();
        fechaLimite.setDate(fechaLimite.getDate() + 45);
        
        // Actualizar cliente con referidor
        await dbRun('UPDATE clientes_app SET referido_por = ?, fecha_referido = ? WHERE id = ?', 
            [referidor.id, ahora, clienteId]);
        
        // Crear registro de bono pendiente
        await dbRun(`
            INSERT INTO referidos_bonos (referidor_id, referido_id, fecha_inicio, fecha_limite)
            VALUES (?, ?, ?, ?)
        `, [referidor.id, clienteId, ahora, fechaLimite.toISOString()]);
        
        res.json({
            success: true,
            mensaje: `Codigo aplicado! Fuiste referido por ${referidor.nombre}`,
            referidor: referidor.nombre
        });
    } catch (error) {
        console.error('Error aplicando codigo referido:', error);
        res.status(500).json({ error: 'Error al aplicar codigo de referido' });
    }
});

// GET /api/cliente/referidos/mis-referidos - Ver mis referidos y estado de bonos
app.get('/api/cliente/referidos/mis-referidos', clienteAuth, async (req, res) => {
    try {
        const clienteId = req.clienteApp.id;
        
        // Obtener todos mis referidos con su estado de bono y si el cupon fue usado
        const referidos = await dbAll(`
            SELECT 
                c.id,
                c.nombre,
                c.email,
                c.fecha_referido,
                rb.id as bono_id,
                rb.monto_acumulado,
                rb.meta_monto,
                rb.estado as estado_bono,
                rb.fecha_limite,
                rb.bono_monto,
                rb.fecha_completado,
                rb.fecha_pagado,
                rb.codigo_cupon,
                rb.notas,
                (SELECT COUNT(*) FROM uso_codigos_promocionales ucp 
                 JOIN codigos_promocionales cp ON cp.id = ucp.codigo_id 
                 WHERE cp.codigo = rb.codigo_cupon) as cupon_usado
            FROM clientes_app c
            JOIN referidos_bonos rb ON rb.referido_id = c.id
            WHERE rb.referidor_id = ?
            ORDER BY c.fecha_referido DESC
        `, [clienteId]);
        
        // Calcular estadisticas
        const stats = {
            total_referidos: referidos.length,
            bonos_completados: referidos.filter(r => r.estado_bono === 'completado' || r.estado_bono === 'pagado').length,
            bonos_pendientes: referidos.filter(r => r.estado_bono === 'pendiente').length,
            bonos_expirados: referidos.filter(r => r.estado_bono === 'expirado').length,
            total_ganado: referidos.filter(r => r.estado_bono === 'pagado').reduce((sum, r) => sum + (r.bono_monto || 0), 0),
            total_por_cobrar: referidos.filter(r => r.estado_bono === 'completado' || r.estado_bono === 'reclamado').reduce((sum, r) => sum + (r.bono_monto || 0), 0)
        };
        
        res.json({
            referidos: referidos.map(r => ({
                id: r.id,
                bono_id: r.bono_id,
                nombre: r.nombre,
                fecha_referido: r.fecha_referido,
                monto_acumulado: r.monto_acumulado || 0,
                meta_monto: r.meta_monto || 100000,
                progreso: Math.min(100, ((r.monto_acumulado || 0) / (r.meta_monto || 100000)) * 100).toFixed(1),
                estado_bono: r.estado_bono,
                fecha_limite: r.fecha_limite,
                bono_monto: r.bono_monto || 10000,
                fecha_completado: r.fecha_completado,
                fecha_pagado: r.fecha_pagado,
                codigo_cupon: r.codigo_cupon,
                cupon_usado: r.cupon_usado > 0,
                notas: r.notas
            })),
            estadisticas: stats
        });
    } catch (error) {
        console.error('Error obteniendo referidos:', error);
        res.status(500).json({ error: 'Error al obtener referidos' });
    }
});

// GET /api/cliente/referidos/mi-referidor - Ver quien me refirio
app.get('/api/cliente/referidos/mi-referidor', clienteAuth, async (req, res) => {
    try {
        const cliente = await dbGet(`
            SELECT c.referido_por, r.nombre as referidor_nombre, c.fecha_referido
            FROM clientes_app c
            LEFT JOIN clientes_app r ON r.id = c.referido_por
            WHERE c.id = ?
        `, [req.clienteApp.id]);
        
        if (!cliente.referido_por) {
            return res.json({ tiene_referidor: false });
        }
        
        res.json({
            tiene_referidor: true,
            referidor: cliente.referidor_nombre,
            fecha: cliente.fecha_referido
        });
    } catch (error) {
        console.error('Error obteniendo referidor:', error);
        res.status(500).json({ error: 'Error al obtener informacion de referidor' });
    }
});

// POST /api/cliente/referidos/reclamar/:bonoId - Reclamar bono de referido
app.post('/api/cliente/referidos/reclamar/:bonoId', clienteAuth, async (req, res) => {
    try {
        const clienteId = req.clienteApp.id;
        const bonoId = req.params.bonoId;
        
        // Verificar que el bono existe y pertenece al cliente
        const bono = await dbGet(`
            SELECT rb.*, c.nombre as referido_nombre
            FROM referidos_bonos rb
            JOIN clientes_app c ON c.id = rb.referido_id
            WHERE rb.id = ? AND rb.referidor_id = ? AND rb.estado = 'completado'
        `, [bonoId, clienteId]);
        
        if (!bono) {
            return res.status(400).json({ error: 'Bono no encontrado o no disponible para reclamar' });
        }
        
        // Marcar como reclamado
        await dbRun(`UPDATE referidos_bonos SET estado = 'reclamado', fecha_reclamado = ? WHERE id = ?`, 
            [new Date().toISOString(), bonoId]);
        
        // Notificar a admin
        await enviarNotificacionTelegram(
            `&#127873; <b>SOLICITUD DE BONO DE REFERIDO</b>\n\n` +
            `&#128100; Solicitante: ${req.clienteApp.nombre}\n` +
            `&#128101; Referido: ${bono.referido_nombre}\n` +
            `&#128176; Monto: $${bono.bono_monto.toLocaleString('es-CL')} CLP\n\n` +
            `Revisar en panel de administracion`
        );
        
        res.json({ 
            success: true, 
            mensaje: 'Bono reclamado exitosamente. El equipo de administracion revisara tu solicitud.' 
        });
    } catch (error) {
        console.error('Error reclamando bono:', error);
        res.status(500).json({ error: 'Error al reclamar bono' });
    }
});

// Funcion para actualizar progreso de referidos cuando se completa un envio
async function actualizarProgresoReferido(clienteId, montoEnvio) {
    try {
        // Buscar si este cliente tiene un bono pendiente
        const bono = await dbGet(`
            SELECT rb.*, c.nombre as referidor_nombre
            FROM referidos_bonos rb
            JOIN clientes_app c ON c.id = rb.referidor_id
            WHERE rb.referido_id = ? AND rb.estado = 'pendiente'
        `, [clienteId]);
        
        if (!bono) return null;
        
        // Verificar si no ha expirado
        const ahora = new Date();
        const fechaLimite = new Date(bono.fecha_limite);
        
        if (ahora > fechaLimite) {
            // Marcar como expirado
            await dbRun('UPDATE referidos_bonos SET estado = ? WHERE id = ?', ['expirado', bono.id]);
            return { expirado: true };
        }
        
        // Actualizar monto acumulado
        const nuevoMonto = (bono.monto_acumulado || 0) + montoEnvio;
        await dbRun('UPDATE referidos_bonos SET monto_acumulado = ? WHERE id = ?', [nuevoMonto, bono.id]);
        
        // Verificar si alcanzo la meta
        if (nuevoMonto >= bono.meta_monto) {
            await dbRun('UPDATE referidos_bonos SET estado = ?, fecha_completado = ? WHERE id = ?', 
                ['completado', ahora.toISOString(), bono.id]);
            
            // Notificar por Telegram
            const cliente = await dbGet('SELECT nombre FROM clientes_app WHERE id = ?', [clienteId]);
            const mensaje = `&#127881; <b>BONO DE REFERIDO ACTIVADO!</b>\n\n` +
                `&#128100; Referidor: ${bono.referidor_nombre}\n` +
                `&#128101; Referido: ${cliente.nombre}\n` +
                `&#128176; Bono: $${bono.bono_monto.toLocaleString('es-CL')} CLP\n\n` +
                `El referido supero $${bono.meta_monto.toLocaleString('es-CL')} en envios.\n` +
                `Estado: Pendiente de pago`;
            
            await enviarNotificacionTelegram(mensaje);
            
            return { 
                completado: true, 
                bono_monto: bono.bono_monto,
                referidor: bono.referidor_nombre 
            };
        }
        
        return { 
            actualizado: true, 
            monto_acumulado: nuevoMonto,
            meta: bono.meta_monto,
            progreso: (nuevoMonto / bono.meta_monto * 100).toFixed(1)
        };
    } catch (error) {
        console.error('Error actualizando progreso referido:', error);
        return null;
    }
}

// =================================================================
// APP CLIENTE - VERIFICACION DE CUENTA
// =================================================================

// GET /api/cliente/verificacion/estado - Obtener estado de verificacion
app.get('/api/cliente/verificacion/estado', clienteAuth, async (req, res) => {
    try {
        const cliente = await dbGet(
            'SELECT verificacion_estado, verificacion_fecha_solicitud, verificacion_fecha_respuesta, verificacion_notas FROM clientes_app WHERE id = ?',
            [req.clienteApp.id]
        );
        
        res.json({
            estado: cliente?.verificacion_estado || 'no_verificado',
            fecha_solicitud: cliente?.verificacion_fecha_solicitud,
            fecha_respuesta: cliente?.verificacion_fecha_respuesta,
            notas: cliente?.verificacion_notas
        });
    } catch (error) {
        console.error('Error obteniendo estado verificacion:', error);
        res.status(500).json({ error: 'Error al obtener estado de verificacion' });
    }
});

// POST /api/cliente/verificacion/solicitar - Solicitar verificacion con documentos
app.post('/api/cliente/verificacion/solicitar', clienteAuth, upload.fields([
    { name: 'doc_frente', maxCount: 1 },
    { name: 'doc_reverso', maxCount: 1 }
]), async (req, res) => {
    try {
        const clienteId = req.clienteApp.id;
        
        // Verificar que se enviaron ambos documentos
        if (!req.files || !req.files['doc_frente'] || !req.files['doc_reverso']) {
            return res.status(400).json({ error: 'Debes enviar ambas caras del documento' });
        }

        const docFrente = req.files['doc_frente'][0];
        const docReverso = req.files['doc_reverso'][0];

        // Guardar archivos en carpeta uploads (usa DATA_DIR si esta configurado para disco persistente)
        const uploadsBaseDir = process.env.DATA_DIR || __dirname;
        const uploadsDir = path.join(uploadsBaseDir, 'uploads', 'verificaciones', clienteId.toString());
        if (!fs.existsSync(uploadsDir)) {
            fs.mkdirSync(uploadsDir, { recursive: true });
        }

        // Generar nombres seguros para los archivos (evitar path traversal)
        const frenteExt = path.extname(docFrente.originalname).toLowerCase().replace(/[^.a-z]/g, '');
        const reversoExt = path.extname(docReverso.originalname).toLowerCase().replace(/[^.a-z]/g, '');
        const frenteFilename = `frente_${Date.now()}${frenteExt || '.jpg'}`;
        const reversoFilename = `reverso_${Date.now()}${reversoExt || '.jpg'}`;

        fs.writeFileSync(path.join(uploadsDir, frenteFilename), docFrente.buffer);
        fs.writeFileSync(path.join(uploadsDir, reversoFilename), docReverso.buffer);

        const frenteUrl = `/uploads/verificaciones/${clienteId}/${frenteFilename}`;
        const reversoUrl = `/uploads/verificaciones/${clienteId}/${reversoFilename}`;

        // Actualizar estado en BD
        await dbRun(`
            UPDATE clientes_app SET 
                verificacion_estado = 'pendiente',
                verificacion_doc_frente = ?,
                verificacion_doc_reverso = ?,
                verificacion_fecha_solicitud = datetime('now')
            WHERE id = ?
        `, [frenteUrl, reversoUrl, clienteId]);

        // Obtener datos del cliente para notificar
        const cliente = await dbGet('SELECT nombre, email, documento_tipo, documento_numero, telefono FROM clientes_app WHERE id = ?', [clienteId]);

        // Notificar a Telegram
        const mensaje = ` <b>SOLICITUD DE VERIFICACION</b>\n\n` +
            ` <b>Cliente:</b> ${cliente.nombre}\n` +
            ` <b>Email:</b> ${cliente.email}\n` +
            ` <b>Telefono:</b> ${cliente.telefono || 'No registrado'}\n` +
            ` <b>Documento:</b> ${cliente.documento_tipo?.toUpperCase() || 'N/A'} ${cliente.documento_numero || 'N/A'}\n\n` +
            ` Documentos cargados y pendientes de revision.\n` +
            ` Ver en: ${process.env.BASE_URL || 'http://localhost:3000'}/home.html`;

        await enviarNotificacionTelegram(mensaje);

        res.json({ 
            success: true, 
            message: 'Documentos enviados correctamente. Te notificaremos cuando sean revisados.' 
        });
    } catch (error) {
        console.error('Error solicitando verificacion:', error);
        res.status(500).json({ error: 'Error al enviar documentos' });
    }
});

// =================================================================
// APP CLIENTE - BENEFICIARIOS
// =================================================================

// GET /api/cliente/beneficiarios - Listar beneficiarios del cliente
app.get('/api/cliente/beneficiarios', clienteAuth, async (req, res) => {
    try {
        const beneficiarios = await dbAll(
            'SELECT * FROM beneficiarios WHERE cliente_app_id = ? AND activo = 1 ORDER BY alias',
            [req.clienteApp.id]
        );
        res.json(beneficiarios);
    } catch (error) {
        console.error('Error obteniendo beneficiarios:', error);
        res.status(500).json({ error: 'Error al obtener beneficiarios' });
    }
});

// GET /api/cliente/beneficiarios/:id - Obtener un beneficiario concreto
app.get('/api/cliente/beneficiarios/:id', clienteAuth, async (req, res) => {
    try {
        const { id } = req.params;

        const beneficiario = await dbGet(
            'SELECT * FROM beneficiarios WHERE id = ? AND cliente_app_id = ? AND activo = 1',
            [id, req.clienteApp.id]
        );

        if (!beneficiario) {
            return res.status(404).json({ error: 'Beneficiario no encontrado', payload: null });
        }

        // Derivar nombres / apellidos a partir de nombre_completo para compatibilidad con el front
        const nombreCompleto = (beneficiario.nombre_completo || '').trim();
        const partesNombre = nombreCompleto.split(/\s+/);
        const nombres = partesNombre.shift() || nombreCompleto;
        const apellidos = partesNombre.join(' ');

        const payload = {
            ...beneficiario,
            nombres,
            apellidos,
            numero_documento: beneficiario.documento_numero,
            tipo_documento: beneficiario.documento_tipo,
            isFavorite: beneficiario.isFavorite === 1 || beneficiario.isFavorite === true
        };

        res.json({ ...payload, payload });
    } catch (error) {
        console.error('Error obteniendo beneficiario:', error);
        res.status(500).json({ error: 'Error al obtener beneficiario', payload: null });
    }
});

// POST /api/cliente/beneficiarios - Agregar beneficiario
app.post('/api/cliente/beneficiarios', clienteAuth, async (req, res) => {
    try {
        console.log(' Datos recibidos para nuevo beneficiario:', JSON.stringify(req.body, null, 2));
        
        // Sanitizar y validar todos los campos de entrada
        const alias = sanitizarTexto(req.body.alias, 100);
        const nombre_completo = sanitizarTexto(req.body.nombre_completo, 200);
        const documento_tipo = sanitizarTexto(req.body.documento_tipo, 20);
        const documento_numero = sanitizarTexto(req.body.documento_numero, 50);
        const tipo_cuenta = sanitizarTexto(req.body.tipo_cuenta, 50);
        const esPagoMovil = tipo_cuenta === 'pago_movil';
        const banco = sanitizarTexto(req.body.banco, 100) || (esPagoMovil ? 'Pago Móvil' : '');
        const numero_cuenta = sanitizarTexto(req.body.numero_cuenta, 50) || (esPagoMovil ? 'N/A' : '');
        const pais = sanitizarTexto(req.body.pais, 10);
        const telefono = req.body.telefono ? sanitizarTexto(req.body.telefono, 20) : null;
        const email = req.body.email ? sanitizarTexto(req.body.email, 255) : null;
        const isFavorite = req.body.isFavorite;

        if (!alias || !nombre_completo || !pais) {
            return res.status(400).json({ error: 'Faltan campos obligatorios' });
        }

        // Validar que tenga al menos un método de pago
        const tieneBanco = banco && numero_cuenta;
        const tienePagoMovil = telefono && validarTelefono(telefono);
        if (!tieneBanco && !tienePagoMovil) {
            return res.status(400).json({ error: 'Debe incluir datos bancarios o número de pago móvil' });
        }

        if (email && !validarEmail(email)) {
            return res.status(400).json({ error: 'Formato de email inválido' });
        }

        if (telefono && !validarTelefono(telefono)) {
            return res.status(400).json({ error: 'Formato de teléfono inválido' });
        }

        const ahora = new Date().toISOString();

        const result = await dbRun(
            `INSERT INTO beneficiarios (cliente_app_id, alias, nombre_completo, documento_tipo, documento_numero, banco, tipo_cuenta, numero_cuenta, pais, telefono, email, isFavorite, fecha_creacion)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [req.clienteApp.id, alias, nombre_completo, documento_tipo, documento_numero, banco, tipo_cuenta, numero_cuenta, pais, telefono, email, isFavorite ? 1 : 0, ahora]
        );
        
        const beneficiario = await dbGet('SELECT * FROM beneficiarios WHERE id = ?', [result.lastID]);
        
        res.json({
            mensaje: 'Beneficiario agregado exitosamente',
            beneficiario,
            payload: beneficiario
        });
    } catch (error) {
        console.error('Error agregando beneficiario:', error.message, error.stack);
        res.status(500).json({ error: 'Error al agregar beneficiario', detalle: error.message });
    }
});

// PUT /api/cliente/beneficiarios/:id - Actualizar beneficiario
app.put('/api/cliente/beneficiarios/:id', clienteAuth, async (req, res) => {
    try {
        const { id } = req.params;
        console.log(' Actualizando beneficiario ID:', id);
        console.log(' Datos recibidos:', JSON.stringify(req.body, null, 2));
        
        // Sanitizar campos
        const alias = sanitizarTexto(req.body.alias, 100);
        const nombre_completo = sanitizarTexto(req.body.nombre_completo, 200);
        const documento_tipo = sanitizarTexto(req.body.documento_tipo, 20);
        const documento_numero = sanitizarTexto(req.body.documento_numero, 50);
        const tipo_cuenta = sanitizarTexto(req.body.tipo_cuenta || '', 50);
        const esPagoMovil = tipo_cuenta === 'pago_movil';
        const banco = sanitizarTexto(req.body.banco || '', 100) || (esPagoMovil ? 'Pago Móvil' : '');
        const numero_cuenta = sanitizarTexto(req.body.numero_cuenta || '', 50) || (esPagoMovil ? 'N/A' : '');
        const pais = sanitizarTexto(req.body.pais, 10);
        const telefono = req.body.telefono ? sanitizarTexto(req.body.telefono, 20) : null;
        const email = req.body.email ? sanitizarTexto(req.body.email, 255) : null;
        const isFavorite = req.body.isFavorite;

        if (!alias || !nombre_completo || !pais) {
            return res.status(400).json({ error: 'Faltan campos obligatorios' });
        }

        // Validar que tenga al menos un método de pago
        const tieneBanco = banco && numero_cuenta;
        const tienePagoMovil = telefono && validarTelefono(telefono);
        if (!tieneBanco && !tienePagoMovil) {
            return res.status(400).json({ error: 'Debe incluir datos bancarios o número de pago móvil' });
        }

        if (email && !validarEmail(email)) {
            return res.status(400).json({ error: 'Formato de email inválido' });
        }

        // Verificar que el beneficiario pertenezca al cliente
        const beneficiario = await dbGet(
            'SELECT * FROM beneficiarios WHERE id = ? AND cliente_app_id = ?',
            [id, req.clienteApp.id]
        );
        
        if (!beneficiario) {
            return res.status(404).json({ error: 'Beneficiario no encontrado' });
        }
        
        const ahora = new Date().toISOString();
        
        await dbRun(
            `UPDATE beneficiarios SET 
                alias = ?, nombre_completo = ?, documento_tipo = ?, documento_numero = ?, 
                banco = ?, tipo_cuenta = ?, numero_cuenta = ?, pais = ?, telefono = ?, email = ?, isFavorite = ?, fecha_actualizacion = ?
             WHERE id = ?`,
            [alias, nombre_completo, documento_tipo, documento_numero, banco, tipo_cuenta, numero_cuenta, pais, telefono, email, isFavorite ? 1 : 0, ahora, id]
        );
        
        const beneficiarioActualizado = await dbGet('SELECT * FROM beneficiarios WHERE id = ?', [id]);
        
        res.json({
            mensaje: 'Beneficiario actualizado exitosamente',
            beneficiario: beneficiarioActualizado,
            payload: beneficiarioActualizado
        });
    } catch (error) {
        console.error('Error actualizando beneficiario:', error);
        res.status(500).json({ error: 'Error al actualizar beneficiario' });
    }
});

// PUT /api/cliente/beneficiarios/:id/favorito - Marcar/desmarcar favorito
app.put('/api/cliente/beneficiarios/:id/favorito', clienteAuth, async (req, res) => {
    try {
        const { id } = req.params;
        const { isFavorite } = req.body;

        const beneficiario = await dbGet(
            'SELECT * FROM beneficiarios WHERE id = ? AND cliente_app_id = ?',
            [id, req.clienteApp.id]
        );

        if (!beneficiario) {
            return res.status(404).json({ error: 'Beneficiario no encontrado' });
        }

        await dbRun('UPDATE beneficiarios SET isFavorite = ? WHERE id = ?', [isFavorite ? 1 : 0, id]);
        const actualizado = await dbGet('SELECT * FROM beneficiarios WHERE id = ?', [id]);
        res.json({ mensaje: 'Favorito actualizado', beneficiario: actualizado, payload: actualizado });
    } catch (error) {
        console.error('Error actualizando favorito:', error);
        res.status(500).json({ error: 'Error al actualizar favorito' });
    }
});

// DELETE /api/cliente/beneficiarios/:id - Eliminar beneficiario (soft delete)
app.delete('/api/cliente/beneficiarios/:id', clienteAuth, async (req, res) => {
    try {
        const { id } = req.params;
        
        const beneficiario = await dbGet(
            'SELECT * FROM beneficiarios WHERE id = ? AND cliente_app_id = ?',
            [id, req.clienteApp.id]
        );
        
        if (!beneficiario) {
            return res.status(404).json({ error: 'Beneficiario no encontrado' });
        }
        
        await dbRun('UPDATE beneficiarios SET activo = 0 WHERE id = ?', [id]);
        
        res.json({ mensaje: 'Beneficiario eliminado exitosamente' });
    } catch (error) {
        console.error('Error eliminando beneficiario:', error);
        res.status(500).json({ error: 'Error al eliminar beneficiario' });
    }
});

// =================================================================
// APP CLIENTE - CUENTAS DE PAGO
// =================================================================

// GET /api/cliente/cuentas-pago - Listar cuentas de pago disponibles
app.get('/api/cliente/cuentas-pago', clienteAuth, async (req, res) => {
    try {
        const cuentas = await dbAll(
            'SELECT id, nombre, banco, tipo_cuenta, numero_cuenta, titular, pais, moneda FROM cuentas_pago WHERE activo = 1 ORDER BY orden, nombre'
        );
        res.json(cuentas);
    } catch (error) {
        console.error('Error obteniendo cuentas de pago:', error);
        res.status(500).json({ error: 'Error al obtener cuentas de pago' });
    }
});

// =================================================================
// APP CLIENTE - VERSIÓN (para cache busting)
// =================================================================
const APP_VERSION = Date.now().toString();
app.get('/api/cliente/version', (req, res) => {
    res.json({ version: APP_VERSION });
});

// GET /api/cliente/tasa-bcv - Tasa de referencia USD/VES del BCV
app.get('/api/cliente/tasa-bcv', async (req, res) => {
    try {
        const [tasaRow, fechaRow] = await Promise.all([
            dbGet("SELECT valor FROM configuracion WHERE clave = 'tasaBCV'"),
            dbGet("SELECT valor FROM configuracion WHERE clave = 'tasaBCVFecha'")
        ]);
        const tasa = tasaRow ? parseFloat(tasaRow.valor) : 0;
        res.json({
            tasa: tasa > 0 ? tasa : null,
            fecha: fechaRow?.valor || null
        });
    } catch (error) {
        console.error('Error obteniendo tasa BCV:', error);
        res.status(500).json({ tasa: null, fecha: null });
    }
});

// =================================================================
// APP CLIENTE - TASAS Y COTIZACIONES
// =================================================================

// GET /api/cliente/tasa - Obtener tasas de venta automáticas por tramos
app.get('/api/cliente/tasa', async (req, res) => {
    try {
        // Verificar si la app usa tasas independientes o del sistema
        const modoRow = await dbGet("SELECT valor FROM configuracion WHERE clave = 'tasaAppModo'");
        const esIndependiente = modoRow?.valor === 'independiente';

        const [t1, t2, t3] = await Promise.all([
            dbGet(`SELECT valor FROM configuracion WHERE clave = '${esIndependiente ? 'tasaAppNivel1' : 'tasaNivel1'}'`),
            dbGet(`SELECT valor FROM configuracion WHERE clave = '${esIndependiente ? 'tasaAppNivel2' : 'tasaNivel2'}'`),
            dbGet(`SELECT valor FROM configuracion WHERE clave = '${esIndependiente ? 'tasaAppNivel3' : 'tasaNivel3'}'`)
        ]);
        
        // Tasas por tramos (CLP †' VES)
        const tasas = {
            tramos: [
                { minCLP: 5000, maxCLP: 99999, tasa: t1 ? parseFloat(t1.valor) : 0, label: '5.000 - 99.999 CLP' },
                { minCLP: 100000, maxCLP: 249999, tasa: t2 ? parseFloat(t2.valor) : 0, label: '100.000 - 249.999 CLP' },
                { minCLP: 250000, maxCLP: null, tasa: t3 ? parseFloat(t3.valor) : 0, label: '250.000+ CLP' }
            ],
            monedaOrigen: 'CLP',
            monedaDestino: 'VES',
            actualizacion: new Date().toISOString()
        };
        
        // Tasa por defecto (la del primer tramo)
        tasas.tasaDefecto = tasas.tramos[0].tasa;
        
        res.json(tasas);
    } catch (error) {
        console.error('Error obteniendo tasas:', error);
        res.status(500).json({ error: 'Error al obtener tasas' });
    }
});

// GET /api/cliente/estado-app - Verificar si la app está activa (público)
app.get('/api/cliente/estado-app', async (req, res) => {
    try {
        const row = await dbGet("SELECT valor FROM configuracion WHERE clave = 'app_activa'");
        res.json({ activa: row ? row.valor === '1' : true });
    } catch (error) {
        res.json({ activa: true }); // Fail-open: si hay error de BD, permitir operar
    }
});

// =================================================================
// APP CLIENTE - SOLICITUDES DE TRANSFERENCIA
// =================================================================

// POST /api/cliente/solicitudes - Crear nueva solicitud de transferencia
app.post('/api/cliente/solicitudes', clienteAuth, upload.single('comprobante'), async (req, res) => {
    try {
        // Verificar si la app está activa (Kill Switch Maestro)
        const estadoApp = await dbGet("SELECT valor FROM configuracion WHERE clave = 'app_activa'");
        if (estadoApp && estadoApp.valor === '0') {
            return res.status(503).json({
                error: 'app_desactivada',
                mensaje: 'En estos momentos no estamos procesando pedidos, mantente atento a nuestras redes para mantenerte informado'
            });
        }

        const clienteId = req.clienteId;
        const {
            beneficiario_id,
            monto_origen: body_monto_origen,
            moneda_origen = 'CLP',
            monto_destino: body_monto_destino,
            moneda_destino = 'VES',
            tasa_aplicada: body_tasa_aplicada,
            metodo_entrega,
            metodo_pago
        } = req.body;

        // Compatibilidad con payloads anteriores del cliente
        const monto_origen = body_monto_origen ?? req.body.monto_clp;
        const monto_destino = body_monto_destino ?? req.body.monto_ves;
        const tasa_aplicada = body_tasa_aplicada ?? req.body.tasa;

        // Validaciones
        if (!beneficiario_id || !monto_origen || !monto_destino) {
            return res.status(400).json({ error: 'Datos incompletos' });
        }

        if (!validarMontoPositivo(monto_origen) || !validarMontoPositivo(monto_destino)) {
            return res.status(400).json({ error: 'Los montos deben ser números positivos válidos' });
        }

        if (tasa_aplicada && !validarMontoPositivo(tasa_aplicada)) {
            return res.status(400).json({ error: 'La tasa debe ser un número positivo válido' });
        }

        if (!validarMoneda(moneda_origen) || !validarMoneda(moneda_destino)) {
            return res.status(400).json({ error: 'Moneda no soportada' });
        }

        if (!Number.isInteger(Number(beneficiario_id)) || Number(beneficiario_id) <= 0) {
            return res.status(400).json({ error: 'ID de beneficiario inválido' });
        }

        // Verificar que el beneficiario pertenece al cliente
        const beneficiario = await dbGet(
            `SELECT b.*, c.nombre as cliente_nombre, c.email as cliente_email,
                    c.telefono as cliente_telefono, c.documento_tipo as cliente_documento_tipo,
                    c.documento_numero as cliente_documento
             FROM beneficiarios b 
             JOIN clientes_app c ON b.cliente_app_id = c.id
             WHERE b.id = ? AND b.cliente_app_id = ? AND b.activo = 1`,
            [beneficiario_id, clienteId]
        );

        if (!beneficiario) {
            return res.status(404).json({ error: 'Beneficiario no encontrado' });
        }

        // Obtener cuenta de pago activa (la empresa)
        const cuentaPago = await dbGet(
            `SELECT * FROM cuentas_pago WHERE activo = 1 AND moneda = ? ORDER BY orden ASC LIMIT 1`,
            [moneda_origen]
        );

        // Protección contra solicitudes duplicadas (misma data en últimos 30 segundos)
        const hace30s = new Date(Date.now() - 30000).toISOString();
        const duplicada = await dbGet(
            `SELECT id FROM solicitudes_transferencia
             WHERE cliente_app_id = ? AND beneficiario_id = ? AND monto_origen = ? AND monto_destino = ?
             AND fecha_solicitud > ?`,
            [clienteId, beneficiario_id, monto_origen, monto_destino, hace30s]
        );
        if (duplicada) {
            return res.status(409).json({ error: 'Solicitud duplicada', solicitud_id: duplicada.id });
        }

        // Extraer información de cupón si viene
        const cuponCodigo = req.body.cupon_codigo || null;
        const cuponDescuentoCLP = req.body.cupon_descuento_clp || 0;
        const montoSinCupon = req.body.monto_sin_cupon || monto_origen;

        // Manejar comprobante de Caja Vecina si viene archivo
        let comprobanteUrl = null;
        let comprobanteBuffer = null;
        const esCajaVecina = metodo_pago === 'caja_vecina';

        if (req.file && esCajaVecina) {
            comprobanteBuffer = req.file.buffer;
            const uploadsBaseDir = process.env.DATA_DIR || __dirname;
            const uploadsDir = path.join(uploadsBaseDir, 'uploads', 'comprobantes', clienteId.toString());
            if (!fs.existsSync(uploadsDir)) {
                fs.mkdirSync(uploadsDir, { recursive: true });
            }
            const ext = path.extname(req.file.originalname).toLowerCase().replace(/[^.a-z]/g, '') || '.jpg';
            const filename = `comprobante_${Date.now()}${ext}`;
            fs.writeFileSync(path.join(uploadsDir, filename), req.file.buffer);
            comprobanteUrl = `/uploads/comprobantes/${clienteId}/${filename}`;
        }

        const estadoInicial = esCajaVecina && comprobanteUrl ? 'comprobante_enviado' : 'pendiente';

        // Crear la solicitud
        const fechaSolicitud = new Date().toISOString();
        const result = await dbRun(
            `INSERT INTO solicitudes_transferencia
             (cliente_app_id, beneficiario_id, cuenta_pago_id, monto_origen, moneda_origen,
              monto_destino, moneda_destino, tasa_aplicada, estado, fecha_solicitud,
              cupon_codigo, cupon_descuento_clp, monto_sin_cupon, comprobante_url)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [clienteId, beneficiario_id, cuentaPago?.id || 1, monto_origen, moneda_origen,
             monto_destino, moneda_destino, tasa_aplicada, estadoInicial, fechaSolicitud,
             cuponCodigo, cuponDescuentoCLP, montoSinCupon, comprobanteUrl]
        );

        const solicitudId = result.lastID;

        // Enviar notificación a Telegram y guardar message_id
        try {
            const telegramResult = await notificarNuevaSolicitud({
                id: solicitudId,
                cliente_nombre: beneficiario.cliente_nombre,
                cliente_email: beneficiario.cliente_email,
                cliente_telefono: beneficiario.cliente_telefono,
                cliente_documento_tipo: beneficiario.cliente_documento_tipo,
                cliente_documento: beneficiario.cliente_documento,
                monto_origen,
                moneda_origen,
                monto_destino,
                moneda_destino,
                tasa_aplicada,
                beneficiario_nombre: beneficiario.nombre_completo,
                beneficiario_banco: beneficiario.banco,
                beneficiario_cedula: beneficiario.documento_numero,
                beneficiario_tipo_cuenta: beneficiario.tipo_cuenta,
                beneficiario_cuenta: beneficiario.numero_cuenta,
                beneficiario_telefono: beneficiario.telefono,
                tipo_cuenta: metodo_entrega || beneficiario.tipo_cuenta,
                metodo_pago: metodo_pago || 'transferencia_bancaria'
            }, comprobanteBuffer);
            if (telegramResult && telegramResult.result && telegramResult.result.message_id) {
                await dbRun('UPDATE solicitudes_transferencia SET telegram_message_id = ? WHERE id = ?',
                    [telegramResult.result.message_id, solicitudId]);
            }
        } catch (e) { console.error('Error guardando telegram_message_id:', e.message); }

        // Devolver datos de la cuenta para pago
        res.status(201).json({
            solicitud_id: solicitudId,
            estado: estadoInicial,
            cuenta_pago: cuentaPago ? {
                banco: cuentaPago.banco,
                tipo_cuenta: cuentaPago.tipo_cuenta,
                numero_cuenta: cuentaPago.numero_cuenta,
                titular: cuentaPago.titular,
                rut: cuentaPago.rut_titular
            } : null,
            mensaje: 'Solicitud creada. Realiza la transferencia y sube el comprobante.'
        });

    } catch (error) {
        console.error('Error creando solicitud:', error);
        res.status(500).json({ error: 'Error al crear solicitud' });
    }
});

// PUT /api/cliente/solicitudes/:id/comprobante - Subir comprobante de pago
app.put('/api/cliente/solicitudes/:id/comprobante', clienteAuth, async (req, res) => {
    try {
        const { id } = req.params;
        const clienteId = req.clienteId;
        const { comprobante_url, referencia } = req.body;

        // Verificar que la solicitud pertenece al cliente
        const solicitud = await dbGet(
            `SELECT s.*, c.nombre as cliente_nombre 
             FROM solicitudes_transferencia s
             JOIN clientes_app c ON s.cliente_app_id = c.id
             WHERE s.id = ? AND s.cliente_app_id = ?`,
            [id, clienteId]
        );

        if (!solicitud) {
            return res.status(404).json({ error: 'Solicitud no encontrada' });
        }

        if (solicitud.estado !== 'pendiente') {
            return res.status(400).json({ error: 'La solicitud ya no puede modificarse' });
        }

        // Actualizar con comprobante
        await dbRun(
            `UPDATE solicitudes_transferencia 
             SET comprobante_url = ?, referencia = ?, estado = 'comprobante_enviado'
             WHERE id = ?`,
            [comprobante_url, referencia, id]
        );

        // " Notificar a Telegram
        await notificarCambioEstado({
            id,
            cliente_nombre: solicitud.cliente_nombre,
            monto_origen: solicitud.monto_origen,
            monto_destino: solicitud.monto_destino
        }, 'comprobante_enviado');

        res.json({ mensaje: 'Comprobante recibido. Estamos verificando tu pago.' });

    } catch (error) {
        console.error('Error subiendo comprobante:', error);
        res.status(500).json({ error: 'Error al subir comprobante' });
    }
});

// GET /api/cliente/solicitudes - Obtener historial de solicitudes del cliente
app.get('/api/cliente/solicitudes', clienteAuth, async (req, res) => {
    try {
        const clienteId = req.clienteId;
        
        const solicitudes = await dbAll(
            `SELECT s.*, 
                    s.fecha_solicitud as fecha_creacion,
                    b.alias, 
                    b.nombre_completo as beneficiario_nombre, 
                    b.banco as beneficiario_banco
             FROM solicitudes_transferencia s
             JOIN beneficiarios b ON s.beneficiario_id = b.id
             WHERE s.cliente_app_id = ?
             ORDER BY s.fecha_solicitud DESC
             LIMIT 50`,
            [clienteId]
        );

        res.json(solicitudes);
    } catch (error) {
        console.error('Error obteniendo solicitudes:', error);
        res.status(500).json({ error: 'Error al obtener solicitudes' });
    }
});

// GET /api/cliente/solicitudes/:id - Obtener detalle de una solicitud
app.get('/api/cliente/solicitudes/:id', clienteAuth, async (req, res) => {
    try {
        const { id } = req.params;
        const clienteId = req.clienteId;

        const solicitud = await dbGet(
            `SELECT s.*, 
                    s.fecha_solicitud as fecha_creacion,
                    b.alias, 
                    b.nombre_completo as beneficiario_nombre, 
                    b.banco as beneficiario_banco, 
                    b.numero_cuenta as beneficiario_cuenta,
                    cp.banco as cuenta_pago_banco, cp.numero_cuenta as cuenta_pago_numero,
                    cp.titular as cuenta_pago_titular, cp.rut_titular as cuenta_pago_rut
             FROM solicitudes_transferencia s
             JOIN beneficiarios b ON s.beneficiario_id = b.id
             LEFT JOIN cuentas_pago cp ON s.cuenta_pago_id = cp.id
             WHERE s.id = ? AND s.cliente_app_id = ?`,
            [id, clienteId]
        );

        if (!solicitud) {
            return res.status(404).json({ error: 'Solicitud no encontrada' });
        }

        res.json(solicitud);
    } catch (error) {
        console.error('Error obteniendo solicitud:', error);
        res.status(500).json({ error: 'Error al obtener solicitud' });
    }
});

// =================================================================
// API OPERADORES - GESTI'N DE SOLICITUDES APP
// =================================================================

// GET /api/solicitudes-app - Listar solicitudes de la app (para operadores)
app.get('/api/solicitudes-app', apiAuth, async (req, res) => {
    try {
        const { estado, limit = 50, offset = 0 } = req.query;
        
        let query = `
            SELECT s.*,
                   c.nombre as cliente_nombre, c.email as cliente_email, c.telefono as cliente_telefono,
                   c.documento_tipo as cliente_documento_tipo, c.documento_numero as cliente_documento_numero,
                   b.alias, b.nombre_completo as beneficiario_nombre, b.banco as banco_destino, b.numero_cuenta as cuenta_destino,
                   b.documento_tipo as beneficiario_documento_tipo, b.documento_numero as beneficiario_documento,
                   b.tipo_cuenta as tipo_cuenta_destino, b.telefono as beneficiario_telefono,
                   u.username as operador_nombre,
                   s.fecha_tomado, s.tomado_por_nombre,
                   s.tasa_aplicada as tasa, s.cupon_codigo, s.cupon_descuento_clp
            FROM solicitudes_transferencia s
            JOIN clientes_app c ON s.cliente_app_id = c.id
            JOIN beneficiarios b ON s.beneficiario_id = b.id
            LEFT JOIN usuarios u ON s.operador_id = u.id
        `;
        const params = [];

        if (estado) {
            query += ` WHERE s.estado = ?`;
            params.push(estado);
        }

        query += ` ORDER BY s.fecha_solicitud DESC LIMIT ? OFFSET ?`;
        params.push(parseInt(limit), parseInt(offset));

        const solicitudes = await dbAll(query, params);
        
        // Contar por estado
        const conteos = await dbGet(`
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN estado = 'pendiente' THEN 1 ELSE 0 END) as pendientes,
                SUM(CASE WHEN estado = 'comprobante_enviado' THEN 1 ELSE 0 END) as con_comprobante,
                SUM(CASE WHEN estado = 'verificando' THEN 1 ELSE 0 END) as verificando,
                SUM(CASE WHEN estado = 'procesando' THEN 1 ELSE 0 END) as procesando,
                SUM(CASE WHEN estado = 'completada' THEN 1 ELSE 0 END) as completadas,
                SUM(CASE WHEN estado IN ('pendiente', 'comprobante_enviado') 
                    AND datetime(fecha_solicitud) <= datetime('now', '-15 minutes') THEN 1 ELSE 0 END) as urgentes
            FROM solicitudes_transferencia
        `);

        res.json({ solicitudes, conteos });
    } catch (error) {
        console.error('Error obteniendo solicitudes app:', error);
        res.status(500).json({ error: 'Error al obtener solicitudes' });
    }
});

// GET /api/solicitudes-app/stats/volumen-mensual - Volumen de ventas por mes (pedidos completados)
// NOTA: Esta ruta DEBE ir ANTES de las rutas con :id para que Express no la confunda
app.get('/api/solicitudes-app/stats/volumen-mensual', apiAuth, async (req, res) => {
    try {
        // Obtener volumen mensual de los últimos 12 meses
        const stats = await dbAll(`
            SELECT
                strftime('%Y-%m', fecha_completada) as mes,
                strftime('%Y', fecha_completada) as anio,
                strftime('%m', fecha_completada) as mes_num,
                COUNT(*) as cantidad_pedidos,
                SUM(monto_origen) as volumen_clp,
                SUM(monto_destino) as volumen_destino,
                AVG(monto_origen) as promedio_clp
            FROM solicitudes_transferencia
            WHERE estado = 'completada'
              AND fecha_completada IS NOT NULL
              AND fecha_completada >= date('now', '-12 months')
            GROUP BY strftime('%Y-%m', fecha_completada)
            ORDER BY mes DESC
        `);

        // Nombres de meses en español
        const nombresMeses = ['', 'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                             'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'];

        const resultado = stats.map(s => ({
            mes: s.mes,
            mes_nombre: nombresMeses[parseInt(s.mes_num)] + ' ' + s.anio,
            cantidad_pedidos: s.cantidad_pedidos,
            volumen_clp: Math.round(s.volumen_clp || 0),
            volumen_destino: Math.round(s.volumen_destino || 0),
            promedio_clp: Math.round(s.promedio_clp || 0)
        }));

        // Totales generales
        const totales = await dbGet(`
            SELECT
                COUNT(*) as total_pedidos,
                SUM(monto_origen) as total_clp,
                SUM(monto_destino) as total_destino
            FROM solicitudes_transferencia
            WHERE estado = 'completada'
        `);

        res.json({
            meses: resultado,
            totales: {
                total_pedidos: totales?.total_pedidos || 0,
                total_clp: Math.round(totales?.total_clp || 0),
                total_destino: Math.round(totales?.total_destino || 0)
            }
        });
    } catch (error) {
        console.error('Error obteniendo volumen mensual:', error);
        res.status(500).json({ error: 'Error al obtener estadísticas' });
    }
});

// PUT /api/solicitudes-app/:id/estado - Actualizar estado de solicitud (operadores)
app.put('/api/solicitudes-app/:id/estado', apiAuth, async (req, res) => {
    try {
        const { id } = req.params;
        const { estado, notas_operador, operacion_id } = req.body;
        const operadorId = req.session.user?.id;
        const operadorNombre = req.session.user?.username || 'Operador';
        
        console.log('📋 Cambio estado pedido:', { operadorId, operadorNombre, estado, id });

        const solicitud = await dbGet(
            `SELECT s.*, c.nombre as cliente_nombre
             FROM solicitudes_transferencia s
             JOIN clientes_app c ON s.cliente_app_id = c.id
             WHERE s.id = ?`,
            [id]
        );

        if (!solicitud) {
            return res.status(404).json({ error: 'Solicitud no encontrada' });
        }

        // Campos a actualizar según el estado
        let updateFields = ['estado = ?', 'operador_id = ?'];
        let updateParams = [estado, operadorId];

        if (notas_operador) {
            updateFields.push('notas_operador = ?');
            updateParams.push(notas_operador);
        }

        if (operacion_id) {
            updateFields.push('operacion_id = ?');
            updateParams.push(operacion_id);
        }

        if (estado === 'verificando') {
            updateFields.push('fecha_verificacion = ?');
            updateParams.push(new Date().toISOString());
        }

        // Cuando se toma el pedido (procesando), guardar fecha y nombre del operador
        if (estado === 'procesando') {
            // Siempre actualizar quién tomó el pedido
            updateFields.push('tomado_por_nombre = ?');
            updateParams.push(operadorNombre);
            
            // Solo guardar fecha_tomado si no existe
            if (!solicitud.fecha_tomado) {
                updateFields.push('fecha_tomado = ?');
                updateParams.push(new Date().toISOString());
            }
        }

        if (estado === 'completada') {
            updateFields.push('fecha_completada = ?');
            updateParams.push(new Date().toISOString());
        }

        updateParams.push(id);

        await dbRun(
            `UPDATE solicitudes_transferencia SET ${updateFields.join(', ')} WHERE id = ?`,
            updateParams
        );

        // Actualizar progreso de referido si se completa la solicitud
        if (estado === 'completada' && solicitud.cliente_app_id && solicitud.monto_origen) {
            await actualizarProgresoReferido(solicitud.cliente_app_id, solicitud.monto_origen);
        }

        // " Notificar cambio de estado a Telegram
        await notificarCambioEstado({
            id,
            cliente_nombre: solicitud.cliente_nombre,
            monto_origen: solicitud.monto_origen,
            monto_destino: solicitud.monto_destino
        }, estado);

        res.json({ mensaje: 'Estado actualizado correctamente' });
    } catch (error) {
        console.error('Error actualizando solicitud:', error);
        res.status(500).json({ error: 'Error al actualizar solicitud' });
    }
});

// PATCH /api/solicitudes-app/:id - Actualizar estado y notas (usado por pedidos-app.html)
app.patch('/api/solicitudes-app/:id', apiAuth, async (req, res) => {
    try {
        const { id } = req.params;
        const { estado, notas_operador } = req.body;
        const operadorId = req.session.user?.id;
        const operadorNombre = req.session.user?.username || 'Operador';

        console.log('📋 PATCH cambio estado pedido:', { operadorId, operadorNombre, estado, id });

        const solicitud = await dbGet(
            `SELECT s.*, c.nombre as cliente_nombre
             FROM solicitudes_transferencia s
             JOIN clientes_app c ON s.cliente_app_id = c.id
             WHERE s.id = ?`,
            [id]
        );

        if (!solicitud) {
            return res.status(404).json({ error: 'Solicitud no encontrada' });
        }

        // Campos a actualizar
        let updateFields = ['operador_id = ?'];
        let updateParams = [operadorId];

        if (estado) {
            updateFields.push('estado = ?');
            updateParams.push(estado);

            if (estado === 'verificando') {
                updateFields.push('fecha_verificacion = ?');
                updateParams.push(new Date().toISOString());
            }

            if (estado === 'procesando') {
                updateFields.push('tomado_por_nombre = ?');
                updateParams.push(operadorNombre);
                if (!solicitud.fecha_tomado) {
                    updateFields.push('fecha_tomado = ?');
                    updateParams.push(new Date().toISOString());
                }
            }

            if (estado === 'completada') {
                updateFields.push('fecha_completada = ?');
                updateParams.push(new Date().toISOString());
            }
        }

        if (notas_operador !== undefined) {
            updateFields.push('notas_operador = ?');
            updateParams.push(notas_operador);
        }

        updateParams.push(id);

        await dbRun(
            `UPDATE solicitudes_transferencia SET ${updateFields.join(', ')} WHERE id = ?`,
            updateParams
        );

        // Actualizar progreso de referido si se completa
        if (estado === 'completada' && solicitud.cliente_app_id && solicitud.monto_origen) {
            await actualizarProgresoReferido(solicitud.cliente_app_id, solicitud.monto_origen);
        }

        // Notificar cambio de estado a Telegram
        if (estado) {
            await notificarCambioEstado({
                id,
                cliente_nombre: solicitud.cliente_nombre,
                monto_origen: solicitud.monto_origen,
                monto_destino: solicitud.monto_destino
            }, estado);
        }

        res.json({ mensaje: 'Cambios guardados correctamente' });
    } catch (error) {
        console.error('Error actualizando solicitud (PATCH):', error);
        res.status(500).json({ error: 'Error al guardar cambios' });
    }
});

// POST /api/solicitudes-app/:id/tomar - Tomar un pedido (asignarse como operador)
app.post('/api/solicitudes-app/:id/tomar', apiAuth, async (req, res) => {
    try {
        const { id } = req.params;
        const operadorId = req.session.user?.id;
        const operadorNombre = req.session.user?.username || 'Operador';

        console.log('📋 Tomando pedido:', { operadorId, operadorNombre, id });

        const solicitud = await dbGet(
            `SELECT s.*, c.nombre as cliente_nombre
             FROM solicitudes_transferencia s
             JOIN clientes_app c ON s.cliente_app_id = c.id
             WHERE s.id = ?`,
            [id]
        );

        if (!solicitud) {
            return res.status(404).json({ error: 'Solicitud no encontrada' });
        }

        if (solicitud.estado !== 'pendiente' && solicitud.estado !== 'comprobante_enviado') {
            return res.status(400).json({ error: 'Este pedido ya fue tomado o procesado' });
        }

        await dbRun(
            `UPDATE solicitudes_transferencia
             SET estado = 'procesando',
                 operador_id = ?,
                 tomado_por_nombre = ?,
                 fecha_tomado = ?
             WHERE id = ?`,
            [operadorId, operadorNombre, new Date().toISOString(), id]
        );

        // Notificar a Telegram
        await notificarCambioEstado({
            id,
            cliente_nombre: solicitud.cliente_nombre,
            monto_origen: solicitud.monto_origen,
            monto_destino: solicitud.monto_destino
        }, 'procesando');

        res.json({ mensaje: 'Pedido tomado correctamente' });
    } catch (error) {
        console.error('Error tomando pedido:', error);
        res.status(500).json({ error: 'Error al tomar pedido' });
    }
});

// POST /api/solicitudes-app/:id/completar - Marcar pedido como pagado/completado
app.post('/api/solicitudes-app/:id/completar', apiAuth, async (req, res) => {
    try {
        const { id } = req.params;
        const operadorId = req.session.user?.id;
        const operadorNombre = req.session.user?.username || 'Operador';

        console.log('✅ Completando pedido:', { operadorId, operadorNombre, id });

        const solicitud = await dbGet(
            `SELECT s.*, c.nombre as cliente_nombre
             FROM solicitudes_transferencia s
             JOIN clientes_app c ON s.cliente_app_id = c.id
             WHERE s.id = ?`,
            [id]
        );

        if (!solicitud) {
            return res.status(404).json({ error: 'Solicitud no encontrada' });
        }

        if (solicitud.estado === 'completada') {
            return res.status(400).json({ error: 'Este pedido ya está completado' });
        }

        if (solicitud.estado === 'cancelada' || solicitud.estado === 'rechazada') {
            return res.status(400).json({ error: 'No se puede completar un pedido cancelado o rechazado' });
        }

        await dbRun(
            `UPDATE solicitudes_transferencia
             SET estado = 'completada',
                 operador_id = ?,
                 fecha_completada = ?
             WHERE id = ?`,
            [operadorId, new Date().toISOString(), id]
        );

        // Actualizar progreso de referido
        if (solicitud.cliente_app_id && solicitud.monto_origen) {
            await actualizarProgresoReferido(solicitud.cliente_app_id, solicitud.monto_origen);
        }

        // Notificar a Telegram
        await notificarCambioEstado({
            id,
            cliente_nombre: solicitud.cliente_nombre,
            monto_origen: solicitud.monto_origen,
            monto_destino: solicitud.monto_destino
        }, 'completada');

        res.json({ mensaje: 'Pedido completado correctamente' });
    } catch (error) {
        console.error('Error completando pedido:', error);
        res.status(500).json({ error: 'Error al completar pedido' });
    }
});

// POST /api/solicitudes-app/:id/cancelar - Cancelar un pedido
app.post('/api/solicitudes-app/:id/cancelar', apiAuth, async (req, res) => {
    try {
        const { id } = req.params;
        const { motivo } = req.body;
        const operadorId = req.session.user?.id;
        const operadorNombre = req.session.user?.username || 'Operador';

        console.log('❌ Cancelando pedido:', { operadorId, operadorNombre, id, motivo });

        const solicitud = await dbGet(
            `SELECT s.*, c.nombre as cliente_nombre
             FROM solicitudes_transferencia s
             JOIN clientes_app c ON s.cliente_app_id = c.id
             WHERE s.id = ?`,
            [id]
        );

        if (!solicitud) {
            return res.status(404).json({ error: 'Solicitud no encontrada' });
        }

        if (solicitud.estado === 'completada') {
            return res.status(400).json({ error: 'No se puede cancelar un pedido ya completado' });
        }

        if (solicitud.estado === 'cancelada') {
            return res.status(400).json({ error: 'Este pedido ya está cancelado' });
        }

        let notasActualizadas = solicitud.notas_operador || '';
        if (motivo) {
            notasActualizadas += (notasActualizadas ? '\n' : '') + `[Cancelado por ${operadorNombre}]: ${motivo}`;
        }

        await dbRun(
            `UPDATE solicitudes_transferencia
             SET estado = 'cancelada',
                 operador_id = ?,
                 notas_operador = ?
             WHERE id = ?`,
            [operadorId, notasActualizadas, id]
        );

        // Notificar a Telegram
        await notificarCambioEstado({
            id,
            cliente_nombre: solicitud.cliente_nombre,
            monto_origen: solicitud.monto_origen,
            monto_destino: solicitud.monto_destino
        }, 'cancelada');

        res.json({ mensaje: 'Pedido cancelado correctamente' });
    } catch (error) {
        console.error('Error cancelando pedido:', error);
        res.status(500).json({ error: 'Error al cancelar pedido' });
    }
});

// =================================================================
// FIN: APP CLIENTE M"VIL
// =================================================================

// =================================================================
// ENDPOINTS DE BONOS DE REFERIDOS (ADMIN)
// =================================================================

// GET /api/bonos-referidos - Listar todos los bonos de referidos
app.get('/api/bonos-referidos', apiAuth, async (req, res) => {
    try {
        const { estado } = req.query;
        let query = `
            SELECT 
                rb.*,
                referidor.nombre as referidor_nombre,
                referidor.email as referidor_email,
                referido.nombre as referido_nombre,
                referido.email as referido_email
            FROM referidos_bonos rb
            JOIN clientes_app referidor ON referidor.id = rb.referidor_id
            JOIN clientes_app referido ON referido.id = rb.referido_id
        `;
        const params = [];
        
        if (estado) {
            query += ' WHERE rb.estado = ?';
            params.push(estado);
        }
        
        query += ' ORDER BY rb.fecha_reclamado DESC, rb.fecha_completado DESC';
        
        const bonos = await dbAll(query, params);
        
        // Contar por estado
        const stats = await dbGet(`
            SELECT 
                SUM(CASE WHEN estado = 'pendiente' THEN 1 ELSE 0 END) as pendientes,
                SUM(CASE WHEN estado = 'completado' THEN 1 ELSE 0 END) as completados,
                SUM(CASE WHEN estado = 'reclamado' THEN 1 ELSE 0 END) as reclamados,
                SUM(CASE WHEN estado = 'pagado' THEN 1 ELSE 0 END) as pagados,
                SUM(CASE WHEN estado = 'expirado' THEN 1 ELSE 0 END) as expirados,
                SUM(CASE WHEN estado = 'cancelado' THEN 1 ELSE 0 END) as cancelados
            FROM referidos_bonos
        `);
        
        res.json({ bonos, estadisticas: stats });
    } catch (error) {
        console.error('Error listando bonos referidos:', error);
        res.status(500).json({ error: 'Error al listar bonos' });
    }
});

// POST /api/bonos-referidos/:id/aprobar - Aprobar bono y generar cupon
app.post('/api/bonos-referidos/:id/aprobar', apiAuth, async (req, res) => {
    try {
        const bonoId = req.params.id;
        
        // Obtener el bono
        const bono = await dbGet(`
            SELECT rb.*, c.nombre as referidor_nombre, c.email as referidor_email
            FROM referidos_bonos rb
            JOIN clientes_app c ON c.id = rb.referidor_id
            WHERE rb.id = ? AND rb.estado IN ('completado', 'reclamado')
        `, [bonoId]);
        
        if (!bono) {
            return res.status(400).json({ error: 'Bono no encontrado o no esta disponible para aprobar' });
        }
        
        // Generar codigo de cupon unico
        const codigoCupon = 'BONO-' + bono.referidor_id + '-' + Date.now().toString(36).toUpperCase();
        
        // Crear el cupon de descuento en CLP
        await dbRun(`
            INSERT INTO codigos_promocionales (
                codigo, descripcion, tasa_especial, tipo_descuento, monto_descuento_clp,
                usos_maximos, cliente_exclusivo_id, bono_referido_id, activo, fecha_creacion
            ) VALUES (?, ?, 0, 'monto_clp', ?, 1, ?, ?, 1, datetime('now'))
        `, [
            codigoCupon,
            `Bono por referir a ${bono.referido_nombre || 'usuario'}`,
            bono.bono_monto,
            bono.referidor_id,
            bonoId
        ]);
        
        // Actualizar el bono como pagado
        await dbRun(`
            UPDATE referidos_bonos 
            SET estado = 'pagado', fecha_pagado = ?, codigo_cupon = ?
            WHERE id = ?
        `, [new Date().toISOString(), codigoCupon, bonoId]);
        
        // Notificar al usuario (aqui podria enviar email o notificacion push)
        console.log(`Cupon ${codigoCupon} generado para cliente ${bono.referidor_nombre} (ID: ${bono.referidor_id})`);
        
        res.json({ 
            success: true, 
            mensaje: 'Bono aprobado y cupon generado',
            codigo_cupon: codigoCupon,
            monto: bono.bono_monto
        });
    } catch (error) {
        console.error('Error aprobando bono:', error);
        res.status(500).json({ error: 'Error al aprobar bono' });
    }
});

// POST /api/bonos-referidos/:id/rechazar - Rechazar/cancelar bono
app.post('/api/bonos-referidos/:id/rechazar', apiAuth, async (req, res) => {
    try {
        const bonoId = req.params.id;
        const { motivo } = req.body;
        
        // Obtener el bono
        const bono = await dbGet(`
            SELECT rb.*, c.nombre as referidor_nombre
            FROM referidos_bonos rb
            JOIN clientes_app c ON c.id = rb.referidor_id
            WHERE rb.id = ? AND rb.estado IN ('completado', 'reclamado')
        `, [bonoId]);
        
        if (!bono) {
            return res.status(400).json({ error: 'Bono no encontrado o no esta disponible para cancelar' });
        }
        
        // Marcar como cancelado
        await dbRun(`
            UPDATE referidos_bonos 
            SET estado = 'cancelado', notas = ?
            WHERE id = ?
        `, [motivo || 'Cancelado por motivos internos', bonoId]);
        
        res.json({ 
            success: true, 
            mensaje: 'Bono cancelado correctamente'
        });
    } catch (error) {
        console.error('Error rechazando bono:', error);
        res.status(500).json({ error: 'Error al rechazar bono' });
    }
});

// Iniciar el servidor solo después de que las migraciones se hayan completado

// =================================================================
// ENDPOINTS PARA USUARIOS APP CLIENTE (ADMIN)
// =================================================================

// GET /api/clientes-app - Listar usuarios de la app cliente
app.get('/api/clientes-app', apiAuth, async (req, res) => {
    try {
        const page = parseInt(req.query.page || '1', 10);
        const limit = parseInt(req.query.limit || '50', 10);
        const search = req.query.search || '';
        const estado = req.query.estado || '';
        const offset = (page - 1) * limit;

        let whereClause = 'WHERE 1=1';
        const params = [];

        if (search) {
            whereClause += ' AND (nombre LIKE ? OR email LIKE ? OR documento_numero LIKE ?)';
            params.push('%' + search + '%', '%' + search + '%', '%' + search + '%');
        }

        if (estado) {
            whereClause += ' AND verificacion_estado = ?';
            params.push(estado);
        }

        const countResult = await dbGet('SELECT COUNT(*) as total FROM clientes_app ' + whereClause, params);
        const total = countResult?.total || 0;

        const clientes = await dbAll(
            'SELECT * FROM clientes_app ' + whereClause + ' ORDER BY CASE WHEN verificacion_estado = \'pendiente\' THEN 0 ELSE 1 END, fecha_registro DESC LIMIT ? OFFSET ?',
            [...params, limit, offset]
        );

        res.json({ clientes, total, page, limit, totalPages: Math.ceil(total / limit) });
    } catch (error) {
        console.error('Error listando clientes app:', error);
        res.status(500).json({ error: 'Error al listar clientes de la app' });
    }
});

// GET /api/clientes-app/pendientes/count
app.get('/api/clientes-app/pendientes/count', apiAuth, async (req, res) => {
    try {
        const result = await dbGet("SELECT COUNT(*) as count FROM clientes_app WHERE verificacion_estado = 'pendiente'");
        res.json({ count: result?.count || 0 });
    } catch (error) {
        res.status(500).json({ error: 'Error al contar pendientes' });
    }
});

// GET /api/clientes-app/:id
app.get('/api/clientes-app/:id', apiAuth, async (req, res) => {
    try {
        const cliente = await dbGet('SELECT * FROM clientes_app WHERE id = ?', [req.params.id]);
        if (!cliente) return res.status(404).json({ error: 'Cliente no encontrado' });

        const stats = await dbGet(
            "SELECT COUNT(*) as total_envios, COALESCE(SUM(monto_origen), 0) as total_enviado FROM solicitudes_transferencia WHERE cliente_app_id = ? AND estado = 'completada'",
            [req.params.id]
        );

        res.json({ ...cliente, estadisticas: stats });
    } catch (error) {
        res.status(500).json({ error: 'Error al obtener cliente' });
    }
});

// PUT /api/clientes-app/:id/verificacion
app.put('/api/clientes-app/:id/verificacion', apiAuth, async (req, res) => {
    try {
        const { accion, notas } = req.body;
        if (!['aprobar', 'rechazar', 'revocar'].includes(accion)) {
            return res.status(400).json({ error: 'Accion invalida' });
        }

        if (accion === 'revocar') {
            await dbRun(
                `UPDATE clientes_app SET
                    verificacion_estado = 'no_verificado',
                    verificacion_doc_frente = NULL,
                    verificacion_doc_reverso = NULL,
                    verificacion_fecha_solicitud = NULL,
                    verificacion_fecha_respuesta = NULL,
                    verificacion_notas = ?
                WHERE id = ?`,
                [notas || 'Verificación revocada por administrador', req.params.id]
            );
            return res.json({ success: true, estado: 'no_verificado' });
        }

        const nuevoEstado = accion === 'aprobar' ? 'verificado' : 'rechazado';

        await dbRun(
            "UPDATE clientes_app SET verificacion_estado = ?, verificacion_fecha_respuesta = datetime('now'), verificacion_notas = ? WHERE id = ?",
            [nuevoEstado, notas || null, req.params.id]
        );

        res.json({ success: true, estado: nuevoEstado });
    } catch (error) {
        res.status(500).json({ error: 'Error al actualizar verificacion' });
    }
});

// DELETE /api/clientes-app/:id - Eliminar usuario de la app
app.delete('/api/clientes-app/:id', apiAuth, async (req, res) => {
    try {
        const cliente = await dbGet("SELECT id, nombre, email FROM clientes_app WHERE id = ?", [req.params.id]);
        if (!cliente) {
            return res.status(404).json({ error: 'Usuario no encontrado' });
        }

        await dbRun("DELETE FROM clientes_app WHERE id = ?", [req.params.id]);
        console.log(`Usuario App eliminado: ${cliente.nombre} (${cliente.email})`);
        res.json({ success: true, mensaje: `Usuario ${cliente.nombre} eliminado correctamente` });
    } catch (error) {
        console.error('Error al eliminar usuario app:', error);
        res.status(500).json({ error: 'Error al eliminar usuario' });
    }
});

// =================================================================
// ENDPOINTS DE BONOS DE REFERIDOS (ADMIN)
// =================================================================

// GET /api/referidos/bonos - Listar todos los bonos (admin)
app.get('/api/referidos/bonos', apiAuth, async (req, res) => {
    try {
        const { estado } = req.query;
        
        let query = `
            SELECT 
                rb.*,
                referidor.nombre as referidor_nombre,
                referidor.email as referidor_email,
                referidor.telefono as referidor_telefono,
                referido.nombre as referido_nombre,
                referido.email as referido_email
            FROM referidos_bonos rb
            JOIN clientes_app referidor ON referidor.id = rb.referidor_id
            JOIN clientes_app referido ON referido.id = rb.referido_id
        `;
        
        const params = [];
        if (estado) {
            query += ' WHERE rb.estado = ?';
            params.push(estado);
        }
        
        query += ' ORDER BY rb.fecha_inicio DESC';
        
        const bonos = await dbAll(query, params);
        
        // Calcular estadisticas
        const stats = {
            pendientes: bonos.filter(b => b.estado === 'pendiente').length,
            completados: bonos.filter(b => b.estado === 'completado').length,
            pagados: bonos.filter(b => b.estado === 'pagado').length,
            expirados: bonos.filter(b => b.estado === 'expirado').length,
            total_por_pagar: bonos.filter(b => b.estado === 'completado').reduce((sum, b) => sum + (b.bono_monto || 0), 0)
        };
        
        res.json({ bonos, estadisticas: stats });
    } catch (error) {
        console.error('Error listando bonos de referidos:', error);
        res.status(500).json({ error: 'Error al listar bonos de referidos' });
    }
});

// GET /api/referidos/bonos/pendientes-pago - Bonos listos para pagar
app.get('/api/referidos/bonos/pendientes-pago', apiAuth, async (req, res) => {
    try {
        const bonos = await dbAll(`
            SELECT 
                rb.*,
                referidor.nombre as referidor_nombre,
                referidor.email as referidor_email,
                referidor.telefono as referidor_telefono,
                referido.nombre as referido_nombre
            FROM referidos_bonos rb
            JOIN clientes_app referidor ON referidor.id = rb.referidor_id
            JOIN clientes_app referido ON referido.id = rb.referido_id
            WHERE rb.estado = 'completado'
            ORDER BY rb.fecha_completado ASC
        `);
        
        res.json({
            bonos,
            total_por_pagar: bonos.reduce((sum, b) => sum + (b.bono_monto || 0), 0),
            cantidad: bonos.length
        });
    } catch (error) {
        console.error('Error listando bonos pendientes:', error);
        res.status(500).json({ error: 'Error al listar bonos pendientes' });
    }
});

// PUT /api/referidos/bonos/:id/pagar - Marcar bono como pagado
app.put('/api/referidos/bonos/:id/pagar', apiAuth, async (req, res) => {
    try {
        const { notas } = req.body;
        const bonoId = req.params.id;
        
        const bono = await dbGet('SELECT * FROM referidos_bonos WHERE id = ?', [bonoId]);
        if (!bono) {
            return res.status(404).json({ error: 'Bono no encontrado' });
        }
        
        if (bono.estado !== 'completado') {
            return res.status(400).json({ error: 'Solo se pueden pagar bonos en estado completado' });
        }
        
        await dbRun(
            "UPDATE referidos_bonos SET estado = 'pagado', fecha_pagado = datetime('now'), notas = ? WHERE id = ?",
            [notas || null, bonoId]
        );
        
        // Notificar al referidor
        const referidor = await dbGet('SELECT nombre, email FROM clientes_app WHERE id = ?', [bono.referidor_id]);
        const referido = await dbGet('SELECT nombre FROM clientes_app WHERE id = ?', [bono.referido_id]);
        
        const mensaje = `&#128176; <b>BONO DE REFERIDO PAGADO</b>\n\n` +
            `&#128100; Beneficiario: ${referidor.nombre}\n` +
            `&#128176; Monto: $${bono.bono_monto.toLocaleString('es-CL')} CLP\n` +
            `&#127873; Por referir a: ${referido.nombre}\n\n` +
            `&#9989; Pagado por: ${req.session?.user?.username || 'Admin'}`;
        
        await enviarNotificacionTelegram(mensaje);
        
        res.json({ success: true, mensaje: 'Bono marcado como pagado' });
    } catch (error) {
        console.error('Error pagando bono:', error);
        res.status(500).json({ error: 'Error al marcar bono como pagado' });
    }
});

// GET /api/referidos/estadisticas - Estadisticas generales del programa
app.get('/api/referidos/estadisticas', apiAuth, async (req, res) => {
    try {
        const stats = await dbGet(`
            SELECT
                COUNT(*) as total_bonos,
                SUM(CASE WHEN estado = 'pendiente' THEN 1 ELSE 0 END) as pendientes,
                SUM(CASE WHEN estado = 'completado' THEN 1 ELSE 0 END) as completados,
                SUM(CASE WHEN estado = 'pagado' THEN 1 ELSE 0 END) as pagados,
                SUM(CASE WHEN estado = 'expirado' THEN 1 ELSE 0 END) as expirados,
                SUM(CASE WHEN estado = 'completado' THEN bono_monto ELSE 0 END) as total_por_pagar,
                SUM(CASE WHEN estado = 'pagado' THEN bono_monto ELSE 0 END) as total_pagado
            FROM referidos_bonos
        `);
        
        const usuariosConCodigo = await dbGet('SELECT COUNT(*) as total FROM clientes_app WHERE codigo_referido IS NOT NULL');
        const usuariosReferidos = await dbGet('SELECT COUNT(*) as total FROM clientes_app WHERE referido_por IS NOT NULL');
        
        res.json({
            bonos: stats,
            usuarios_con_codigo: usuariosConCodigo?.total || 0,
            usuarios_referidos: usuariosReferidos?.total || 0
        });
    } catch (error) {
        console.error('Error obteniendo estadisticas de referidos:', error);
        res.status(500).json({ error: 'Error al obtener estadisticas' });
    }
});

// =================================================================
// ENDPOINTS DE CDIGOS PROMOCIONALES
// =================================================================

// GET /api/codigos-promocionales - Listar todos los cdigos (admin)
app.get('/api/codigos-promocionales', apiAuth, async (req, res) => {
    try {
        const codigos = await dbAll(`
            SELECT cp.*, u.username as creado_por_nombre
            FROM codigos_promocionales cp
            LEFT JOIN usuarios u ON cp.creado_por = u.id
            ORDER BY cp.fecha_creacion DESC
        `);
        res.json(codigos);
    } catch (error) {
        console.error('Error listando codigos:', error);
        res.status(500).json({ error: 'Error al listar cdigos promocionales' });
    }
});

// POST /api/codigos-promocionales - Crear cdigo (admin)
app.post('/api/codigos-promocionales', apiAuth, async (req, res) => {
    try {
        const { codigo, descripcion, tasa_especial, usos_maximos, fecha_inicio, fecha_expiracion, solo_primer_envio } = req.body;
        
        if (!codigo || !tasa_especial) {
            return res.status(400).json({ error: 'Cdigo y tasa especial son requeridos' });
        }

        // Verificar que el cdigo no exista
        const existe = await dbGet('SELECT id FROM codigos_promocionales WHERE UPPER(codigo) = UPPER(?)', [codigo]);
        if (existe) {
            return res.status(400).json({ error: 'El cdigo ya existe' });
        }

        const result = await dbRun(`
            INSERT INTO codigos_promocionales (codigo, descripcion, tasa_especial, usos_maximos, fecha_inicio, fecha_expiracion, solo_primer_envio, creado_por, fecha_creacion)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        `, [codigo.toUpperCase(), descripcion || null, tasa_especial, usos_maximos || null, fecha_inicio || null, fecha_expiracion || null, solo_primer_envio ? 1 : 0, req.userId]);

        res.json({ success: true, id: result.lastID });
    } catch (error) {
        console.error('Error creando codigo:', error);
        res.status(500).json({ error: 'Error al crear cdigo promocional' });
    }
});

// PUT /api/codigos-promocionales/:id - Actualizar cdigo (admin)
app.put('/api/codigos-promocionales/:id', apiAuth, async (req, res) => {
    try {
        const { descripcion, tasa_especial, activo, usos_maximos, fecha_inicio, fecha_expiracion, solo_primer_envio } = req.body;
        
        await dbRun(`
            UPDATE codigos_promocionales 
            SET descripcion = ?, tasa_especial = ?, activo = ?, usos_maximos = ?, fecha_inicio = ?, fecha_expiracion = ?, solo_primer_envio = ?
            WHERE id = ?
        `, [descripcion, tasa_especial, activo ? 1 : 0, usos_maximos || null, fecha_inicio || null, fecha_expiracion || null, solo_primer_envio ? 1 : 0, req.params.id]);

        res.json({ success: true });
    } catch (error) {
        console.error('Error actualizando codigo:', error);
        res.status(500).json({ error: 'Error al actualizar cdigo promocional' });
    }
});

// DELETE /api/codigos-promocionales/:id - Eliminar cdigo (admin)
app.delete('/api/codigos-promocionales/:id', apiAuth, async (req, res) => {
    try {
        await dbRun('DELETE FROM codigos_promocionales WHERE id = ?', [req.params.id]);
        res.json({ success: true });
    } catch (error) {
        console.error('Error eliminando codigo:', error);
        res.status(500).json({ error: 'Error al eliminar cdigo promocional' });
    }
});

// POST /api/cliente/validar-codigo - Validar cdigo promocional (cliente app)
app.post('/api/cliente/validar-codigo', clienteAuth, async (req, res) => {
    try {
        const { codigo } = req.body;
        const clienteId = req.clienteId;

        if (!codigo) {
            return res.status(400).json({ error: 'Cdigo requerido' });
        }

        // Buscar el cdigo
        const codigoPromo = await dbGet(`
            SELECT * FROM codigos_promocionales 
            WHERE UPPER(codigo) = UPPER(?) AND activo = 1
        `, [codigo]);

        if (!codigoPromo) {
            return res.json({ valido: false, mensaje: 'Cdigo no vlido o inactivo' });
        }

        // Verificar si es un cupón exclusivo para un cliente específico
        if (codigoPromo.cliente_exclusivo_id && codigoPromo.cliente_exclusivo_id !== clienteId) {
            return res.json({ valido: false, mensaje: 'Este cupón no está disponible para tu cuenta' });
        }

        // Verificar fecha de inicio
        if (codigoPromo.fecha_inicio) {
            const inicio = new Date(codigoPromo.fecha_inicio);
            if (new Date() < inicio) {
                return res.json({ valido: false, mensaje: 'El cdigo an no est activo' });
            }
        }

        // Verificar fecha de expiracin
        if (codigoPromo.fecha_expiracion) {
            const expira = new Date(codigoPromo.fecha_expiracion);
            if (new Date() > expira) {
                return res.json({ valido: false, mensaje: 'El cdigo ha expirado' });
            }
        }

        // Verificar usos mximos globales
        if (codigoPromo.usos_maximos && codigoPromo.usos_actuales >= codigoPromo.usos_maximos) {
            return res.json({ valido: false, mensaje: 'El cdigo ha alcanzado el lmite de usos' });
        }

        // Verificar si el cliente ya us este cdigo
        const yaUsado = await dbGet(`
            SELECT id FROM uso_codigos_promocionales 
            WHERE codigo_id = ? AND cliente_app_id = ?
        `, [codigoPromo.id, clienteId]);

        if (yaUsado) {
            return res.json({ valido: false, mensaje: 'Ya has utilizado este cdigo' });
        }

        // Si es solo_primer_envio, verificar que el cliente no tenga envos previos
        if (codigoPromo.solo_primer_envio) {
            const enviosPrevios = await dbGet(`
                SELECT COUNT(*) as total FROM solicitudes_transferencia 
                WHERE cliente_app_id = ? AND estado IN ('completada', 'procesando', 'pendiente')
            `, [clienteId]);

            if (enviosPrevios && enviosPrevios.total > 0) {
                return res.json({ valido: false, mensaje: 'Este cdigo es solo para el primer envo' });
            }
        }

        // Determinar tipo de descuento y respuesta
        const tipoDescuento = codigoPromo.tipo_descuento || 'tasa';
        
        // Cdigo vlido
        res.json({
            valido: true,
            codigo_id: codigoPromo.id,
            tipo_descuento: tipoDescuento,
            tasa_especial: tipoDescuento === 'tasa' ? codigoPromo.tasa_especial : null,
            monto_descuento_clp: tipoDescuento === 'monto_clp' ? codigoPromo.monto_descuento_clp : null,
            descripcion: codigoPromo.descripcion,
            mensaje: codigoPromo.descripcion || 'Cdigo aplicado!'
        });

    } catch (error) {
        console.error('Error validando codigo:', error);
        res.status(500).json({ error: 'Error al validar cdigo' });
    }
});

// POST /api/cliente/usar-codigo - Registrar uso de cdigo al crear solicitud
app.post('/api/cliente/usar-codigo', clienteAuth, async (req, res) => {
    try {
        const { codigo_id, solicitud_id } = req.body;
        const clienteId = req.clienteId;

        // Registrar el uso
        await dbRun(`
            INSERT INTO uso_codigos_promocionales (codigo_id, cliente_app_id, solicitud_id, fecha_uso)
            VALUES (?, ?, ?, datetime('now'))
        `, [codigo_id, clienteId, solicitud_id]);

        // Incrementar contador de usos
        await dbRun(`
            UPDATE codigos_promocionales SET usos_actuales = usos_actuales + 1 WHERE id = ?
        `, [codigo_id]);

        res.json({ success: true });
    } catch (error) {
        console.error('Error registrando uso de codigo:', error);
        res.status(500).json({ error: 'Error al registrar uso del cdigo' });
    }
});


// =================================================================
// API TRANSFERENCIAS BANCO (bot bancario)
// =================================================================

// Token de autenticación para el bot (sin sesión de cookies)
const BOT_TOKEN = process.env.DEFI_BOT_TOKEN || 'bot-defi-2026';

const botAuth = (req, res, next) => {
    const authHeader = req.headers['authorization'] || '';
    const token = authHeader.replace('Bearer ', '');
    if (token === BOT_TOKEN) return next();
    return res.status(401).json({ error: 'Token invalido' });
};

// Tracking de última señal del bot (para determinar online/offline)
let botUltimaSenal = null;       // Date de última señal recibida
let botUltimoResultado = null;   // { insertadas, duplicadas, total }
const BOT_TIMEOUT_MS = 10 * 60 * 1000; // 10 minutos

// POST /api/transferencias-banco/push - El bot envía transferencias (auth por token)
app.post('/api/transferencias-banco/push', botAuth, async (req, res) => {
    try {
        const { transferencias, lote_id, tipo, cuenta_nombre } = req.body;
        if (!transferencias || !Array.isArray(transferencias) || transferencias.length === 0) {
            return res.status(400).json({ error: 'Se requiere un array de transferencias' });
        }

        const ahora = new Date().toISOString();
        const loteFinal = lote_id || `lote_${Date.now()}`;
        const cuentaDest = sanitizarTexto(cuenta_nombre || '', 50);
        let insertadas = 0;
        let duplicadas = 0;

        for (const t of transferencias) {
            const nOp = sanitizarTexto(t.n_operacion || '', 50);
            const fecha = sanitizarTexto(t.fecha || '', 50);
            if (!nOp) continue;

            const montoStr = String(t.monto || t.monto_str || '0').trim().slice(0, 30);
            const montoNum = parseInt(montoStr.replace(/[\$\.\s]/g, '').replace(/,/g, '')) || 0;

            try {
                await dbRun(`INSERT INTO transferencias_banco
                    (n_operacion, fecha, rut_origen, nombre_origen, cuenta_origen, banco, monto, monto_numerico, lote_id, cuenta_destino, fecha_importacion)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [nOp, fecha, sanitizarTexto(t.rut_origen || '', 30), sanitizarTexto(t.nombre_origen || '', 100),
                     sanitizarTexto(t.cuenta_origen || '', 30), sanitizarTexto(t.banco || '', 50),
                     montoStr, montoNum, loteFinal, cuentaDest, ahora]);
                insertadas++;
            } catch (e) {
                if (e.message && e.message.includes('UNIQUE')) {
                    duplicadas++;
                } else {
                    console.error('Error insertando transferencia bot:', e.message);
                }
            }
        }

        // Registrar señal del bot
        botUltimaSenal = new Date();
        botUltimoResultado = { insertadas, duplicadas, total: transferencias.length };

        console.log(`[BOT] Importadas ${insertadas} transferencias (${duplicadas} duplicadas) - lote: ${loteFinal}`);
        res.json({ success: true, lote_id: loteFinal, insertadas, duplicadas, total: transferencias.length });
    } catch (error) {
        console.error('Error en push del bot:', error);
        res.status(500).json({ error: 'Error al importar transferencias' });
    }
});

// POST /api/transferencias-banco/importar - Importar desde UI (auth por sesión)
app.post('/api/transferencias-banco/importar', apiAuth, async (req, res) => {
    try {
        const { transferencias, lote_id } = req.body;
        if (!transferencias || !Array.isArray(transferencias) || transferencias.length === 0) {
            return res.status(400).json({ error: 'Se requiere un array de transferencias' });
        }

        const ahora = new Date().toISOString();
        const loteFinal = lote_id || `lote_${Date.now()}`;
        let insertadas = 0;
        let duplicadas = 0;

        for (const t of transferencias) {
            const nOp = sanitizarTexto(t.n_operacion || '', 50);
            const fecha = sanitizarTexto(t.fecha || '', 50);
            if (!nOp) continue;

            const montoStr = String(t.monto || '0').trim().slice(0, 30);
            const montoNum = parseInt(montoStr.replace(/[\$\.\s]/g, '').replace(/,/g, '')) || 0;

            try {
                await dbRun(`INSERT INTO transferencias_banco
                    (n_operacion, fecha, rut_origen, nombre_origen, cuenta_origen, banco, monto, monto_numerico, lote_id, fecha_importacion)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [nOp, fecha, sanitizarTexto(t.rut_origen || '', 30), sanitizarTexto(t.nombre_origen || '', 100),
                     sanitizarTexto(t.cuenta_origen || '', 30), sanitizarTexto(t.banco || '', 50),
                     montoStr, montoNum, loteFinal, ahora]);
                insertadas++;
            } catch (e) {
                if (e.message && e.message.includes('UNIQUE')) {
                    duplicadas++;
                } else {
                    console.error('Error insertando transferencia:', e.message);
                }
            }
        }

        res.json({
            success: true,
            lote_id: loteFinal,
            insertadas,
            duplicadas,
            total: transferencias.length
        });
    } catch (error) {
        console.error('Error importando transferencias:', error);
        res.status(500).json({ error: 'Error al importar transferencias' });
    }
});

// GET /api/transferencias-banco - Listar transferencias con filtros
app.get('/api/transferencias-banco', apiAuth, async (req, res) => {
    try {
        const { estado, fecha, lote_id, limit: lim, offset: off } = req.query;
        let where = [];
        let params = [];

        if (estado) { where.push('estado = ?'); params.push(estado); }
        if (fecha) { where.push('fecha LIKE ?'); params.push(`%${fecha}%`); }
        if (lote_id) { where.push('lote_id = ?'); params.push(lote_id); }

        const whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';
        const limitVal = Math.min(parseInt(lim) || 200, 500);
        const offsetVal = parseInt(off) || 0;

        const total = await dbGet(`SELECT COUNT(*) as total FROM transferencias_banco ${whereClause}`, params);
        const rows = await dbAll(`
            SELECT tb.*, u.username as tomado_por_nombre
            FROM transferencias_banco tb
            LEFT JOIN usuarios u ON tb.tomado_por = u.id
            ${whereClause}
            ORDER BY tb.id DESC
            LIMIT ? OFFSET ?`, [...params, limitVal, offsetVal]);

        const stats = await dbGet(`
            SELECT
                COUNT(*) as total_registros,
                SUM(CASE WHEN estado = 'disponible' THEN 1 ELSE 0 END) as disponibles,
                SUM(CASE WHEN estado = 'tomado' THEN 1 ELSE 0 END) as tomados,
                SUM(CASE WHEN estado = 'procesado' THEN 1 ELSE 0 END) as procesados,
                SUM(monto_numerico) as monto_total
            FROM transferencias_banco ${whereClause}`, params);

        res.json({ transferencias: rows, total: total.total, stats });
    } catch (error) {
        console.error('Error obteniendo transferencias:', error);
        res.status(500).json({ error: 'Error al obtener transferencias' });
    }
});

// PUT /api/transferencias-banco/:id/tomar - Marcar como tomado
app.put('/api/transferencias-banco/:id/tomar', apiAuth, async (req, res) => {
    try {
        const { id } = req.params;
        const userId = req.session.user.id;
        const ahora = new Date().toISOString();

        const tf = await dbGet(`SELECT * FROM transferencias_banco WHERE id = ?`, [id]);
        if (!tf) return res.status(404).json({ error: 'Transferencia no encontrada' });
        if (tf.estado !== 'disponible') return res.status(400).json({ error: 'Esta transferencia ya fue tomada' });

        await dbRun(`UPDATE transferencias_banco SET estado = 'tomado', tomado_por = ?, tomado_fecha = ? WHERE id = ?`,
            [userId, ahora, id]);

        res.json({ success: true, message: 'Transferencia marcada como tomada' });
    } catch (error) {
        console.error('Error tomando transferencia:', error);
        res.status(500).json({ error: 'Error al tomar transferencia' });
    }
});

// PUT /api/transferencias-banco/:id/liberar - Liberar (volver a disponible)
app.put('/api/transferencias-banco/:id/liberar', apiAuth, async (req, res) => {
    try {
        const { id } = req.params;
        await dbRun(`UPDATE transferencias_banco SET estado = 'disponible', tomado_por = NULL, tomado_fecha = NULL WHERE id = ?`, [id]);
        res.json({ success: true });
    } catch (error) {
        console.error('Error liberando transferencia:', error);
        res.status(500).json({ error: 'Error al liberar transferencia' });
    }
});

// PUT /api/transferencias-banco/:id/procesado - Marcar como procesado
app.put('/api/transferencias-banco/:id/procesado', apiAuth, async (req, res) => {
    try {
        const { id } = req.params;
        const tf = await dbGet(`SELECT * FROM transferencias_banco WHERE id = ?`, [id]);
        if (!tf) return res.status(404).json({ error: 'Transferencia no encontrada' });

        await dbRun(`UPDATE transferencias_banco SET estado = 'procesado' WHERE id = ?`, [id]);
        res.json({ success: true });
    } catch (error) {
        console.error('Error marcando como procesado:', error);
        res.status(500).json({ error: 'Error al marcar como procesado' });
    }
});

// GET /api/transferencias-banco/lotes - Listar lotes importados
app.get('/api/transferencias-banco/lotes', apiAuth, async (req, res) => {
    try {
        const lotes = await dbAll(`
            SELECT lote_id,
                   MIN(fecha_importacion) as fecha_importacion,
                   COUNT(*) as cantidad,
                   SUM(monto_numerico) as monto_total,
                   SUM(CASE WHEN estado = 'disponible' THEN 1 ELSE 0 END) as disponibles,
                   SUM(CASE WHEN estado = 'tomado' THEN 1 ELSE 0 END) as tomados,
                   SUM(CASE WHEN estado = 'procesado' THEN 1 ELSE 0 END) as procesados
            FROM transferencias_banco
            GROUP BY lote_id
            ORDER BY fecha_importacion DESC
            LIMIT 50`);
        res.json({ lotes });
    } catch (error) {
        console.error('Error obteniendo lotes:', error);
        res.status(500).json({ error: 'Error al obtener lotes' });
    }
});

// ============================================
// BOT PROXY - Solicitar tareas al bot local
// ============================================
const BOT_LOCAL_URL = process.env.BOT_LOCAL_URL || 'http://localhost:5050';

// GET /api/transferencias-banco/bot-status - Estado del bot basado en última señal
app.get('/api/transferencias-banco/bot-status', apiAuth, async (req, res) => {
    if (!botUltimaSenal) {
        return res.json({ estado: 'offline', mensaje: 'Bot no ha enviado señales aun' });
    }

    const ahora = new Date();
    const diff = ahora - botUltimaSenal;
    const minutosAtras = Math.round(diff / 60000);

    if (diff <= BOT_TIMEOUT_MS) {
        return res.json({
            estado: 'listo',
            mensaje: `Bot activo - ultima señal hace ${minutosAtras} min`,
            ultima_senal: botUltimaSenal.toISOString(),
            ultimo_resultado: botUltimoResultado
        });
    } else {
        return res.json({
            estado: 'offline',
            mensaje: `Bot sin señal hace ${minutosAtras} min (limite: 10 min)`,
            ultima_senal: botUltimaSenal.toISOString(),
            ultimo_resultado: botUltimoResultado
        });
    }
});

// POST /api/transferencias-banco/solicitar - Enviar comando al bot
app.post('/api/transferencias-banco/solicitar', apiAuth, async (req, res) => {
    try {
        const { tarea, fecha } = req.body || {};
        console.log(`[Bot Proxy] Solicitud: ${tarea || 'transferencias_recibidas'} fecha: ${fecha || 'hoy'}`);

        const resp = await axios.post(`${BOT_LOCAL_URL}/ejecutar`, {
            tarea: tarea || 'transferencias_recibidas',
            fecha: fecha || null
        }, {
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${BOT_TOKEN}`
            },
            timeout: 120000  // 2 minutos - el bot puede tardar en login+navegar+leer+logout
        });

        console.log(`[Bot Proxy] Resultado:`, resp.data);
        res.json(resp.data);
    } catch (error) {
        if (error.code === 'ECONNREFUSED' || error.code === 'ECONNRESET') {
            return res.status(503).json({
                success: false,
                error: 'Bot no esta corriendo. Inicia el bot con: python agente_conciliador.py'
            });
        }
        if (error.code === 'ECONNABORTED' || error.message.includes('timeout')) {
            return res.status(504).json({
                success: false,
                error: 'El bot tardo demasiado en responder (timeout 2 min)'
            });
        }
        console.error('[Bot Proxy] Error:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

runMigrations()
    .then(() => {
        app.listen(PORT, () => {
            console.log(`- Servidor corriendo en http://localhost:${PORT}`);
            iniciarMonitoreoProactivo(); // ... Iniciar monitoreo proactivo
            iniciarMonitoreoTasas();     // ... Iniciar monitoreo de tasas P2P
            // Tasa BCV: consultar al arrancar y cada 4 horas
            setTimeout(obtenerTasaBCV, 15000);
            setInterval(obtenerTasaBCV, 4 * 60 * 60 * 1000);
            console.log('Sistema de tasa BCV iniciado (cada 4 horas)');
            // Conciliación automática: cada 2 minutos
            setTimeout(conciliarPedidos, 30000);
            setInterval(conciliarPedidos, 2 * 60 * 1000);
            console.log('Sistema de conciliación automática iniciado (cada 2 min)');
        });
    })
    .catch(err => {
        console.error(" No se pudo iniciar el servidor debido a un error en la migración de la base de datos:", err);
        process.exit(1); // Detiene la aplicación si la BD no se puede inicializar
    });





