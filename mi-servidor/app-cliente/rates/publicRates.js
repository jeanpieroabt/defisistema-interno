// Tasas publicas mostradas en el login del cliente.
// Consulta el endpoint real y cae en un fallback estatico si falla.
const API_BASE = (typeof window !== 'undefined' && window.API_BASE_URL)
    ? window.API_BASE_URL.replace(/\/$/, '')
    : '';

const FALLBACK_RATES = [
    { min: "Desde 5.000 CLP", value: "0.4000 VES" },
    { min: "Desde 100.000 CLP", value: "0.4100 VES" },
    { min: "Desde 250.000 CLP", value: "0.4200 VES" }
];

const formatCLP = (value) => Number(value || 0).toLocaleString('es-CL');

export async function getPublicRates() {
    const endpoint = `${API_BASE}/api/cliente/tasa`;

    try {
        const response = await fetch(endpoint);
        if (!response.ok) {
            throw new Error(`Respuesta HTTP ${response.status}`);
        }

        const data = await response.json();
        const tramos = Array.isArray(data?.tramos) ? data.tramos : [];
        if (!tramos.length) {
            throw new Error('Respuesta sin tramos de tasas');
        }

        const rates = tramos.map((tramo) => {
            const tasa = Number(tramo.tasa ?? 0);
            const minLabel = formatCLP(tramo.minCLP);
            const min = `Desde ${minLabel} CLP`;
            const value = Number.isFinite(tasa) ? `${tasa.toFixed(4)} VES` : 'N/D';
            return { min, value };
        });

        return {
            rates,
            updatedAt: data?.actualizacion || new Date().toISOString()
        };
    } catch (error) {
        console.error('Error obteniendo tasas publicas:', error);
        return { rates: FALLBACK_RATES, updatedAt: null };
    }
}
