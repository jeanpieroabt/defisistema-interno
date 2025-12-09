// Datos publicos temporales de tasas CLP -> VES.
// Reemplazar con fetch a un endpoint real cuando este disponible.
export async function getPublicRates() {
    // Simulacion de lectura rapida; puede cambiarse por fetch.
    const rates = [
        { min: ">= 5.000 CLP", value: "0.4000 VES" },
        { min: ">= 100.000 CLP", value: "0.4100 VES" },
        { min: ">= 250.000 CLP", value: "0.4200 VES" }
    ];

    const updatedAt = new Date().toISOString();

    return { rates, updatedAt };
}
