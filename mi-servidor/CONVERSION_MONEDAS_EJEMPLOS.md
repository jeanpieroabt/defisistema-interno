# 💱 Sistema de Conversión de Monedas P2P

## 📋 Monedas Soportadas

| Código | Moneda | País | Tasa Base (a CLP) |
|--------|--------|------|-------------------|
| **CLP** | Peso Chileno | Chile | 1 (base) |
| **COP** | Peso Colombiano | Colombia | 0.25 (1 COP = 0.25 CLP) |
| **VES** | Bolívar Venezolano | Venezuela | 33.33 (1 VES = 33.33 CLP) |
| **USD** | Dólar | Estados Unidos | 950 (1 USD = 950 CLP) |
| **ARS** | Peso Argentino | Argentina | 1.05 (1 ARS = 1.05 CLP) |
| **PEN** | Sol | Perú | 250 (1 PEN = 250 CLP) |
| **BRL** | Real | Brasil | 190 (1 BRL = 190 CLP) |
| **MXN** | Peso Mexicano | México | 55 (1 MXN = 55 CLP) |
| **EUR** | Euro | Europa | 1050 (1 EUR = 1050 CLP) |
| **UYU** | Peso Uruguayo | Uruguay | 23 (1 UYU = 23 CLP) |

## 📐 Fórmulas de Conversión

### Fórmula General

```
Cantidad en Moneda B = Cantidad en Moneda A × Tasa(A→B)
```

### Cálculo de Tasa entre dos monedas

```
Tasa(A→B) = Tasa(A→CLP) / Tasa(B→CLP)
```

## 💡 Ejemplos Prácticos

### Ejemplo 1: CLP → COP
**Pregunta:** "¿Cuántos pesos colombianos son 100.000 pesos chilenos?"

```
Tasa CLP→COP = 1 / 0.25 = 4
100.000 CLP × 4 = 400.000 COP
```

**Respuesta:** "100.000 pesos chilenos equivalen a 400.000 pesos colombianos"

---

### Ejemplo 2: COP → CLP (CASO INVERSO)
**Pregunta:** "¿Cuántos CLP debo transferir para que lleguen 40.000 pesos colombianos?"

```
Tasa COP→CLP = 0.25 / 1 = 0.25
40.000 COP × 0.25 = 10.000 CLP
```

**Respuesta:** "Para que lleguen 40.000 pesos colombianos, debes transferir 10.000 pesos chilenos"

---

### Ejemplo 3: CLP → VES
**Pregunta:** "¿Cuántos bolívares recibe el cliente por 50.000 CLP?"

```
Tasa CLP→VES = 1 / 33.33 = 0.03
50.000 CLP × 0.03 = 1.500 VES
```

**Respuesta:** "Por 50.000 pesos chilenos, el cliente recibe 1.500 bolívares venezolanos"

---

### Ejemplo 4: VES → CLP (CASO INVERSO)
**Pregunta:** "¿Cuántos CLP necesito para enviar 10.000 VES?"

```
Tasa VES→CLP = 33.33 / 1 = 33.33
10.000 VES × 33.33 = 333.300 CLP
```

**Respuesta:** "Para enviar 10.000 bolívares, necesitas 333.300 pesos chilenos"

---

### Ejemplo 5: CLP → USD
**Pregunta:** "Convertir 100.000 CLP a dólares"

```
Tasa CLP→USD = 1 / 950 = 0.00105
100.000 CLP × 0.00105 = 105.26 USD
```

**Respuesta:** "100.000 pesos chilenos equivalen a 105.26 dólares"

---

### Ejemplo 6: COP → VES (Entre dos monedas no-CLP)
**Pregunta:** "¿Cuántos bolívares son 50.000 pesos colombianos?"

```
Paso 1: COP → CLP
50.000 COP × 0.25 = 12.500 CLP

Paso 2: CLP → VES
12.500 CLP × 0.03 = 375 VES

O directamente:
Tasa COP→VES = 0.25 / 33.33 = 0.0075
50.000 COP × 0.0075 = 375 VES
```

**Respuesta:** "50.000 pesos colombianos equivalen a 375 bolívares venezolanos"

---

## 🤖 Uso en el Chatbot

El chatbot detecta automáticamente preguntas de conversión y llama a la función correspondiente:

### Ejemplos de preguntas que el chatbot entiende:

✅ "¿cuánto debo transferir para que lleguen 40.000 pesos colombianos?"
✅ "convertir 100.000 CLP a VES"
✅ "cuántos dólares son 500.000 pesos chilenos?"
✅ "equivalencia entre pesos chilenos y colombianos"
✅ "cuál es la tasa CLP a COP"
✅ "¿cuántos COP recibe por 50.000 CLP?"
✅ "necesito enviar 100 USD, cuántos CLP son?"

### Interpretación Inteligente

El chatbot interpreta correctamente:

**"¿Cuánto debo transferir para que lleguen X [moneda]?"**
→ Usuario conoce destino, necesita saber origen
→ Convierte desde moneda destino hacia moneda origen

**"¿Cuánto llega si envío X [moneda]?"**
→ Usuario conoce origen, necesita saber destino
→ Convierte desde moneda origen hacia moneda destino

---

## 🔧 Actualizar Tasas de Cambio

Las tasas están definidas en `servidor.js`:

```javascript
const TASAS_CAMBIO_P2P = {
    'CLP': 1,
    'COP': 0.25,
    'VES': 33.33,
    'USD': 950,
    // ... etc
};
```

**Importante:** Actualizar estas tasas regularmente según el mercado y las tasas de Binance P2P.

---

## 📱 Ejemplo de Conversación Real

**Usuario:** "hola, ¿cuánto debo transferir en pesos chilenos para que a mi cliente le lleguen 200.000 pesos colombianos?"

**Chatbot:** 
> 🤖 Agente llamando función: calcular_conversion_moneda con args: {monto: 200000, moneda_origen: "COP", moneda_destino: "CLP"}
> 
> ¡Hola! 👋
> 
> Para que a tu cliente le lleguen **200.000 Pesos Colombianos**, debes transferir:
> 
> **💰 50.000 Pesos Chilenos**
> 
> Tasa de cambio: 1 COP = 0.25 CLP
> Fórmula: 200.000 COP × 0.25 = 50.000 CLP
> 
> ¿Necesitas algo más? 😊

---

## ✅ Ventajas del Sistema

1. **Bidireccional:** Calcula en ambas direcciones automáticamente
2. **Multi-moneda:** Soporta 10 monedas diferentes
3. **Inteligente:** Interpreta la intención del usuario
4. **Preciso:** Muestra fórmula y tasa aplicada
5. **Fácil actualización:** Tasas centralizadas en un solo lugar

---

## 🎯 Casos de Uso

### Para Operadores:
- Calcular rápidamente cuánto debe transferir el cliente
- Verificar tasas de cambio actuales
- Dar cotizaciones inmediatas al cliente
- Convertir entre cualquier par de monedas P2P

### Para Supervisores:
- Revisar márgenes de conversión
- Actualizar tasas según mercado
- Capacitar operadores en conversiones

---

**Fecha de actualización:** Noviembre 2025
**Versión:** 1.0
