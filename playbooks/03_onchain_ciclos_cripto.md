# Playbook Institucional 03: Ciclos Cripto On-Chain y Dinámica de Derivados

**Mercado:** Criptoactivos (Spot, Futuros Perpetuos y On-Chain)  
**Horizonte:** Swing y Holder (`SWING_HOLDER` / `INTRADAY`)  
**Especialidad:** Métricas On-Chain, Open Interest, Funding Rates y Mapas de Liquidación

---

## 1. Ciclos On-Chain: MVRV Z-Score y Reservas en Exchanges

### Fundamento Matemático
El ratio **MVRV** compara la capitalización de mercado (*Market Value*) con la capitalización realizada (*Realized Value*, que tasa cada moneda al precio en que se movió por última vez en la blockchain).
$$\text{MVRV Z-Score} = \frac{\text{Market Cap} - \text{Realized Cap}}{\sigma_{\text{Market Cap}}}$$

### Umbrales Estratégicos:
1. **Z-Score < 0.1 (Fase de Acumulación Profunda / Holder):**
   - El mercado se encuentra en pérdidas no realizadas severas. Tasa de capitulación máxima.
   - *Acción:* Asignación escalonada según criterio de Kelly Fraccionado ($f^*$).
2. **Z-Score > 6.0 (Zona de Distribución y Euforia):**
   - Más del $95\%$ de las monedas están en beneficios masivos.
   - *Acción:* Desinversión sistemática y cobertura de cartera con futuros perpetuos cortos.

---

## 2. Dinámica de Derivados: Short Squeeze y Long Squeeze

### Indicadores Clave:
* **Funding Rate (Tasa de Financiación):** Tasa periódica pagada entre operadores largos y cortos en contratos de futuros perpetuos.
  - Tasa normal de equilibrio: $+0.01\%$ por 8 horas.
  - Tasa fuertemente positiva ($> +0.05\%$): El mercado minorista está apalancado al alza; riesgo extremo de *Long Squeeze*.
  - Tasa fuertemente negativa ($< -0.03\%$): Exceso de apalancamiento a la baja; riesgo elevado de *Short Squeeze*.
* **Open Interest (OI) Agregado:**
  - Si el precio lateraliza cerca de soporte y el OI se dispara con Funding negativo, una absorción institucional provocará un rebote violento hacia el Point of Control (POC).

---

## 3. Mapas de Calor de Liquidaciones y Gestión de Entrada

### Reglas de Entrada Institucional:
1. Identificar *clusters* masivos de liquidación por encima de la resistencia o por debajo del soporte local.
2. Esperar a que el precio ejecute el barrido (*sweep*) del *cluster* de liquidaciones.
3. Ingresar la orden cuando el *Open Interest* experimente una caída abrupta (señal de desapalancamiento forzado completado) y el precio reingrese al rango de valor previo (reversión hacia el POC).

