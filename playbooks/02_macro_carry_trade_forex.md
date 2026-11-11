# Playbook Institucional 02: Forex Macro y Estrategias Carry Trade

**Mercado:** Divisas (Forex G10) y Materias Primas  
**Horizonte:** Swing y Holder (`SWING_HOLDER` / `INTRADAY`)  
**Especialidad:** Diferenciales de Tasas de Interés, Reportes COT y Correlaciones Macro

---

## 1. Fundamentos de la Estrategia Carry Trade

### Fundamento Cuantitativo
El *Carry Trade* consiste en endeudarse en una divisa con baja tasa de interés (moneda de fondeo, ej. JPY o CHF) e invertir en una divisa con alta tasa de rendimiento real (moneda de inversión, ej. USD o divisas emergentes sólidas).
$$\text{Retorno Neto Carry} = (r_{\text{inversión}} - r_{\text{fondeo}}) + \Delta S$$
Donde $\Delta S$ es la variación del tipo de cambio spot.

### Reglas Institucionales:
1. **Filtro de Volatilidad (VIX / FX Volatility):** Solo ingresar operaciones de Carry cuando el índice VIX sea inferior a 20. Si el VIX supera 25, liquidar inmediatamente la exposición para evitar el desplome por *unwinding* de carry.
2. **Dirección Técnica Obligatoria:** La tendencia técnica semanal debe coincidir con el diferencial de tasas ($H > 0.50$). Nunca operar carry contra una tendencia estructural fuerte.

---

## 2. Decodificación de los Reportes COT (Commitment of Traders - CFTC)

### Actores del Mercado:
* **Commercials (Hedgers):** Productores e importadores/exportadores corporativos. Operan contra la tendencia extrema para cubrir riesgos de negocio.
* **Non-Commercials (Leveraged Funds / Asset Managers):** Especuladores institucionales y fondos cuantitativos de tendencia (*trend followers*).

### Señales Cuantitativas Institucionales:
1. **Extremo de Posicionamiento:** Cuando el posicionamiento neto de los *Leveraged Funds* se encuentra en los percentiles extremos ($> 95\%$ o $< 5\%$ en una ventana móvil de 3 años), el mercado se encuentra vulnerable a una reversión masiva por liquidación en cascada.
2. **Cruce de Cero (Zero Line Crossover):** Giro de posición neta de negativa a positiva confirma el inicio de un ciclo macro plurianual.

---

## 3. Correlación con Curvas de Rendimiento (US10Y & T10Y2Y)

### Reglas de Correlación:
1. **Pares con el USD (EURUSD, USDJPY):**
   - Una ampliación en el diferencial de rendimiento soberano (ej. US 10-Year Treasury Yield vs German Bunds) actúa como el principal vector de empuje para la divisa con mayor rendimiento.
   - En USDJPY, una correlación positiva histórica $> 0.80$ con el rendimiento de los bonos del Tesoro a 10 años ($US10Y$) dicta la dirección primaria del flujo intradía.

2. **Inversión de la Curva (T10Y2Y < 0):**
   - Activar perfil defensivo en materias primas cíclicas y aumentar asignación a activos de refugio ($XAU/USD$, Oro).

