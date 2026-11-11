# Playbook Institucional 01: Microestructura y Barrido de Liquidez

**Mercado:** Todos (Cripto, Equities, Forex)  
**Horizonte:** Intradía (`INTRADAY`)  
**Especialidad:** Microestructura de Mercado, VWAP y Perfiles de Volumen

---

## 1. Patrón de Barrido de Liquidez (Liquidity Sweep)

### Fundamento Cuantitativo
Los participantes institucionales requieren grandes bloques de contrapartida para ingresar posiciones sin incurrir en *slippage* excesivo. Dichos bloques residen en las órdenes de *Stop Loss* acumuladas por encima de máximos iguales (*Equal Highs*) o por debajo de mínimos iguales (*Equal Lows*), definidos con una tolerancia de:
$$\text{Tolerancia} = 0.10 \times ATR(14)$$

### Regla de Entrada
1. **Identificación:** El precio penetra agresivamente un nivel de *Equal Highs* o *Equal Lows*.
2. **Confirmación:** La vela cierra inmediatamente por debajo del nivel violado (rechazo o absorción institucional).
3. **Delta de Volumen:** El Cumulative Volume Delta (CVD) no acompaña la ruptura (divergencia de absorción pasiva en el libro de órdenes).
4. **Trigger:** Entrada en la vela siguiente con Stop Loss a $0.5 \times ATR$ más allá del extremo del *sweep*.

---

## 2. Absorción en Bandas Extremas de VWAP ($\pm 2\sigma$ y $\pm 3\sigma$)

### Fundamento Cuantitativo
El VWAP (*Volume Weighted Average Price*) representa el verdadero costo promedio ponderado por volumen de la sesión. 
- La banda a $+2\sigma$ abarca el $\approx 95.4\%$ de la distribución estadística normal de precios intradía.
- La banda a $+3\sigma$ abarca el $\approx 99.7\%$ de la distribución.

### Reglas de Ejecución:
* **Filtro de Régimen:** Solo ejecutar cuando el Exponente de Hurst sea $H < 0.45$ (Régimen de Reversión a la Media). Si $H > 0.55$, **NO OPERAR** contra la tendencia.
* **Señal de Compra:** El precio testea la banda $-2\sigma$ o $-3\sigma$, exhibiendo desaceleración de volumen vendedor y divergencia alcista en el CVD.
* **Take Profit:**
  - TP 1: Retorno al VWAP de la sesión.
  - TP 2: Point of Control (POC) del perfil de volumen de la sesión previa.
* **Stop Loss:** Cierre sostenido por fuera de la banda $\pm 3.2\sigma$ o distancia de $1.0 \times ATR$.

---

## 3. Divergencias en Cumulative Volume Delta (CVD)

### Fundamento
El CVD mide la diferencia acumulada neta entre órdenes a mercado compradoras (*market buys*) y órdenes a mercado vendedoras (*market sells*).

### Patologías de Mercado Operables:
1. **Divergencia de Absorción Bajista:** El precio marca un nuevo máximo local, pero el CVD marca un máximo descendente. Señal: Grandes participantes están absorbiendo las compras mediante órdenes límite pasivas (*limit sells*).
2. **Divergencia de Absorción Alcista:** El precio marca un nuevo mínimo local, pero el CVD se mantiene plano o ascendente. Señal: Absorción institucional activa mediante órdenes límite de compra.

---

## 4. Opening Range Breakout (ORB) Institucional

### Configuración:
* Intervalo del rango inicial: Primeros 15 a 30 minutos de la apertura del mercado (ej. 09:30 - 10:00 EST para Equities, o inicio de sesión Londres 08:00 UTC para Forex).
* **Confirmación de Ruptura Válida:**
  1. Ruptura del rango con incremento de volumen $> 1.5\times$ la media móvil de volumen de 20 periodos.
  2. El precio debe consolidar por encima del *Value Area High* (VAH) para posiciones largas, o por debajo del *Value Area Low* (VAL) para posiciones cortas.
  3. Exponente de Hurst $H > 0.55$ confirmando persistencia direccional.

