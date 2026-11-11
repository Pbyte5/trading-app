"""Institutional Cibersecurity & Guardrails Engine.

Provides deep defense against Prompt Injections, jailbreak attempts,
system prompt leakage, and strict domain boundaries while gracefully
handling greetings and courteous interactions.
"""

import re
from typing import Tuple, Dict, Any, Optional

# 1. Greetings & Courtesy Matchers
GREETING_PATTERNS = [
    r"^\s*(hola|buenos?\s+d[ií]as?|buenas?\s+(tardes?|noches?)|qu[eé]\s+tal|c[oó]mo\s+est[aá]s?)\b",
    r"^\s*(hey|hello|hi|saludos|buen\s+d[ií]a)\b",
    r"^\s*(gracias|muchas\s+gracias|ok\s+gracias|vale\s+gracias)\b",
    r"^\s*(adi[oó]s|chao|hasta\s+luego|nos\s+vemos)\b"
]

# 2. Heuristic Anti-Prompt Injection & Jailbreak Patterns
PROMPT_INJECTION_PATTERNS = [
    r"\b(ignore|ignora|olvida|deshazte|anula)\b.*?\b(instrucciones|instructions|reglas|rules|directrices|prompts?)\b",
    r"\b(act\s+as|act[uú]a\s+como|finge\s+ser)\b.*?\b(DAN|jailbreak|desarrollador|unrestricted|sin\s+l[ií]mites)\b",
    r"\b(show|display|reveal|revela|muestra|dime|imprime)\b.*?\b(system\s+prompt|prompt|instrucciones|instructions)\b",
    r"\b(repeat|repite)\b.*?\b(words\s+above|palabras\s+de\s+arriba|mandato|prompt)\b",
    r"\b(exfiltrate|extrae|dame|muestra|show)\b.*?\b(api[_-]?keys?|tokens?|claves?|contraseñas?|secrets?)\b",
    r"\b(snowflake_user|snowflake_password|gemini_api_key|private_key|connection\s+string)\b",
    r"<\s*/?(script|style|iframe|eval|prompt|system|context)\b",
    r"\b(drop\s+database|drop\s+table|delete\s+from|insert\s+into)\b",
    r"\b(bypass|salta|evade)\b.*?\b(seguridad|filtro|restricci[oó]n|guardrails?)\b"
]

# 3. Domain Whitelist Keywords (Business model, financial markets, quant methods)
DOMAIN_KEYWORDS = [
    "mercado", "inversi", "inver", "activo", "precio", "vela", "gr[aá]fico",
    "btc", "eth", "sol", "nvda", "aapl", "msft", "eur", "gbp", "jpy", "usd",
    "crypto", "cripto", "forex", "equity", "equities", "acci[oó]n", "acciones",
    "vwap", "poc", "vah", "val", "volumen", "cvd", "hurst", "atr", "volatilidad",
    "riesgo", "cro", "stop", "loss", "take", "profit", "kelly", "ratio", "r:r",
    "estrategia", "intrad[ií]a", "swing", "macro", "comit[eé]", "agente", "playbook",
    "snowflake", "medallion", "silver", "gold", "cortex", "modelo", "negocio",
    "capital", "cuenta", "portafolio", "ganancia", "p[eé]rdida", "tendencia", "reversi[oó]n",
    "order", "liquidez", "drawdown", "retorno", "rentabilidad", "ejecuci[oó]n"
]


def is_courtesy_greeting(text: str) -> bool:
    """Detect if the input is a standalone polite greeting or farewell."""
    cleaned = text.strip().lower()
    # If the text is short and matches greeting pattern
    if len(cleaned.split()) <= 6:
        for pattern in GREETING_PATTERNS:
            if re.search(pattern, cleaned, re.IGNORECASE):
                return True
    return False


def get_courtesy_response(text: str) -> str:
    """Generate a high-end institutional greeting response."""
    cleaned = text.strip().lower()
    if any(w in cleaned for w in ["gracias", "muchas gracias"]):
        return (
            "¡Con gusto! Estoy aquí para apoyarte en cualquier análisis cuantitativo, "
            "evaluación de microestructura o validación de riesgo. ¿Hay algún activo específico que desees explorar?"
        )
    if any(w in cleaned for w in ["adiós", "adios", "chao", "hasta luego"]):
        return (
            "Hasta pronto. La **Plataforma de Análisis de Mercado** continuará monitoreando "
            "los flujos de liquidez y regímenes en Snowflake. ¡Éxito en tus operaciones!"
        )
    return (
        "¡Hola! Buenos días. Soy el **Asistente Cuantitativo Institucional** de la "
        "Plataforma de Análisis de Mercado.\n\n"
        "Estoy a tu disposición para:\n"
        "- **Proponer oportunidades de inversión** con niveles de Stop Loss (ATR), Take Profit y R:R >= 2.0.\n"
        "- **Explicar las decisiones y vetos del Chief Risk Officer (CRO)**.\n"
        "- **Responder consultas sobre el modelo de negocio** institucional y gestión de capital por 1/4 Kelly.\n"
        "- **Ilustrar con gráficos** la lógica de microestructura (VWAP, POC, CVD, Hurst).\n\n"
        "¿Qué activo o consulta te gustaría revisar hoy?"
    )


def validate_user_input(text: str) -> Tuple[bool, str, str]:
    """Validate user input against prompt injections and domain boundaries.
    
    Returns:
        (is_allowed: bool, category: str, response_or_cleaned_query: str)
        category can be: 'INJECTION_DETECTED', 'GREETING', 'OFF_DOMAIN', 'VALID'
    """
    if not text or not text.strip():
        return False, "EMPTY", "Por favor ingresa una consulta."

    cleaned = text.strip()

    # Step 1: Prompt Injection / Jailbreak Filter
    for pattern in PROMPT_INJECTION_PATTERNS:
        if re.search(pattern, cleaned, re.IGNORECASE):
            warning = (
                "**Alerta de Seguridad Institucional:**\n\n"
                "La consulta contiene patrones de instrucción no permitidos (intento de evasión de directrices, "
                "meta-prompt probing o inyección de código). Esta plataforma opera bajo gobernanza estricta "
                "de seguridad de datos y preservación de capital. Tu interacción ha sido contenida de forma segura."
            )
            return False, "INJECTION_DETECTED", warning

    # Step 2: Courtesy Greeting Filter
    if is_courtesy_greeting(cleaned):
        return True, "GREETING", get_courtesy_response(cleaned)

    # Step 3: Domain Whitelist Filter
    lower_text = cleaned.lower()
    has_domain_keyword = any(k in lower_text for k in DOMAIN_KEYWORDS)
    
    # If the message does not have any domain keyword and is asking non-financial questions
    if not has_domain_keyword:
        off_domain_msg = (
            "**Alcance Exclusivo de Negocio:**\n\n"
            "Soy el Asesor Cuantitativo de la **Plataforma de Análisis de Mercado**. "
            "Mi especialidad se enfoca exclusivamente en:\n"
            "1. **Análisis cuantitativo de activos** (Crypto, Acciones, Forex).\n"
            "2. **Propuestas de inversión fundamentadas** con gestión de riesgo institucional.\n"
            "3. **Consultas sobre el modelo de negocio**, arquitectura Medallion en Snowflake y filtro de veto del CRO.\n\n"
            "¿Deseas evaluar la microestructura de algún activo o consultar una estrategia de inversión?"
        )
        return False, "OFF_DOMAIN", off_domain_msg

    # Passed all checks
    # Neutralize any XML tags inside query to prevent markup tampering
    sanitized = cleaned.replace("<", "&lt;").replace(">", "&gt;")
    return True, "VALID", sanitized
