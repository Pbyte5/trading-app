"""Playbook ingestion and vectorization using native Snowflake Cortex.

Chunks markdown files and calculates 1024-dimension embeddings directly in Snowflake.
"""

import re
import uuid
from pathlib import Path
from typing import Any, Dict, List

from src.config.snowflake_session import get_snowpark_session, get_raw_connection


def parse_playbook_markdown(file_path: Path) -> List[Dict[str, Any]]:
    """Parse a playbook markdown file into structured sections with metadata."""
    content = file_path.read_text(encoding="utf-8")
    lines = content.splitlines()

    # Extract Title and Header Metadata
    title = file_path.stem
    market_type = "ALL"
    strategy_horizon = "ALL"

    for line in lines[:10]:
        if line.startswith("# "):
            title = line.replace("# ", "").strip()
        elif "**Mercado:**" in line:
            raw_m = line.split("**Mercado:**")[-1].strip().upper()
            if "CRIPTO" in raw_m:
                market_type = "CRYPTO"
            elif "EQUITIES" in raw_m or "ACCIONES" in raw_m:
                market_type = "EQUITIES"
            elif "FOREX" in raw_m or "DIVISAS" in raw_m:
                market_type = "FOREX"
            else:
                market_type = "ALL"
        elif "**Horizonte:**" in line:
            raw_h = line.split("**Horizonte:**")[-1].strip().upper()
            if "INTRADAY" in raw_h:
                strategy_horizon = "INTRADAY"
            elif "HOLDER" in raw_h or "SWING" in raw_h:
                strategy_horizon = "SWING_HOLDER"
            else:
                strategy_horizon = "ALL"

    # Split into sections by '## '
    sections = re.split(r'\n(?=##\s+)', content)
    chunks = []

    for sec in sections:
        sec_clean = sec.strip()
        if not sec_clean:
            continue

        # Extract section header
        first_line = sec_clean.splitlines()[0]
        header = first_line.replace("#", "").strip() if first_line.startswith("#") else "General Overview"

        chunks.append({
            "playbook_id": str(uuid.uuid4()),
            "title": title,
            "market_type": market_type,
            "strategy_horizon": strategy_horizon,
            "section_header": header,
            "chunk_text": sec_clean
        })

    return chunks


def ingest_all_playbooks() -> None:
    """Ingest playbooks into Snowflake GOLD_QUANT_RAG.INSTITUTIONAL_PLAYBOOKS and compute Cortex embeddings."""
    print("=" * 70)
    print("INGESTA Y VECTORIZACIÓN CORTEX DE PLAYBOOKS INSTITUCIONALES (FASE 4)")
    print("=" * 70)

    project_root = Path(__file__).resolve().parent.parent.parent
    playbooks_dir = project_root / "playbooks"

    md_files = list(playbooks_dir.glob("*.md"))
    if not md_files:
        print("[!] No se encontraron playbooks en:", playbooks_dir)
        return

    all_chunks: List[Dict[str, Any]] = []
    for md_file in md_files:
        print(f"[*] Parseando playbook: {md_file.name}")
        chunks = parse_playbook_markdown(md_file)
        all_chunks.extend(chunks)

    print(f"[*] Total de fragmentos generados: {len(all_chunks)}")

    # Connect to Snowflake
    try:
        session = get_snowpark_session()
        use_snowpark = True
    except Exception:
        session = get_raw_connection()
        use_snowpark = False

    try:
        # Check if Cortex embeddings are available
        cortex_supported = True
        test_cortex_sql = "SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_1024('voyage-multilingual-2', 'test')"
        try:
            if use_snowpark:
                session.sql(test_cortex_sql).collect()
            else:
                with session.cursor() as cur:
                    cur.execute(test_cortex_sql)
        except Exception:
            cortex_supported = False
            print("[AVISO] Snowflake AI embeddings restringidos en cuenta de prueba.")
            print("[*] Ingestando fragmentos de playbooks con CHUNK_VECTOR = NULL (búsqueda léxica e indexación activas).")

        inserted = 0
        for chunk in all_chunks:
            p_id = chunk["playbook_id"]
            title = chunk["title"].replace("'", "''")
            market_type = chunk["market_type"]
            horizon = chunk["strategy_horizon"]
            header = chunk["section_header"].replace("'", "''")
            text = chunk["chunk_text"].replace("'", "''")

            vector_expr = (
                f"SNOWFLAKE.CORTEX.EMBED_TEXT_1024('voyage-multilingual-2', '{text}')"
                if cortex_supported
                else "NULL"
            )

            insert_sql = f"""
            MERGE INTO QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG.INSTITUTIONAL_PLAYBOOKS AS target
            USING (
                SELECT 
                    '{p_id}' AS PLAYBOOK_ID,
                    '{title}' AS TITLE,
                    '{market_type}' AS MARKET_TYPE,
                    '{horizon}' AS STRATEGY_HORIZON,
                    '{header}' AS SECTION_HEADER,
                    '{text}' AS CHUNK_TEXT,
                    {vector_expr} AS CHUNK_VECTOR,
                    PARSE_JSON('{{"source": "markdown_playbook"}}') AS METADATA
            ) AS src
            ON target.TITLE = src.TITLE AND target.SECTION_HEADER = src.SECTION_HEADER
            WHEN MATCHED THEN
                UPDATE SET
                    target.CHUNK_TEXT = src.CHUNK_TEXT,
                    target.CHUNK_VECTOR = src.CHUNK_VECTOR,
                    target.UPDATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (PLAYBOOK_ID, TITLE, MARKET_TYPE, STRATEGY_HORIZON, SECTION_HEADER, CHUNK_TEXT, CHUNK_VECTOR, METADATA)
                VALUES (src.PLAYBOOK_ID, src.TITLE, src.MARKET_TYPE, src.STRATEGY_HORIZON, src.SECTION_HEADER, src.CHUNK_TEXT, src.CHUNK_VECTOR, src.METADATA);
            """

            if use_snowpark:
                session.sql(insert_sql).collect()
            else:
                with session.cursor() as cur:
                    cur.execute(insert_sql)
            inserted += 1

        if cortex_supported:
            print(f"\n[OK] {inserted} fragmentos vectorizados con Snowflake Cortex (voyage-multilingual-2) e indexados con éxito.")
        else:
            print(f"\n[OK] {inserted} fragmentos institucionales indexados exitosamente en GOLD_QUANT_RAG.INSTITUTIONAL_PLAYBOOKS.")
    finally:
        session.close()


if __name__ == "__main__":
    ingest_all_playbooks()

