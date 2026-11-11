"""Step 1 Verification Test: Session Singleton, Query Tagging, and Git Sanitization."""

import os
import subprocess
from src.config.snowflake_session import (
    get_raw_connection,
    get_snowpark_session,
    close_all_shared_connections,
    set_active_query_tag
)


def test_git_hygiene_live_daemon_state_not_tracked():
    """Verify live_daemon_state.json is removed from git tracking."""
    result = subprocess.run(
        ["git", "ls-files", "live_daemon_state.json"],
        capture_output=True,
        text=True,
        check=True
    )
    assert result.stdout.strip() == "", "live_daemon_state.json must not be tracked in git index"


def test_gitignore_contains_required_patterns():
    """Verify .gitignore contains live_daemon_state.json and bronze buffer directories."""
    with open(".gitignore", "r", encoding="utf-8") as f:
        content = f.read()
    assert "live_daemon_state.json" in content, ".gitignore must ignore live_daemon_state.json"
    assert "data/bronze_buffer/" in content, ".gitignore must ignore data/bronze_buffer/"
    assert "data/live_state/" in content, ".gitignore must ignore data/live_state/"


def test_raw_connection_singleton_and_proxy_close():
    """Verify get_raw_connection() returns a singleton whose close() doesn't drop the session."""
    conn1 = get_raw_connection(query_tag="STEP1_TEST_RAW")
    with conn1.cursor() as cur:
        cur.execute("SELECT CURRENT_SESSION()")
        session_id_1 = cur.fetchone()[0]

    # Simulating a caller's 'finally: conn1.close()'
    conn1.close()

    # Second call should return the exact same active session
    conn2 = get_raw_connection()
    with conn2.cursor() as cur:
        cur.execute("SELECT CURRENT_SESSION()")
        session_id_2 = cur.fetchone()[0]

    assert session_id_1 == session_id_2, (
        f"Singleton failed: expected session {session_id_1}, got {session_id_2}"
    )


def test_query_tag_setting():
    """Verify query tag is dynamically updated."""
    conn = get_raw_connection(query_tag="DYNAMIC_TAG_TEST")
    with conn.cursor() as cur:
        cur.execute("SHOW PARAMETERS LIKE 'QUERY_TAG'")
        rows = cur.fetchall()
        val = next(r[1] for r in rows if r[0] == "QUERY_TAG")
    assert val == "DYNAMIC_TAG_TEST", f"Expected QUERY_TAG=DYNAMIC_TAG_TEST, got {val}"


def test_warehouse_is_xsmall_and_auto_suspend_60():
    """Verify PROYECTO warehouse is X-Small with auto-suspend = 60 seconds."""
    conn = get_raw_connection()
    with conn.cursor() as cur:
        cur.execute("SHOW WAREHOUSES LIKE 'PROYECTO'")
        cols = [c[0].lower() for c in cur.description]
        row = cur.fetchone()
        wh_info = dict(zip(cols, row))
    
    assert wh_info.get("size", "").lower() == "x-small", (
        f"Expected warehouse size X-Small, got {wh_info.get('size')}"
    )
    assert int(wh_info.get("auto_suspend", 0)) == 60, (
        f"Expected auto_suspend 60, got {wh_info.get('auto_suspend')}"
    )


if __name__ == "__main__":
    print("Running test_git_hygiene_live_daemon_state_not_tracked...")
    test_git_hygiene_live_daemon_state_not_tracked()
    print("  [PASS]")

    print("Running test_gitignore_contains_required_patterns...")
    test_gitignore_contains_required_patterns()
    print("  [PASS]")

    print("Running test_raw_connection_singleton_and_proxy_close...")
    test_raw_connection_singleton_and_proxy_close()
    print("  [PASS]")

    print("Running test_query_tag_setting...")
    test_query_tag_setting()
    print("  [PASS]")

    print("Running test_warehouse_is_xsmall_and_auto_suspend_60...")
    test_warehouse_is_xsmall_and_auto_suspend_60()
    print("  [PASS]")

    print("\nALL STEP 1 TESTS PASSED SUCCESSFULLY!")

