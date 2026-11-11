"""Verification test for Step 7: Full-screen trading terminal layout,

Sidebar Quant Chat Advisor with suggested prompts, and standalone Configuration Popover.
"""
import py_compile
from pathlib import Path


def test_app_compilation():
    """Verify app.py compiles cleanly with standard python py_compile."""
    app_path = Path("app.py")
    assert app_path.exists(), "app.py does not exist"
    py_compile.compile(str(app_path), doraise=True)
    print("  [PASS] app.py compiles cleanly.")


def test_fullscreen_chart_layout():
    """Verify trading terminal height and full width css in app.py."""
    content = Path("app.py").read_text()
    
    # Check full width CSS
    assert ".block-container" in content, "Missing .block-container CSS"
    assert "max-width: 100% !important;" in content, "Missing full-width canvas CSS"
    
    # Check chart height is large for full-screen view (780px)
    assert "components.html(chart_html, height=780)" in content, "Chart height should be 780px"
    print("  [PASS] Full-screen terminal chart container verified.")


def test_sidebar_chat_advisor_and_suggestions():
    """Verify quant advisor is in the sidebar with suggested prompt chips."""
    content = Path("app.py").read_text()
    
    # Check Quant Advisor header in sidebar
    assert "ASESOR CUANTITATIVO" in content, "Missing ASESOR CUANTITATIVO in sidebar"
    
    # Check suggested prompts
    assert "📋 Plan Trading" in content, "Missing '📋 Plan Trading' suggestion button"
    assert "🔍 Microestructura" in content, "Missing '🔍 Microestructura' suggestion button"
    assert "🛡️ Veredicto CRO" in content, "Missing '🛡️ Veredicto CRO' suggestion button"
    assert "💧 Zonas Liquidez" in content, "Missing '💧 Zonas Liquidez' suggestion button"
    assert "sidebar_chat_input" in content, "Missing sidebar chat_input"
    print("  [PASS] Sidebar chat advisor and prompt suggestions verified.")


def test_configuration_popover():
    """Verify separate configuration popover with 4 required modular sections."""
    content = Path("app.py").read_text()
    
    # Check popover presence
    assert 'st.popover("⚙️ Configuración"' in content, "Missing st.popover('⚙️ Configuración')"
    
    # Check the 4 configuration sections
    assert "1. Capas de Microestructura" in content, "Missing Microstructure section in popover"
    assert "2. Pipeline en Vivo & Sincronización" in content, "Missing Live Pipeline section in popover"
    assert "3. Gestión de Capital" in content, "Missing Capital Management section in popover"
    assert "4. Configuración Gemini API" in content, "Missing Gemini API section in popover"
    
    # Check that main area only has 2 tabs (no chat tab cluttering main area)
    assert 'tab_chart, tab_committee = st.tabs(' in content, "Main area tabs should only be Chart and Committee"
    print("  [PASS] Configuration popover and clean main layout verified.")


if __name__ == "__main__":
    print("Running Step 7 Layout & UI Tests...")
    test_app_compilation()
    test_fullscreen_chart_layout()
    test_sidebar_chat_advisor_and_suggestions()
    test_configuration_popover()
    print("\nALL STEP 7 UI TESTS PASSED SUCCESSFULLY!")
