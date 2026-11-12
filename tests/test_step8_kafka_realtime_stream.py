"""Verification test for Step 8: Kafka Real-Time Candlestick Streaming & WebSocket Gateway."""

import json
from pathlib import Path
import py_compile
import sys
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))


def test_docker_compose_kafka():
    """Verify docker-compose.kafka.yml exists and has valid configuration."""
    compose_path = Path("docker-compose.kafka.yml")
    assert compose_path.exists(), "docker-compose.kafka.yml does not exist"

    with open(compose_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert "services" in data, "Missing services in docker-compose"
    assert "redpanda" in data["services"], "Missing redpanda service"
    assert "console" in data["services"], "Missing console service"
    print("  [PASS] docker-compose.kafka.yml verified.")


def test_confluent_kafka_library():
    """Verify confluent_kafka is installed and ready."""
    import confluent_kafka
    assert hasattr(confluent_kafka, "Producer"), "confluent_kafka missing Producer"
    assert hasattr(confluent_kafka, "Consumer"), "confluent_kafka missing Consumer"
    print(f"  [PASS] confluent-kafka library verified (version {confluent_kafka.__version__}).")


def test_pipeline_compilation():
    """Verify pipeline files compile cleanly."""
    for p in ["src/pipeline/kafka_market_pipeline.py", "src/pipeline/ws_gateway.py"]:
        path = Path(p)
        assert path.exists(), f"File {p} does not exist"
        py_compile.compile(str(path), doraise=True)
    print("  [PASS] Kafka pipeline and WebSocket gateway compile cleanly.")


def test_producer_initialization():
    """Verify KafkaCandleProducer initializes with correct topics."""
    from src.pipeline.kafka_market_pipeline import KafkaCandleProducer

    received = []
    producer = KafkaCandleProducer(
        symbol="BTCUSDT",
        timeframe="5m",
        on_candle_update=lambda c: received.append(c)
    )

    assert producer.symbol == "BTCUSDT"
    assert producer.trade_topic == "market.crypto.trades.btcusdt"
    assert producer.candle_topic == "market.crypto.candles.btcusdt"

    # Test candle update simulation
    mock_kline = {
        "k": {
            "t": 1700000000000,
            "o": "65000.00",
            "h": "65100.00",
            "l": "64950.00",
            "c": "65050.00",
            "v": "15.42",
            "x": False
        }
    }
    producer.handle_kline(mock_kline)
    assert len(received) == 1
    assert received[0]["close"] == 65050.00
    assert received[0]["high"] == 65100.00
    assert received[0]["time"] == 1700000000
    print("  [PASS] KafkaCandleProducer candle aggregation verified.")


def test_chart_websocket_bridge_in_app():
    """Verify app.py includes the real-time sub-second WebSocket bridge."""
    content = Path("app.py").read_text(encoding="utf-8")
    assert "ws://localhost:8765/ws/candles/" in content or ":8765/ws/candles/" in content, "Missing WebSocket gateway URL in app.py"
    assert "candleSeries.update" in content, "Missing candleSeries.update in app.py"
    assert "live-stream-badge" in content, "Missing live-stream-badge in app.py"
    print("  [PASS] TradingView real-time candle streaming bridge in app.py verified.")


if __name__ == "__main__":
    print("Running Step 8 Kafka Real-Time Stream Tests...")
    test_docker_compose_kafka()
    test_confluent_kafka_library()
    test_pipeline_compilation()
    test_producer_initialization()
    test_chart_websocket_bridge_in_app()
    print("\nALL STEP 8 KAFKA STREAMING TESTS PASSED SUCCESSFULLY!")
