"""
Producer de cotacoes da B3 -> Kafka.

A B3 nao oferece feed WebSocket gratuito de tempo real (dado real-time de bolsa
e produto pago). Entao "streaming" aqui e implementado como POLLING em alta
frequencia contra a API REST da brapi.dev: cada leitura vira um evento no Kafka.

Do ponto de vista do resto da arquitetura (Kafka -> Spark Structured Streaming
-> Delta) nao ha diferenca nenhuma: o que muda e a origem dos eventos.

Modos de operacao (env SOURCE_MODE):
    brapi     -> sempre a API real
    simulated -> random walk local (demo 24/7, nao consome cota da API)
    auto      -> brapi durante o pregao da B3, simulado fora dele  [default]

Todo evento carrega o campo `source` ("brapi" ou "simulated"), que e propagado
ate a camada gold. Dado real e dado simulado nunca se confundem.
"""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import date, datetime, time as dtime, timezone
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import requests
from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "b3.quotes.raw")
TOPIC_PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "3"))
TOPIC_RETENTION_HOURS = int(os.getenv("TOPIC_RETENTION_HOURS", "24"))

TICKERS = [t.strip().upper() for t in os.getenv("TICKERS", "PETR4,VALE3,ITUB4,MGLU3").split(",") if t.strip()]
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "15"))

SOURCE_MODE = os.getenv("SOURCE_MODE", "auto").strip().lower()
BRAPI_BASE_URL = os.getenv("BRAPI_BASE_URL", "https://brapi.dev/api")
BRAPI_TOKEN = os.getenv("BRAPI_TOKEN", "").strip()
# Free = 1 ticker por chamada | Startup = 10 | Pro = 20
BRAPI_TICKERS_PER_REQUEST = max(1, int(os.getenv("BRAPI_TICKERS_PER_REQUEST", "1")))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT_SECONDS", "10"))

MARKET_TZ = ZoneInfo(os.getenv("MARKET_TZ", "America/Sao_Paulo"))
MARKET_OPEN = dtime(10, 0)
MARKET_CLOSE = dtime(18, 0)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-7s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("producer")

# Precos de partida do simulador (usados so quando a API real nao esta
# disponivel para semear o random walk).
FALLBACK_PRICES = {
    "PETR4": 38.50,
    "VALE3": 61.20,
    "ITUB4": 35.80,
    "MGLU3": 9.40,
    "BBAS3": 27.30,
    "WEGE3": 42.10,
    "B3SA3": 12.85,
    "ABEV3": 13.60,
}

_shutdown = False


def _handle_signal(signum, _frame):
    global _shutdown
    log.info("sinal %s recebido, encerrando...", signum)
    _shutdown = True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def market_is_open(now: datetime | None = None) -> bool:
    """Pregao regular da B3: dias uteis, 10h-18h BRT. Nao trata feriados."""
    now = now or datetime.now(MARKET_TZ)
    if now.weekday() >= 5:
        return False
    return MARKET_OPEN <= now.time() <= MARKET_CLOSE


def chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _as_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Fonte 1: API real da brapi.dev
# ---------------------------------------------------------------------------

class BrapiClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "b3-realtime-lakehouse/1.0"
        if BRAPI_TOKEN:
            self.session.headers["Authorization"] = f"Bearer {BRAPI_TOKEN}"
        self.request_count = 0

    def fetch(self, tickers: list[str]) -> list[dict[str, Any]]:
        """Uma chamada HTTP por lote de tickers (lote = 1 no plano Free)."""
        results: list[dict[str, Any]] = []
        for batch in chunked(tickers, BRAPI_TICKERS_PER_REQUEST):
            url = f"{BRAPI_BASE_URL}/quote/{','.join(batch)}"
            response = self.session.get(url, timeout=HTTP_TIMEOUT)
            self.request_count += 1
            if response.status_code == 429:
                raise RuntimeError("brapi retornou 429: cota do plano estourada ou rate limit")
            response.raise_for_status()
            payload = response.json()
            batch_results = payload.get("results") or []
            if len(batch_results) < len(batch):
                encontrados = {r.get("symbol") for r in batch_results}
                log.warning("tickers sem retorno da brapi: %s", sorted(set(batch) - encontrados))
            results.extend(batch_results)
        return results

    @staticmethod
    def to_event(raw: dict[str, Any], cycle_id: str) -> dict[str, Any]:
        return {
            "event_id": str(uuid.uuid4()),
            "ticker": raw.get("symbol"),
            "short_name": raw.get("shortName") or raw.get("longName"),
            "currency": raw.get("currency") or "BRL",
            "price": _as_float(raw.get("regularMarketPrice")),
            "previous_close": _as_float(raw.get("regularMarketPreviousClose")),
            "day_open": _as_float(raw.get("regularMarketOpen")),
            "day_high": _as_float(raw.get("regularMarketDayHigh")),
            "day_low": _as_float(raw.get("regularMarketDayLow")),
            "volume": int(raw.get("regularMarketVolume") or 0),
            "change_pct_api": _as_float(raw.get("regularMarketChangePercent")),
            "market_cap": _as_float(raw.get("marketCap")),
            # regularMarketTime = horario da cotacao na origem. A diferenca entre
            # ele e ingestion_ts e o atraso real do feed, medido na camada gold.
            "quote_ts": raw.get("regularMarketTime"),
            "ingestion_ts": utc_now_iso(),
            "source": "brapi",
            "poll_cycle_id": cycle_id,
        }


# ---------------------------------------------------------------------------
# Fonte 2: simulador (random walk geometrico)
# ---------------------------------------------------------------------------

class PriceSimulator:
    """Mantem o estado intraday de cada ticker: abertura, maxima, minima, volume."""

    SIGMA = 0.0006  # desvio padrao do retorno por tick

    def __init__(self, tickers: list[str]) -> None:
        self.state: dict[str, dict[str, Any]] = {}
        for ticker in tickers:
            self._reset(ticker, FALLBACK_PRICES.get(ticker, 20.0))

    def _reset(self, ticker: str, price: float) -> None:
        self.state[ticker] = {
            "price": price,
            "previous_close": price,
            "day_open": price,
            "day_high": price,
            "day_low": price,
            "volume": random.randint(200_000, 2_000_000),
            "trading_date": datetime.now(MARKET_TZ).date(),
        }

    def seed_from_real(self, events: list[dict[str, Any]]) -> None:
        """Semeia o random walk com os precos reais, quando disponiveis."""
        for event in events:
            ticker, price = event.get("ticker"), event.get("price")
            if ticker in self.state and price:
                s = self.state[ticker]
                s["price"] = price
                s["previous_close"] = event.get("previous_close") or price
                s["day_open"] = event.get("day_open") or price
                s["day_high"] = max(event.get("day_high") or price, price)
                s["day_low"] = min(event.get("day_low") or price, price)
                s["volume"] = event.get("volume") or s["volume"]

    def tick(self, ticker: str, cycle_id: str) -> dict[str, Any]:
        s = self.state[ticker]

        today: date = datetime.now(MARKET_TZ).date()
        if s["trading_date"] != today:
            self._reset(ticker, s["price"])
            s = self.state[ticker]

        drift = random.gauss(0, self.SIGMA)
        if random.random() < 0.01:  # eventos raros de maior amplitude
            drift *= 5
        s["price"] = round(max(0.01, s["price"] * (1 + drift)), 2)
        s["day_high"] = max(s["day_high"], s["price"])
        s["day_low"] = min(s["day_low"], s["price"])
        s["volume"] += random.randint(5_000, 60_000)

        return {
            "event_id": str(uuid.uuid4()),
            "ticker": ticker,
            "short_name": ticker,
            "currency": "BRL",
            "price": s["price"],
            "previous_close": round(s["previous_close"], 2),
            "day_open": round(s["day_open"], 2),
            "day_high": round(s["day_high"], 2),
            "day_low": round(s["day_low"], 2),
            "volume": s["volume"],
            "change_pct_api": round((s["price"] / s["previous_close"] - 1) * 100, 4),
            "market_cap": None,
            "quote_ts": utc_now_iso(),
            "ingestion_ts": utc_now_iso(),
            "source": "simulated",
            "poll_cycle_id": cycle_id,
        }


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

def ensure_topic() -> None:
    """Cria o topico explicitamente (particionado por ticker) em vez de confiar
    no auto-create, que criaria 1 particao sem retencao definida."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    new_topic = NewTopic(
        KAFKA_TOPIC,
        num_partitions=TOPIC_PARTITIONS,
        replication_factor=1,
        config={
            "retention.ms": str(TOPIC_RETENTION_HOURS * 3600 * 1000),
            "cleanup.policy": "delete",
        },
    )
    for name, future in admin.create_topics([new_topic]).items():
        try:
            future.result()
            log.info("topico '%s' criado (%s particoes, retencao %sh)", name, TOPIC_PARTITIONS, TOPIC_RETENTION_HOURS)
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                log.info("topico '%s' ja existe", name)
            else:
                raise


def delivery_report(err, msg) -> None:
    if err is not None:
        log.error("falha ao publicar em %s: %s", msg.topic() if msg else "?", err)


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "b3-quotes-producer",
            "linger.ms": 50,
            "compression.type": "snappy",
            "enable.idempotence": True,
            "acks": "all",
        }
    )


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------

def resolve_mode() -> str:
    if SOURCE_MODE in ("brapi", "simulated"):
        return SOURCE_MODE
    return "brapi" if market_is_open() else "simulated"


def collect_events(mode: str, brapi: BrapiClient, simulator: PriceSimulator, cycle_id: str) -> tuple[list[dict], str]:
    """Retorna (eventos, modo_efetivo). Em 'auto', cai para simulado se a API falhar."""
    if mode == "simulated":
        return [simulator.tick(t, cycle_id) for t in TICKERS], "simulated"

    try:
        raw_results = brapi.fetch(TICKERS)
        events = [BrapiClient.to_event(r, cycle_id) for r in raw_results if r.get("symbol")]
        simulator.seed_from_real(events)
        return events, "brapi"
    except Exception as exc:  # noqa: BLE001 - qualquer falha de rede/API
        log.error("erro na brapi: %s", exc)
        if SOURCE_MODE == "auto":
            log.warning("caindo para modo simulado neste ciclo")
            return [simulator.tick(t, cycle_id) for t in TICKERS], "simulated"
        return [], "brapi"


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    log.info(
        "producer iniciando | tickers=%s | intervalo=%ss | modo=%s | token=%s",
        ",".join(TICKERS),
        POLL_INTERVAL_SECONDS,
        SOURCE_MODE,
        "sim" if BRAPI_TOKEN else "nao (sandbox)",
    )

    for attempt in range(1, 11):
        try:
            ensure_topic()
            break
        except Exception as exc:  # noqa: BLE001
            log.warning("kafka indisponivel (tentativa %s/10): %s", attempt, exc)
            time.sleep(3)
    else:
        log.error("kafka nao respondeu, abortando")
        sys.exit(1)

    producer = build_producer()
    brapi = BrapiClient()
    simulator = PriceSimulator(TICKERS)

    # Semeia o simulador com os precos reais no boot, para o random walk comecar
    # de um patamar plausivel mesmo com o mercado fechado.
    try:
        simulator.seed_from_real([BrapiClient.to_event(r, "seed") for r in brapi.fetch(TICKERS)])
        log.info("simulador semeado com precos reais da brapi")
    except Exception as exc:  # noqa: BLE001
        log.warning("nao foi possivel semear com precos reais (%s), usando fallback", exc)

    cycle = 0
    published_total = 0

    while not _shutdown:
        cycle += 1
        started = time.monotonic()
        cycle_id = str(uuid.uuid4())

        events, effective_mode = collect_events(resolve_mode(), brapi, simulator, cycle_id)

        for event in events:
            producer.produce(
                topic=KAFKA_TOPIC,
                key=str(event["ticker"]),
                value=json.dumps(event, ensure_ascii=False),
                callback=delivery_report,
            )
        producer.poll(0)
        producer.flush(10)
        published_total += len(events)

        elapsed = time.monotonic() - started
        log.info(
            "ciclo=%s fonte=%s publicados=%s/%s total=%s req_brapi=%s elapsed=%.2fs",
            cycle,
            effective_mode,
            len(events),
            len(TICKERS),
            published_total,
            brapi.request_count,
            elapsed,
        )

        # Sleep fatiado para responder rapido ao SIGTERM.
        remaining = max(0.0, POLL_INTERVAL_SECONDS - elapsed)
        while remaining > 0 and not _shutdown:
            step = min(1.0, remaining)
            time.sleep(step)
            remaining -= step

    log.info("flush final...")
    producer.flush(10)
    log.info("producer encerrado | ciclos=%s eventos=%s", cycle, published_total)


if __name__ == "__main__":
    main()
