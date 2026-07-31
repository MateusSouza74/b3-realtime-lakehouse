"""
Terminal financeiro B3 - camada de apresentacao do lakehouse.

Le as tabelas Delta direto do volume compartilhado usando delta-rs (nenhuma JVM,
nenhum Spark aqui) e nao faz NENHUM calculo de negocio: todas as metricas
(variacao, media movel, volatilidade, volume por janela) chegam prontas da
camada gold. Se uma metrica esta errada, o bug esta no dbt, nao no dashboard.

Arquitetura de leitura (lambda simplificada):
  * preco ao vivo      -> bronze  (atualiza a cada 15s, junto com o streaming)
  * metricas e candles -> gold    (atualiza a cada 5 min, junto com a DAG)
Os dois horizontes ficam rotulados na tela para nao confundir quem le.

Design: paleta validada com o script de acessibilidade da skill de dataviz.
O par verde/vermelho tradicional de terminal financeiro FALHA o teste de
daltonismo (deltaE 4.1 em deuteranopia, piso 8). O par usado aqui (aqua/salmao)
passa, e mesmo assim a direcao nunca depende so da cor: sempre acompanha seta e
sinal explicito.
"""

from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

LAKE_ROOT = Path(os.getenv("LAKE_ROOT", "/data/delta"))
ALERTS_DIR = Path(os.getenv("ALERTS_PATH", "/data/alerts"))
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "10"))

BRONZE_QUOTES = LAKE_ROOT / "bronze" / "quotes"
GOLD_SNAPSHOT = LAKE_ROOT / "gold" / "gold_ticker_snapshot"
GOLD_CANDLES = LAKE_ROOT / "gold" / "gold_intraday_candles"
GOLD_SECTORS = LAKE_ROOT / "gold" / "gold_sector_performance"

TZ = ZoneInfo("America/Sao_Paulo")

# Paleta (modo escuro, superficie #1a1a19) - todos os valores validados.
SURFACE = "#1a1a19"
PLANE = "#0d0d0d"
INK = "#ffffff"
INK_2 = "#c3c2b7"
MUTED = "#898781"
GRID = "#2c2c2a"
AXIS = "#383835"
UP = "#199e70"      # alta   (aqua)
DOWN = "#e66767"    # baixa  (salmao)
NEUTRAL = "#383835"  # midpoint neutro do par divergente
SMA_9 = "#3987e5"   # slot categorico 1
SMA_20 = "#c98500"  # slot categorico 4
WARNING = "#fab219"
CRITICAL = "#d03b3b"
GOOD = "#0ca30c"

st.set_page_config(
    page_title="B3 Real-Time Lakehouse",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
    initial_sidebar_state="collapsed",
)

CSS = """
<style>
  .block-container { padding-top: 1.6rem; padding-bottom: 2rem; max-width: 1500px; }
  #MainMenu, footer, header { visibility: hidden; }

  .term-head { display:flex; align-items:baseline; gap:.9rem; flex-wrap:wrap;
               border-bottom:1px solid #2c2c2a; padding-bottom:.6rem; margin-bottom:.9rem; }
  .term-title { font-size:1.15rem; font-weight:700; letter-spacing:.06em; color:#ffffff; }
  .term-sub { font-size:.74rem; color:#898781; letter-spacing:.10em; text-transform:uppercase; }

  .chips { display:flex; gap:.45rem; flex-wrap:wrap; margin-left:auto; }
  .chip { font-size:.68rem; letter-spacing:.08em; text-transform:uppercase;
          padding:.2rem .5rem; border-radius:3px; border:1px solid rgba(255,255,255,.12);
          color:#c3c2b7; white-space:nowrap; font-variant-numeric:tabular-nums; }
  .chip b { color:#ffffff; font-weight:600; }
  .chip-ok   { border-color:#0ca30c; color:#0ca30c; }
  .chip-warn { border-color:#fab219; color:#fab219; }
  .chip-crit { border-color:#d03b3b; color:#d03b3b; }

  .tile { background:#1a1a19; border:1px solid rgba(255,255,255,.10); border-radius:4px;
          padding:.7rem .8rem; height:100%; }
  .tile-head { display:flex; justify-content:space-between; align-items:center; margin-bottom:.35rem; }
  .tile-tk { font-size:.9rem; font-weight:700; letter-spacing:.05em; color:#ffffff; }
  .tile-src { font-size:.6rem; letter-spacing:.09em; color:#898781; border:1px solid #383835;
              border-radius:2px; padding:.05rem .3rem; }
  .tile-price { font-size:1.55rem; font-weight:600; color:#ffffff; line-height:1.15;
                font-variant-numeric:tabular-nums; }
  .tile-delta { font-size:.86rem; font-weight:600; margin-top:.1rem;
                font-variant-numeric:tabular-nums; }
  .tile-delta small { font-weight:400; color:#898781; margin-left:.35rem; }
  .up   { color:#199e70; }
  .down { color:#e66767; }
  .flat { color:#c3c2b7; }
  .tile-meta { font-size:.66rem; color:#898781; letter-spacing:.05em; margin-top:.45rem;
               font-variant-numeric:tabular-nums; }
  .range-track { position:relative; height:3px; background:#2c2c2a; border-radius:2px; margin:.5rem 0 .25rem; }
  .range-mark { position:absolute; top:-2px; width:2px; height:7px; background:#c3c2b7; border-radius:1px; }

  .sec-title { font-size:.72rem; letter-spacing:.12em; text-transform:uppercase; color:#898781;
               margin:1.3rem 0 .5rem; border-bottom:1px solid #2c2c2a; padding-bottom:.3rem; }
  .note { font-size:.7rem; color:#898781; line-height:1.5; }
  .alert-line { font-size:.72rem; color:#c3c2b7; font-variant-numeric:tabular-nums;
                border-left:2px solid #d03b3b; padding-left:.5rem; margin-bottom:.3rem; }
  .run-line { font-size:.72rem; color:#c3c2b7; font-variant-numeric:tabular-nums;
              border-left:2px solid #0ca30c; padding-left:.5rem; margin-bottom:.3rem; }
  .run-line.bad { border-left-color:#d03b3b; }

  div[data-testid="stDataFrame"] { border:1px solid rgba(255,255,255,.10); border-radius:4px; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Leitura das tabelas Delta
# ---------------------------------------------------------------------------

def read_delta(path: Path, columns: list[str] | None = None, since: date | None = None) -> pd.DataFrame:
    """Le uma tabela Delta. Devolve DataFrame vazio se ela ainda nao existe.

    `since` aplica pruning na coluna de particao ingestion_date, para nao varrer
    dias antigos a cada refresh do dashboard.
    """
    try:
        from deltalake import DeltaTable
    except ImportError:
        return pd.DataFrame()

    if not (path / "_delta_log").exists():
        return pd.DataFrame()

    try:
        table = DeltaTable(str(path))
        if since is not None:
            try:
                import pyarrow.dataset as pads

                dataset = table.to_pyarrow_dataset()
                arrow = dataset.to_table(
                    columns=columns,
                    filter=pads.field("ingestion_date") >= since,
                )
                return arrow.to_pandas()
            except Exception:
                pass  # sem pruning: cai na leitura completa abaixo
        return table.to_pyarrow_table(columns=columns).to_pandas()
    except Exception as exc:  # noqa: BLE001 - dashboard nunca deve quebrar por leitura
        st.session_state.setdefault("read_errors", []).append(f"{path.name}: {exc}")
        return pd.DataFrame()


def read_jsonl(path: Path, limit: int = 5) -> list[dict]:
    if not path.exists():
        return []
    try:
        linhas = path.read_text(encoding="utf-8").strip().splitlines()
    except OSError:
        return []
    saida = []
    for linha in linhas[-limit:]:
        try:
            saida.append(json.loads(linha))
        except json.JSONDecodeError:
            continue
    return list(reversed(saida))


# ---------------------------------------------------------------------------
# Formatacao pt-BR
# ---------------------------------------------------------------------------

def num_ptbr(valor: float | None, dec: int = 2) -> str:
    if valor is None or pd.isna(valor):
        return "--"
    texto = f"{valor:,.{dec}f}"
    return texto.replace(",", "@").replace(".", ",").replace("@", ".")


def brl(valor: float | None) -> str:
    return "--" if valor is None or pd.isna(valor) else f"R$ {num_ptbr(valor)}"


def compacto(valor: float | None) -> str:
    if valor is None or pd.isna(valor):
        return "--"
    valor = float(valor)
    for limite, sufixo in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "k")):
        if abs(valor) >= limite:
            return f"{num_ptbr(valor / limite, 1)}{sufixo}"
    return num_ptbr(valor, 0)


def direcao(valor: float | None) -> tuple[str, str]:
    """Retorna (classe css, glifo). O glifo e a codificacao secundaria da cor."""
    if valor is None or pd.isna(valor) or abs(valor) < 1e-9:
        return "flat", "="
    return ("up", "▲") if valor > 0 else ("down", "▼")


def pct_assinado(valor: float | None) -> str:
    if valor is None or pd.isna(valor):
        return "--"
    return f"{'+' if valor > 0 else ''}{num_ptbr(valor)}%"


def idade_humana(segundos: float | None) -> str:
    if segundos is None or pd.isna(segundos):
        return "--"
    segundos = int(segundos)
    if segundos < 90:
        return f"{segundos}s"
    if segundos < 5400:
        return f"{segundos // 60}min"
    return f"{segundos // 3600}h{(segundos % 3600) // 60:02d}"


def mercado_aberto(agora: datetime | None = None) -> bool:
    agora = agora or datetime.now(TZ)
    return agora.weekday() < 5 and dtime(10, 0) <= agora.time() <= dtime(18, 0)


# ---------------------------------------------------------------------------
# Carga
# ---------------------------------------------------------------------------

agora = datetime.now(TZ)
hoje = agora.date()

snapshot = read_delta(GOLD_SNAPSHOT)
candles = read_delta(GOLD_CANDLES)
setores = read_delta(GOLD_SECTORS)

# Preco ao vivo direto da bronze: a gold so e reconstruida a cada 5 minutos.
ao_vivo = read_delta(
    BRONZE_QUOTES,
    columns=["ticker", "price", "ingestion_ts", "quote_ts", "source", "ingestion_date"],
    since=hoje - timedelta(days=1),
)
if not ao_vivo.empty:
    ao_vivo = (
        ao_vivo.sort_values("ingestion_ts")
        .groupby("ticker", as_index=False)
        .last()
        .set_index("ticker")
    )

alertas = read_jsonl(ALERTS_DIR / "alerts.jsonl", limit=5)
execucoes = read_jsonl(ALERTS_DIR / "pipeline_runs.jsonl", limit=5)


# ---------------------------------------------------------------------------
# Cabecalho + chips de status
# ---------------------------------------------------------------------------

def chip(rotulo: str, valor: str, classe: str = "") -> str:
    return f'<span class="chip {classe}">{rotulo} <b>{valor}</b></span>'


if not ao_vivo.empty:
    ultima_ingestao = pd.to_datetime(ao_vivo["ingestion_ts"].max(), utc=True)
    idade_stream = (pd.Timestamp.now(tz="UTC") - ultima_ingestao).total_seconds()
    fonte_dado = str(ao_vivo["source"].mode().iat[0]).upper()
elif not snapshot.empty:
    ultima_ingestao = pd.to_datetime(snapshot["last_ingestion_ts"].max(), utc=True)
    idade_stream = (pd.Timestamp.now(tz="UTC") - ultima_ingestao).total_seconds()
    fonte_dado = str(snapshot["source"].mode().iat[0]).upper()
else:
    idade_stream = None
    fonte_dado = "--"

if idade_stream is None:
    classe_stream = "chip-warn"
elif idade_stream < 90:
    classe_stream = "chip-ok"
elif idade_stream < 20 * 60:
    classe_stream = "chip-warn"
else:
    classe_stream = "chip-crit"

if execucoes:
    ultima = execucoes[0]
    classe_pipe = "chip-ok" if ultima.get("status") == "ok" else "chip-crit"
    txt_pipe = (
        f"{ultima.get('status', '?').upper()} · "
        f"{ultima.get('tests_pass', 0)}/{ultima.get('tests_pass', 0) + ultima.get('tests_fail', 0)} testes"
    )
else:
    classe_pipe, txt_pipe = "chip-warn", "aguardando 1a execucao"

chips = "".join(
    [
        chip("fonte", fonte_dado, "chip-ok" if fonte_dado == "BRAPI" else ""),
        chip("pregao", "aberto" if mercado_aberto(agora) else "fechado",
             "chip-ok" if mercado_aberto(agora) else ""),
        chip("ultimo tick", idade_humana(idade_stream), classe_stream),
        chip("pipeline", txt_pipe, classe_pipe),
        chip("agora", agora.strftime("%d/%m %H:%M:%S"), ""),
    ]
)

st.markdown(
    f"""
    <div class="term-head">
      <span class="term-title">B3 REAL-TIME LAKEHOUSE</span>
      <span class="term-sub">kafka · spark streaming · delta · dbt · airflow</span>
      <span class="chips">{chips}</span>
    </div>
    """,
    unsafe_allow_html=True,
)

if snapshot.empty:
    st.warning(
        "Camada gold ainda vazia. O streaming grava a bronze em segundos, mas a gold "
        "so aparece depois da primeira execucao da DAG `b3_batch_lakehouse` "
        "(agendada a cada 5 minutos). Confira em http://localhost:8080."
    )
    if ao_vivo.empty:
        st.info(
            "A bronze tambem esta vazia. Verifique os logs: "
            "`docker compose logs -f producer spark-streaming`."
        )

# ---------------------------------------------------------------------------
# Tiles por ticker
# ---------------------------------------------------------------------------

if not snapshot.empty:
    snapshot = snapshot.sort_values("ticker").reset_index(drop=True)
    colunas = st.columns(min(len(snapshot), 4), gap="small")

    for posicao, linha in snapshot.iterrows():
        ticker = linha["ticker"]

        # Preco ao vivo da bronze quando disponivel; senao o da gold.
        preco = linha["last_price"]
        rotulo_preco = "gold"
        if ticker in getattr(ao_vivo, "index", []):
            preco = float(ao_vivo.loc[ticker, "price"])
            rotulo_preco = "bronze"

        base = linha["previous_close"]
        var_pct = (preco / base - 1) * 100 if base else None
        var_abs = preco - base if base else None
        classe, glifo = direcao(var_pct)

        posicao_faixa = linha.get("day_range_position_pct")
        marcador = 0.0 if pd.isna(posicao_faixa) else max(0.0, min(100.0, float(posicao_faixa)))

        with colunas[posicao % len(colunas)]:
            st.markdown(
                f"""
                <div class="tile">
                  <div class="tile-head">
                    <span class="tile-tk">{ticker}</span>
                    <span class="tile-src">{str(linha.get('source', '')).upper()} · {rotulo_preco}</span>
                  </div>
                  <div class="tile-price">{brl(preco)}</div>
                  <div class="tile-delta {classe}">{glifo} {pct_assinado(var_pct)}
                    <small>{'+' if (var_abs or 0) > 0 else ''}{num_ptbr(var_abs)}</small>
                  </div>
                  <div class="range-track"><div class="range-mark" style="left:{marcador:.1f}%"></div></div>
                  <div class="tile-meta">
                    MIN {num_ptbr(linha.get('day_low'))} · MAX {num_ptbr(linha.get('day_high'))}<br>
                    VOL {compacto(linha.get('volume_cumulative'))} ·
                    VOL20 {num_ptbr(linha.get('volatility_pct_20'))}% ·
                    ATRASO {idade_humana(linha.get('feed_delay_seconds'))}
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

# ---------------------------------------------------------------------------
# Serie intraday: candles + volume (dois eixos EMPILHADOS, nunca eixo duplo)
# ---------------------------------------------------------------------------

st.markdown('<div class="sec-title">Serie intraday · candles de 1 minuto</div>', unsafe_allow_html=True)

if candles.empty:
    st.markdown('<div class="note">Sem candles ainda.</div>', unsafe_allow_html=True)
else:
    tickers = sorted(candles["ticker"].unique())
    esquerda, direita = st.columns([3, 1])
    with esquerda:
        selecionado = st.radio("Papel", tickers, horizontal=True, label_visibility="collapsed")
    with direita:
        janela = st.selectbox(
            "Janela", ["Ultimas 2h", "Ultimas 6h", "Pregao inteiro"],
            index=0, label_visibility="collapsed",
        )

    serie = candles[candles["ticker"] == selecionado].copy()
    serie["window_start"] = pd.to_datetime(serie["window_start"])
    serie = serie.sort_values("window_start")

    if janela != "Pregao inteiro" and not serie.empty:
        horas = 2 if janela == "Ultimas 2h" else 6
        corte = serie["window_start"].max() - pd.Timedelta(hours=horas)
        serie = serie[serie["window_start"] >= corte]

    if serie.empty:
        st.markdown('<div class="note">Sem dado na janela escolhida.</div>', unsafe_allow_html=True)
    else:
        cores_volume = [
            UP if fechamento >= abertura else DOWN
            for fechamento, abertura in zip(serie["close_price"], serie["open_price"])
        ]

        figura = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            row_heights=[0.74, 0.26], vertical_spacing=0.05,
        )

        figura.add_trace(
            go.Candlestick(
                x=serie["window_start"],
                open=serie["open_price"], high=serie["high_price"],
                low=serie["low_price"], close=serie["close_price"],
                name="OHLC",
                increasing=dict(line=dict(color=UP, width=1), fillcolor=UP),
                decreasing=dict(line=dict(color=DOWN, width=1), fillcolor=DOWN),
                showlegend=False,
                hovertext=[
                    f"abertura {num_ptbr(a)} · maxima {num_ptbr(h)}<br>"
                    f"minima {num_ptbr(l)} · fechamento {num_ptbr(c)}<br>{n} leituras"
                    for a, h, l, c, n in zip(
                        serie["open_price"], serie["high_price"],
                        serie["low_price"], serie["close_price"], serie["n_ticks"],
                    )
                ],
                hoverinfo="x+text",
            ),
            row=1, col=1,
        )

        for coluna, cor, rotulo in (("sma_9", SMA_9, "MMS 9"), ("sma_20", SMA_20, "MMS 20")):
            if coluna in serie:
                figura.add_trace(
                    go.Scatter(
                        x=serie["window_start"], y=serie[coluna],
                        mode="lines", name=rotulo,
                        line=dict(color=cor, width=2),
                        hovertemplate=rotulo + " %{y:.2f}<extra></extra>",
                    ),
                    row=1, col=1,
                )

        figura.add_trace(
            go.Bar(
                x=serie["window_start"], y=serie["volume_delta"],
                name="Volume", marker=dict(color=cores_volume, line=dict(width=0)),
                showlegend=False,
                hovertemplate="volume %{y:,.0f}<extra></extra>",
            ),
            row=2, col=1,
        )

        eixo = dict(gridcolor=GRID, zerolinecolor=AXIS, linecolor=AXIS,
                    tickfont=dict(color=MUTED, size=11), title_font=dict(color=MUTED, size=11))
        figura.update_layout(
            height=430,
            paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
            font=dict(family="system-ui, -apple-system, 'Segoe UI', sans-serif", color=INK_2, size=12),
            margin=dict(l=10, r=10, t=34, b=10),
            hovermode="x unified",
            hoverlabel=dict(bgcolor=PLANE, bordercolor=AXIS, font=dict(color=INK, size=12)),
            legend=dict(orientation="h", yanchor="bottom", y=1.01, x=0,
                        bgcolor="rgba(0,0,0,0)", font=dict(color=INK_2, size=11)),
            bargap=0.25,  # respiro entre as barras de volume
            xaxis_rangeslider_visible=False,
            dragmode="pan",
        )
        figura.update_xaxes(**eixo, showspikes=True, spikecolor=MUTED, spikethickness=1,
                            spikedash="dot", spikemode="across")
        figura.update_yaxes(**eixo)
        figura.update_yaxes(title_text="R$", row=1, col=1)
        figura.update_yaxes(title_text="vol/min", row=2, col=1)

        st.plotly_chart(figura, use_container_width=True, config={"displayModeBar": False})

        st.markdown(
            f'<div class="note">Volume por janela = volume acumulado do dia menos o da janela '
            f'anterior (a API entrega apenas o acumulado). Ultima janela: '
            f'{serie["window_start"].max():%d/%m %H:%M} · {len(serie)} candles na tela.</div>',
            unsafe_allow_html=True,
        )

# ---------------------------------------------------------------------------
# Performance setorial + tabela (a tabela e a via de acesso alternativa a cor)
# ---------------------------------------------------------------------------

coluna_setor, coluna_tabela = st.columns([1, 2], gap="medium")

with coluna_setor:
    st.markdown('<div class="sec-title">Variacao por setor</div>', unsafe_allow_html=True)
    if setores.empty:
        st.markdown('<div class="note">Sem dado setorial ainda.</div>', unsafe_allow_html=True)
    else:
        dados = setores.sort_values("avg_change_pct")
        figura_setor = go.Figure(
            go.Bar(
                x=dados["avg_change_pct"], y=dados["sector"], orientation="h",
                marker=dict(
                    color=[UP if v > 0 else DOWN if v < 0 else NEUTRAL for v in dados["avg_change_pct"]],
                    line=dict(width=0),
                ),
                text=[pct_assinado(v) for v in dados["avg_change_pct"]],
                textposition="outside",
                textfont=dict(color=INK_2, size=11),
                hovertemplate="%{y}<br>media %{x:.2f}%<extra></extra>",
            )
        )
        figura_setor.update_layout(
            height=max(180, 46 * len(dados)),
            paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
            font=dict(family="system-ui, -apple-system, 'Segoe UI', sans-serif", color=INK_2, size=12),
            margin=dict(l=10, r=52, t=10, b=10),
            showlegend=False, bargap=0.35,
            hoverlabel=dict(bgcolor=PLANE, bordercolor=AXIS, font=dict(color=INK)),
        )
        figura_setor.update_xaxes(gridcolor=GRID, zerolinecolor=AXIS, tickfont=dict(color=MUTED, size=11),
                                  ticksuffix="%")
        figura_setor.update_yaxes(gridcolor="rgba(0,0,0,0)", tickfont=dict(color=INK_2, size=11))
        st.plotly_chart(figura_setor, use_container_width=True, config={"displayModeBar": False})

with coluna_tabela:
    st.markdown('<div class="sec-title">Posicao consolidada (camada gold)</div>', unsafe_allow_html=True)
    if snapshot.empty:
        st.markdown('<div class="note">Sem dado consolidado ainda.</div>', unsafe_allow_html=True)
    else:
        tabela = pd.DataFrame(
            {
                "Papel": snapshot["ticker"],
                "Setor": snapshot["sector"],
                "Ultimo": snapshot["last_price"].map(lambda v: num_ptbr(v)),
                # Seta + sinal: a direcao nunca depende so da cor.
                "Var %": [f"{direcao(v)[1]} {pct_assinado(v)}" for v in snapshot["change_pct"]],
                "Minima": snapshot["day_low"].map(lambda v: num_ptbr(v)),
                "Maxima": snapshot["day_high"].map(lambda v: num_ptbr(v)),
                "Volume": snapshot["volume_cumulative"].map(compacto),
                "MMS 9": snapshot["sma_9"].map(lambda v: num_ptbr(v)),
                "MMS 20": snapshot["sma_20"].map(lambda v: num_ptbr(v)),
                "Volat. 20": snapshot["volatility_pct_20"].map(lambda v: f"{num_ptbr(v)}%"),
                "Leituras": snapshot["n_ticks_dia"],
                "Atraso feed": snapshot["feed_delay_seconds"].map(idade_humana),
                "Fonte": snapshot["source"],
            }
        )
        st.dataframe(tabela, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------------------
# Observabilidade: execucoes do pipeline e alertas
# ---------------------------------------------------------------------------

coluna_runs, coluna_alertas = st.columns(2, gap="medium")

with coluna_runs:
    st.markdown('<div class="sec-title">Ultimas execucoes do pipeline</div>', unsafe_allow_html=True)
    if not execucoes:
        st.markdown('<div class="note">Nenhuma execucao registrada ainda.</div>', unsafe_allow_html=True)
    for execucao in execucoes:
        ruim = execucao.get("status") != "ok"
        st.markdown(
            f'<div class="run-line{" bad" if ruim else ""}">'
            f'{str(execucao.get("finished_at", ""))[:19].replace("T", " ")} · '
            f'{str(execucao.get("status", "?")).upper()} · '
            f'{execucao.get("models", 0)} modelos · '
            f'{execucao.get("tests_pass", 0)} testes ok · '
            f'{execucao.get("tests_fail", 0)} falhas · '
            f'{execucao.get("elapsed_seconds", 0)}s</div>',
            unsafe_allow_html=True,
        )

with coluna_alertas:
    st.markdown('<div class="sec-title">Alertas</div>', unsafe_allow_html=True)
    if not alertas:
        st.markdown('<div class="note">Nenhum alerta registrado.</div>', unsafe_allow_html=True)
    for alerta in alertas:
        st.markdown(
            f'<div class="alert-line">{str(alerta.get("detected_at", ""))[:19].replace("T", " ")} · '
            f'{alerta.get("dag_id", "?")}.{alerta.get("task_id", "?")} '
            f'(tentativa {alerta.get("try_number", "?")})<br>'
            f'{str(alerta.get("exception", ""))[:160]}</div>',
            unsafe_allow_html=True,
        )

# ---------------------------------------------------------------------------
# Rodape
# ---------------------------------------------------------------------------

st.markdown(
    f"""
    <div class="note" style="margin-top:1.6rem; border-top:1px solid #2c2c2a; padding-top:.7rem;">
      Fonte: API REST publica da brapi.dev (plano gratuito, cotacao com atraso) ou simulador local -
      sempre identificado no campo <code>source</code> de todas as camadas.
      Preco dos tiles vem da bronze (atualiza a cada 15s); metricas, candles e setores vem da gold
      (reconstruida a cada 5 min pelo Airflow). Projeto de portfolio, nao e recomendacao de investimento.
      Atualizacao automatica a cada {REFRESH_SECONDS}s.
    </div>
    """,
    unsafe_allow_html=True,
)

if st.session_state.get("read_errors"):
    with st.expander("Erros de leitura do lakehouse"):
        for erro in st.session_state["read_errors"][-10:]:
            st.code(erro)

time.sleep(REFRESH_SECONDS)
st.rerun()
