/*
    Cotacao com horario no futuro nao existe: se aparecer, e bug de fuso horario
    entre a API (UTC), o producer e o Spark (America/Sao_Paulo).

    Erro de timezone e uma das falhas mais silenciosas em pipeline financeiro -
    o dado parece certo, mas o candle cai na janela errada.
    Tolerancia de 5 minutos para diferenca de relogio.
*/

select
    ticker,
    quote_ts,
    ingestion_ts,
    source
from {{ ref('silver_quotes') }}
where quote_ts > current_timestamp() + interval 5 minutes
