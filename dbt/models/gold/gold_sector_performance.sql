{{ config(materialized = 'table') }}

/*
    gold_sector_performance
    -----------------------
    Performance por setor economico, usando o dado de referencia do seed
    dim_tickers. Uma linha por setor.

    Com os 4 tickers de sandbox (sem token) sao 4 setores com 1 papel cada; com
    token e mais tickers no seed, a leitura setorial fica de fato interessante.
*/

select
    sector,
    count(*)                              as n_tickers,
    round(avg(change_pct), 4)             as avg_change_pct,
    round(max(change_pct), 4)             as best_change_pct,
    round(min(change_pct), 4)             as worst_change_pct,
    max_by(ticker, change_pct)            as best_ticker,
    min_by(ticker, change_pct)            as worst_ticker,
    sum(volume_cumulative)                as total_volume,
    round(avg(volatility_pct_20), 4)      as avg_volatility_pct_20,
    max(refreshed_at)                     as refreshed_at
from {{ ref('gold_ticker_snapshot') }}
where sector is not null
  and change_pct is not null
group by sector
