{{
    config(
        materialized = 'table',
        partition_by = ['trading_date']
    )
}}

/*
    gold_intraday_candles
    ---------------------
    Candles OHLCV de 1 minuto por ticker + metricas de janela.

    Detalhes de dominio que valem o comentario:

    * `volume` na API e ACUMULADO do dia. Volume por candle e a diferenca contra
      a janela anterior (volume_delta), com piso em zero para o caso de o feed
      corrigir o acumulado para baixo.
    * `volatility_pct_20` e o desvio padrao dos retornos de 1 minuto nas ultimas
      20 janelas, em %. Nao esta anualizado de proposito: para leitura intraday,
      anualizar so adiciona um fator constante e confunde.
    * Toda janela e particionada por (ticker, trading_date): media movel nunca
      atravessa o fechamento de um pregao para o outro.
*/

with ticks as (

    select *
    from {{ ref('silver_quotes') }}
    where trading_date >= date_sub(current_date(), {{ var('gold_lookback_days') }})

),

candles as (

    select
        ticker,
        date_trunc('minute', quote_ts)  as window_start,
        min_by(price, quote_ts)         as open_price,
        max(price)                      as high_price,
        min(price)                      as low_price,
        max_by(price, quote_ts)         as close_price,
        count(*)                        as n_ticks,
        max(volume)                     as volume_cumulative,
        round(avg(feed_delay_seconds), 1) as avg_feed_delay_seconds,
        max(source)                     as source,
        trading_date
    from ticks
    group by ticker, trading_date, date_trunc('minute', quote_ts)

),

com_lag as (

    select
        *,
        lag(close_price) over (
            partition by ticker, trading_date order by window_start
        ) as prev_close_price,
        lag(volume_cumulative) over (
            partition by ticker, trading_date order by window_start
        ) as prev_volume_cumulative
    from candles

),

retornos as (

    select
        *,
        greatest(coalesce(volume_cumulative - prev_volume_cumulative, 0), 0) as volume_delta,
        case
            when prev_close_price > 0
            then round((close_price / prev_close_price - 1) * 100, 4)
        end as return_pct
    from com_lag

)

select
    ticker,
    window_start,
    open_price,
    high_price,
    low_price,
    close_price,
    n_ticks,
    volume_cumulative,
    volume_delta,
    return_pct,

    round(avg(close_price) over (
        partition by ticker, trading_date order by window_start
        rows between 8 preceding and current row
    ), 4) as sma_9,

    round(avg(close_price) over (
        partition by ticker, trading_date order by window_start
        rows between 19 preceding and current row
    ), 4) as sma_20,

    round(stddev_samp(return_pct) over (
        partition by ticker, trading_date order by window_start
        rows between 19 preceding and current row
    ), 4) as volatility_pct_20,

    avg_feed_delay_seconds,
    source,
    current_timestamp() as refreshed_at,
    trading_date
from retornos
