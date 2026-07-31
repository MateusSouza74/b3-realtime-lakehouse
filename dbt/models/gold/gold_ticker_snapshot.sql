{{ config(materialized = 'table') }}

/*
    gold_ticker_snapshot
    --------------------
    Uma linha por ticker: o estado atual do papel, pronto para consumo direto
    pelo dashboard (sem nenhum calculo na camada de apresentacao).

    `seconds_since_last_tick` mede a saude do PIPELINE (quanto tempo desde a
    ultima linha ingerida) e `feed_delay_seconds` mede o atraso da FONTE.
    Sao duas coisas diferentes e as duas importam: a primeira acusa producer ou
    streaming caidos, a segunda e uma caracteristica do plano gratuito da API.
*/

with ultimo_tick as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by ticker
                order by ingestion_ts desc, kafka_offset desc
            ) as rn
        from {{ ref('silver_quotes') }}
        where trading_date >= date_sub(current_date(), {{ var('gold_lookback_days') }})
    ) ranqueado
    where rn = 1

),

ultima_candle as (

    select *
    from (
        select
            ticker,
            window_start,
            sma_9,
            sma_20,
            volatility_pct_20,
            row_number() over (partition by ticker order by window_start desc) as rn
        from {{ ref('gold_intraday_candles') }}
    ) ranqueado
    where rn = 1

),

agregado_do_dia as (

    select
        ticker,
        trading_date,
        count(*)    as n_ticks_dia,
        max(volume) as volume_dia
    from {{ ref('silver_quotes') }}
    group by ticker, trading_date

)

select
    tick.ticker,
    dim.company_name,
    dim.sector,
    dim.b3_index,

    tick.price                                        as last_price,
    tick.previous_close,
    round(tick.price - tick.previous_close, 4)         as change_abs,
    tick.change_pct,

    tick.day_open,
    tick.day_high,
    tick.day_low,
    -- Onde o preco atual esta dentro da faixa do dia (0% = minima, 100% = maxima).
    case
        when tick.day_high > tick.day_low
        then round((tick.price - tick.day_low) / (tick.day_high - tick.day_low) * 100, 2)
    end                                               as day_range_position_pct,

    tick.volume                                       as volume_cumulative,
    dia.n_ticks_dia,

    candle.sma_9,
    candle.sma_20,
    candle.volatility_pct_20,
    candle.window_start                               as last_candle_window,

    tick.quote_ts                                     as last_quote_ts,
    tick.ingestion_ts                                 as last_ingestion_ts,
    tick.feed_delay_seconds,
    cast(unix_timestamp(current_timestamp()) - unix_timestamp(tick.ingestion_ts) as int)
                                                      as seconds_since_last_tick,

    tick.source,
    current_timestamp()                               as refreshed_at,
    tick.trading_date

from ultimo_tick tick
left join {{ ref('dim_tickers') }} dim
       on dim.ticker = tick.ticker
left join ultima_candle candle
       on candle.ticker = tick.ticker
left join agregado_do_dia dia
       on dia.ticker = tick.ticker
      and dia.trading_date = tick.trading_date
