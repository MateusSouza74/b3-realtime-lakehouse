{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['ticker', 'quote_ts'],
        partition_by = ['trading_date'],
        on_schema_change = 'append_new_columns'
    )
}}

/*
    silver_quotes
    -------------
    Grao: uma linha por (ticker, horario da cotacao na origem).

    Por que existe deduplicacao aqui: o producer faz polling a cada 15s, mas o
    feed gratuito da brapi tem atraso de ~30 min e so muda de valor de tempo em
    tempo. Varias leituras consecutivas devolvem EXATAMENTE a mesma cotacao.
    A bronze guarda todas as leituras (auditoria); a silver guarda a verdade.

    Estrategia incremental: merge por (ticker, quote_ts) com janela de reprocesso
    de 10 minutos - absorve evento atrasado sem varrer a bronze inteira.
    O `row_number` antes do merge e obrigatorio: MERGE do Delta falha se a fonte
    tiver mais de uma linha para a mesma chave.
*/

with eventos as (

    select *
    from {{ source('bronze', 'quotes') }}
    where ticker is not null
      and quote_ts is not null
      and price is not null
      and price > 0

    {% if is_incremental() %}
      and ingestion_ts >= (
            select coalesce(max(ingestion_ts) - interval 10 minutes, timestamp '1970-01-01')
            from {{ this }}
      )
    {% endif %}

),

tipado as (

    select
        event_id,
        ticker,
        short_name,
        currency,
        quote_ts,
        ingestion_ts,
        processing_ts,

        cast(price as double)          as price,
        cast(previous_close as double) as previous_close,
        cast(day_open as double)       as day_open,
        cast(day_high as double)       as day_high,
        cast(day_low as double)        as day_low,
        cast(volume as bigint)         as volume,
        cast(change_pct_api as double) as change_pct_api,

        -- Variacao recalculada em casa: nao dependemos do campo da API.
        round((price / nullif(previous_close, 0) - 1) * 100, 4) as change_pct,

        -- Metrica-chave deste projeto: o quanto o dado esta atrasado na origem.
        cast(unix_timestamp(ingestion_ts) - unix_timestamp(quote_ts) as int) as feed_delay_seconds,

        source,
        kafka_partition,
        kafka_offset,

        -- Data do pregao no fuso America/Sao_Paulo (spark.sql.session.timeZone).
        to_date(quote_ts) as trading_date

    from eventos

),

ordenado as (

    select
        *,
        row_number() over (
            partition by ticker, quote_ts
            order by ingestion_ts asc, kafka_offset asc
        ) as rn
    from tipado

)

select
    event_id,
    ticker,
    short_name,
    currency,
    quote_ts,
    ingestion_ts,
    processing_ts,
    price,
    previous_close,
    day_open,
    day_high,
    day_low,
    volume,
    change_pct_api,
    change_pct,
    feed_delay_seconds,
    source,
    kafka_partition,
    kafka_offset,
    trading_date
from ordenado
where rn = 1
