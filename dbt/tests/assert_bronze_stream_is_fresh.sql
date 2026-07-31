/*
    Alarme de pipeline parado.

    Se a bronze nao recebe evento novo ha mais de `max_feed_staleness_minutes`,
    algo morreu: producer, Kafka ou o job de streaming. Este teste falha, a task
    do Airflow falha e o `on_failure_callback` dispara o alerta.

    Diferente do atraso do feed (que e caracteristica do plano gratuito da API),
    isto aqui e falha de infraestrutura.
*/

select
    max(ingestion_ts)   as last_ingestion_ts,
    current_timestamp() as checked_at,
    cast(unix_timestamp(current_timestamp()) - unix_timestamp(max(ingestion_ts)) as int) as staleness_seconds
from {{ source('bronze', 'quotes') }}
having max(ingestion_ts) is null
    or max(ingestion_ts) < current_timestamp() - interval {{ var('max_feed_staleness_minutes') }} minutes
