{#
    SQL ad-hoc no lakehouse, sem precisar de cliente SQL nenhum:

      dbt run-operation show_query --args "{sql: 'select * from gold.gold_ticker_snapshot'}"

    Util para depurar o pipeline (conferir contagem por camada, olhar o ultimo
    tick, comparar bronze e silver) e para checar rapidamente se o metastore esta
    consistente.
#}
{% macro show_query(sql, max_rows=50) %}

    {% if execute %}
        {% set resultado = run_query(sql) %}
        {% do resultado.print_table(max_rows=max_rows, max_columns=20, max_column_width=30) %}
        {% do log("linhas retornadas: " ~ resultado | length, info=True) %}
    {% endif %}

{% endmacro %}
