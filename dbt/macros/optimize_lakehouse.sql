{#
    Manutencao das tabelas Delta.

    O streaming grava um arquivo novo a cada micro-batch (15s), o que gera o
    classico problema de small files: em 8h de pregao sao ~2.000 arquivos por dia
    de particao, e a leitura degrada. OPTIMIZE compacta esses arquivos.

    Roda por CAMINHO (delta.`/path`) e nao por nome de tabela, de proposito: assim
    o macro nao depende do registro no catalogo e pode ser chamado via
    `dbt run-operation`, que nao executa os hooks on-run-start.

    OPTIMIZE e seguro concorrente com o streaming: compactacao nao altera dados,
    e o controle de concorrencia otimista do Delta resolve os appends em paralelo.
#}
{% macro optimize_lakehouse() %}

    {% set caminhos = [
        var('bronze_path'),
        var('silver_path') ~ '/silver_quotes',
        var('gold_path') ~ '/gold_intraday_candles'
    ] %}

    {% for caminho in caminhos %}
        {% set sql %}
            optimize delta.`{{ caminho }}`
        {% endset %}
        {% do log("OPTIMIZE " ~ caminho, info=True) %}
        {% if execute %}
            {% do run_query(sql) %}
            {% do log("  ok: " ~ caminho, info=True) %}
        {% endif %}
    {% endfor %}

{% endmacro %}
