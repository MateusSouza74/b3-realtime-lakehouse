{#
    Teste generico: unicidade de chave composta.

    Aqui vale como prova de que a deduplicacao da silver funcionou: o polling
    entrega a mesma cotacao varias vezes, e (ticker, quote_ts) tem que ser unico
    depois do tratamento.
#}
{% test unique_combination_of_columns(model, combination_of_columns) %}

{%- set colunas = combination_of_columns | join(', ') -%}

select
    {{ colunas }},
    count(*) as ocorrencias
from {{ model }}
group by {{ colunas }}
having count(*) > 1

{% endtest %}
