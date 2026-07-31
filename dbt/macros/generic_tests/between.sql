{#
    Teste generico: valores fora de uma faixa plausivel.

    Usado para "preco de acao da B3 tem que estar entre R$ 0,01 e R$ 5.000" e
    "variacao diaria de +-30% e implausivel" - o tipo de regra que pega erro de
    parsing, virgula/ponto decimal trocados e resposta corrompida da API.
#}
{% test between(model, column_name, min_value, max_value) %}

select
    {{ column_name }} as valor_invalido,
    count(*) as ocorrencias
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})
group by 1

{% endtest %}
