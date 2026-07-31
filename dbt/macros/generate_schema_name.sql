{#
    Por padrao o dbt concatena o schema do profile com o schema customizado
    (silver + gold = "silver_gold"). Aqui queremos os nomes exatos das camadas:
    silver e gold.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
