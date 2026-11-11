{#- Usa o schema configurado (gold, referencia) como está, sem o prefixo padrão do dbt. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
