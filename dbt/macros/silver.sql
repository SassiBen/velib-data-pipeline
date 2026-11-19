{#- En production les modèles lisent les tables Delta du lac ; en CI, un
    échantillon Parquet réel versionné dans le dépôt. -#}
{% macro silver(name) -%}
  {%- if target.name == 'ci' -%}
    read_parquet('{{ var("sample_dir") }}/{{ name }}.parquet')
  {%- else -%}
    delta_scan('{{ var("lake") }}/silver/{{ name }}')
  {%- endif -%}
{%- endmacro %}
