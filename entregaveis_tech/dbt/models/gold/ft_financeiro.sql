{{ config(
    unique_key='sk_trimestre',
    incremental_strategy='delete+insert',
    indexes=[{'columns': ['cd_conta', 'sk_trimestre']}]
) }}
{#-
  Grão: trimestre x operadora x conta contábil (DIOPS até o 4º nível).
  Recarga idempotente por trimestre, como na fato de beneficiários; a variável `trimestres`
  (1º dia de cada trimestre) diz quais recarregar.
-#}
{%- set trimestres = var('trimestres', none) %}
select
    to_char(s.dt_trimestre, 'YYYYMMDD')::integer   as sk_trimestre,
    o.sk_operadora::integer                    as sk_operadora,
    s.cd_conta_contabil::varchar(20)           as cd_conta,
    s.vl_saldo_inicial::numeric(20, 2)         as vl_saldo_inicial,
    s.vl_saldo_final::numeric(20, 2)           as vl_saldo_final,
    s.vl_movimento_trimestre::numeric(20, 2)   as vl_movimento_trimestre,
    now()                                      as dh_carga_dw
from {{ source('silver', 'tb_financeiro') }} s
join {{ ref('dm_operadora') }} o on o.cd_operadora = s.cd_operadora
{%- if trimestres is not none %}
where s.dt_trimestre in ({% for t in trimestres %}date '{{ t }}'{% if not loop.last %}, {% endif %}{% else %}null{% endfor %})
{%- endif %}
