{{ config(
    unique_key='sk_competencia',
    incremental_strategy='delete+insert',
    indexes=[{'columns': ['sk_competencia', 'sg_uf']}, {'columns': ['sk_produto']}]
) }}
{#-
  Grão: competência x operadora x produto x UF x cobertura assistencial x contratação
  (a Silver no RustFS mantém o detalhe por município, sexo e faixa etária).
  Recarga idempotente por competência: o dbt apaga a competência e insere de novo, na mesma
  transação. A variável `competencias` (passada pela DAG) diz quais recarregar; sem ela,
  todas as que estão no schema silver.
-#}
{%- set competencias = var('competencias', none) %}
select
    to_char(s.dt_competencia, 'YYYYMMDD')::integer  as sk_competencia,
    o.sk_operadora::integer                         as sk_operadora,
    p.sk_produto::integer                           as sk_produto,
    s.sg_uf::char(2)                                as sg_uf,
    s.ds_cobertura_assistencial::varchar(40)           as ds_cobertura_assistencial,
    s.ds_contratacao::varchar(80)                      as ds_contratacao,
    sum(s.qt_beneficiario_ativo)::bigint            as qt_beneficiario_ativo,
    sum(s.qt_beneficiario_aderido)::bigint          as qt_beneficiario_aderido,
    sum(s.qt_beneficiario_cancelado)::bigint        as qt_beneficiario_cancelado,
    max(s.dt_carga)::date                           as dt_carga_fonte,
    now()                                           as dh_carga_dw
from {{ source('silver', 'tb_beneficiarios') }} s
join {{ ref('dm_operadora') }} o on o.cd_operadora = s.cd_operadora
join {{ ref('dm_produto') }} p on p.cd_operadora = s.cd_operadora and p.cd_plano = s.cd_plano
{%- if competencias is not none %}
where s.dt_competencia in ({% for c in competencias %}date '{{ c }}'{% if not loop.last %}, {% endif %}{% else %}null{% endfor %})
{%- endif %}
group by 1, 2, 3, 4, 5, 6
