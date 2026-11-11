{{ config(unique_key='cd_conta', incremental_strategy='merge') }}
{#-
  Plano de contas padrão ANS até o 4º nível. Receita assistencial = conta 31; despesa
  assistencial = 41. A descrição é a mais frequente da conta (a grafia varia entre operadoras).
-#}
with contas as (
    select cd_conta_contabil as cd_conta,
           coalesce(mode() within group (order by ds_conta_contabil), 'SEM DESCRIÇÃO') as ds_conta
    from {{ source('silver', 'tb_financeiro') }}
    group by cd_conta_contabil
)
select
    c.cd_conta::varchar(20)                        as cd_conta,
    c.ds_conta::varchar(300)                       as ds_conta,
    length(c.cd_conta)::smallint                   as nr_nivel,
    left(c.cd_conta, 1)::char(1)                   as cd_classe,
    (case left(c.cd_conta, 1)
        when '1' then 'ATIVO'
        when '2' then 'PASSIVO'
        when '3' then 'RECEITAS'
        when '4' then 'DESPESAS'
        else 'OUTRAS'
     end)::varchar(30)                             as ds_classe,
    left(c.cd_conta, 2)::varchar(2)                as cd_grupo,
    now()                                          as dh_atualizacao
from contas c
{% if is_incremental() %}
where not exists (select 1 from {{ this }} d where d.cd_conta = c.cd_conta and d.ds_conta = c.ds_conta)
{% endif %}
