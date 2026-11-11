{{ config(unique_key='cd_operadora', incremental_strategy='merge', indexes=[{'columns': ['ds_modalidade']}]) }}
{#-
  Operadoras (SCD tipo 1). Entra o cadastro de ativas (CADOP) e, como membros inferidos,
  operadoras que aparecem nos beneficiários (SIB) ou no DIOPS sem estar no cadastro.
  MERGE incremental: só passam linhas novas ou com algum atributo mudado, e a chave
  substituta de quem já existe nunca muda (as fatos apontam para ela).
-#}
with cadop as (
    select * from {{ source('silver', 'tb_operadoras') }}
),

sib as (
    select distinct on (cd_operadora)
        cd_operadora, nr_cnpj, coalesce(nm_razao_social, 'NÃO INFORMADA') as nm_razao_social, ds_modalidade
    from {{ source('silver', 'tb_beneficiarios') }}
    order by cd_operadora, qt_beneficiario_ativo desc
),

diops as (
    select distinct cd_operadora from {{ source('silver', 'tb_financeiro') }}
),

atual as (
{%- if is_incremental() %}
    select * from {{ this }}
{%- else %}
    select null::integer as sk_operadora, null::varchar as cd_operadora, null::varchar as nr_cnpj,
           null::varchar as nm_razao_social, null::varchar as nm_fantasia, null::varchar as ds_modalidade,
           null::varchar as sg_uf_sede, null::varchar as nm_cidade_sede, null::smallint as cd_regiao_comercializacao,
           null::date as dt_registro_ans, null::boolean as fl_ativa, null::varchar as tp_origem,
           null::timestamptz as dh_atualizacao
    where false
{%- endif %}
),

candidatas as (
    -- 1) cadastro de operadoras ativas
    select cd_operadora, nr_cnpj, nm_razao_social, nm_fantasia, ds_modalidade, sg_uf as sg_uf_sede, nm_cidade as nm_cidade_sede,
           cd_regiao_comercializacao, dt_registro_ans, true as fl_ativa, 'CADOP' as tp_origem
    from cadop

    union all
    -- 2) saíram do cadastro: continuam na dimensão (histórico das fatos), inativas.
    --    Com o cadastro vazio (ambiente ainda sem snapshot), ninguém é inativado.
    select a.cd_operadora, a.nr_cnpj, a.nm_razao_social, a.nm_fantasia, a.ds_modalidade, a.sg_uf_sede, a.nm_cidade_sede,
           a.cd_regiao_comercializacao, a.dt_registro_ans, false, a.tp_origem
    from atual a
    where exists (select 1 from cadop)
      and a.fl_ativa and a.sk_operadora > 0
      and not exists (select 1 from cadop c where c.cd_operadora = a.cd_operadora)

    union all
    -- 3) membros inferidos pelo SIB (ex.: canceladas): novos, ou que eram inferidos pelo DIOPS
    select s.cd_operadora, s.nr_cnpj, s.nm_razao_social, null, s.ds_modalidade, null, null, null, null, false, 'SIB'
    from sib s
    left join atual a on a.cd_operadora = s.cd_operadora
    where not exists (select 1 from cadop c where c.cd_operadora = s.cd_operadora)
      and (a.cd_operadora is null or a.tp_origem = 'DIOPS')

    union all
    -- 4) membros inferidos pelo DIOPS: entregaram balanço, mas não estão no cadastro nem no SIB
    select d.cd_operadora, null, 'NÃO IDENTIFICADA (REGISTRO ' || d.cd_operadora || ')', null, 'NÃO INFORMADA',
           null, null, null, null, false, 'DIOPS'
    from diops d
    where not exists (select 1 from cadop c where c.cd_operadora = d.cd_operadora)
      and not exists (select 1 from sib s where s.cd_operadora = d.cd_operadora)
      and not exists (select 1 from atual a where a.cd_operadora = d.cd_operadora)

    union all
    -- 5) membro "desconhecido" (sk = -1)
    select '000000', null, 'NÃO IDENTIFICADA', null, 'NÃO INFORMADA', null, null, null, null, false, 'N/A'
    where not exists (select 1 from atual a where a.sk_operadora = -1)
),

mudou as (
    select c.*, a.sk_operadora as sk_atual
    from candidatas c
    left join atual a on a.cd_operadora = c.cd_operadora
    where a.cd_operadora is null
       or (c.nr_cnpj, c.nm_razao_social, c.nm_fantasia, c.ds_modalidade, c.sg_uf_sede, c.nm_cidade_sede,
           c.cd_regiao_comercializacao, c.dt_registro_ans, c.fl_ativa, c.tp_origem)
          is distinct from
          (a.nr_cnpj, a.nm_razao_social, a.nm_fantasia, a.ds_modalidade, a.sg_uf_sede, a.nm_cidade_sede,
           a.cd_regiao_comercializacao, a.dt_registro_ans, a.fl_ativa, a.tp_origem)
),

maior as (
    select coalesce(max(sk_operadora), 0) as sk from atual
)

select
    (case
        when m.cd_operadora = '000000' then -1
        when m.sk_atual is not null then m.sk_atual
        else maior.sk + row_number() over (partition by m.sk_atual is null, m.cd_operadora = '000000'
                                           order by m.cd_operadora)
     end)::integer                         as sk_operadora,
    m.cd_operadora::varchar(6)             as cd_operadora,
    m.nr_cnpj::varchar(14)                    as nr_cnpj,
    m.nm_razao_social::varchar(300)           as nm_razao_social,
    m.nm_fantasia::varchar(300)          as nm_fantasia,
    m.ds_modalidade::varchar(60)              as ds_modalidade,
    m.sg_uf_sede::char(2)                  as sg_uf_sede,
    m.nm_cidade_sede::varchar(100)            as nm_cidade_sede,
    m.cd_regiao_comercializacao::smallint     as cd_regiao_comercializacao,
    m.dt_registro_ans::date                as dt_registro_ans,
    m.fl_ativa::boolean                    as fl_ativa,
    m.tp_origem::varchar(10)                  as tp_origem,
    now()                                  as dh_atualizacao
from mudou m
cross join maior
