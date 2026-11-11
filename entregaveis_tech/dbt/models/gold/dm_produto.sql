{{ config(unique_key=['cd_operadora', 'cd_plano'], incremental_strategy='merge') }}
{#-
  Produtos/planos (SCD tipo 1), chave natural (registro_ans, cd_plano). Entra a base de
  produtos (pda-008); planos citados nos beneficiários (SIB) e ausentes dela entram como
  membros inferidos e são "promovidos" quando o plano passa a constar na base.
-#}
with pda as (
    select * from {{ source('silver', 'tb_produtos') }}
),

sib as (
    select distinct on (cd_operadora, cd_plano)
        cd_operadora, cd_plano, tp_vigencia_plano, ds_contratacao, ds_segmentacao, ds_abrangencia_geografica,
        ds_cobertura_assistencial
    from {{ source('silver', 'tb_beneficiarios') }}
    order by cd_operadora, cd_plano, qt_beneficiario_ativo desc
),

atual as (
{%- if is_incremental() %}
    select * from {{ this }}
{%- else %}
    select null::integer as sk_produto, null::varchar as cd_operadora, null::varchar as cd_plano,
           null::bigint as id_plano, null::varchar as nm_plano, null::varchar as tp_vigencia_plano,
           null::varchar as ds_contratacao, null::varchar as ds_grupo_contratacao, null::varchar as ds_segmentacao_assistencial,
           null::varchar as ds_grupo_segmentacao_assistencial, null::varchar as ds_cobertura, null::boolean as fl_odontologico,
           null::varchar as ds_obstetricia, null::varchar as ds_abrangencia_cobertura, null::varchar as tp_financiamento,
           null::varchar as ds_fator_moderador, null::varchar as ds_acomodacao_hospitalar, null::varchar as ds_livre_escolha,
           null::varchar as ds_porte_operadora, null::varchar as ds_situacao_plano, null::date as dt_situacao,
           null::date as dt_registro_plano, null::varchar as tp_origem, null::timestamptz as dh_atualizacao
    where false
{%- endif %}
),

candidatos as (
    -- 1) base de produtos (pda-008)
    select cd_operadora, cd_plano, id_plano, coalesce(nm_plano, 'SEM NOME') as nm_plano, tp_vigencia_plano,
           ds_contratacao, ds_grupo_contratacao, ds_segmentacao_assistencial, ds_grupo_segmentacao_assistencial, ds_cobertura,
           fl_odontologico, ds_obstetricia, ds_abrangencia_cobertura, tp_financiamento, ds_fator_moderador,
           ds_acomodacao_hospitalar, ds_livre_escolha, ds_porte_operadora, ds_situacao_plano, dt_situacao, dt_registro_plano,
           'PDA-008' as tp_origem
    from pda

    union all
    -- 2) membros inferidos pelo SIB: planos que ainda não existem na dimensão
    select s.cd_operadora, s.cd_plano, null, 'NÃO CONSTA NA BASE DE PRODUTOS', s.tp_vigencia_plano, s.ds_contratacao,
           null, s.ds_segmentacao, null, s.ds_cobertura_assistencial, s.ds_cobertura_assistencial = 'Odontológico', null,
           s.ds_abrangencia_geografica, null, null, null, null, null, null, null, null, 'SIB'
    from sib s
    where not exists (select 1 from pda p where p.cd_operadora = s.cd_operadora and p.cd_plano = s.cd_plano)
      and not exists (select 1 from atual a where a.cd_operadora = s.cd_operadora and a.cd_plano = s.cd_plano)

    union all
    -- 3) membro "desconhecido" (sk = -1)
    select '000000', 'NI', null, 'NÃO IDENTIFICADO', null, null, null, null, null, null, null, null, null, null,
           null, null, null, null, null, null, null, 'N/A'
    where not exists (select 1 from atual a where a.sk_produto = -1)
),

mudou as (
    select c.*, a.sk_produto as sk_atual
    from candidatos c
    left join atual a on a.cd_operadora = c.cd_operadora and a.cd_plano = c.cd_plano
    where a.cd_operadora is null
       or (c.id_plano, c.nm_plano, c.tp_vigencia_plano, c.ds_contratacao, c.ds_grupo_contratacao, c.ds_segmentacao_assistencial,
           c.ds_grupo_segmentacao_assistencial, c.ds_cobertura, c.fl_odontologico, c.ds_obstetricia, c.ds_abrangencia_cobertura,
           c.tp_financiamento, c.ds_fator_moderador, c.ds_acomodacao_hospitalar, c.ds_livre_escolha, c.ds_porte_operadora,
           c.ds_situacao_plano, c.dt_situacao, c.dt_registro_plano, c.tp_origem)
          is distinct from
          (a.id_plano, a.nm_plano, a.tp_vigencia_plano, a.ds_contratacao, a.ds_grupo_contratacao, a.ds_segmentacao_assistencial,
           a.ds_grupo_segmentacao_assistencial, a.ds_cobertura, a.fl_odontologico, a.ds_obstetricia, a.ds_abrangencia_cobertura,
           a.tp_financiamento, a.ds_fator_moderador, a.ds_acomodacao_hospitalar, a.ds_livre_escolha, a.ds_porte_operadora,
           a.ds_situacao_plano, a.dt_situacao, a.dt_registro_plano, a.tp_origem)
),

maior as (
    select coalesce(max(sk_produto), 0) as sk from atual
)

select
    (case
        when m.cd_operadora = '000000' and m.cd_plano = 'NI' then -1
        when m.sk_atual is not null then m.sk_atual
        else maior.sk + row_number() over (partition by m.sk_atual is null, m.cd_operadora = '000000' and m.cd_plano = 'NI'
                                           order by m.cd_operadora, m.cd_plano)
     end)::integer                             as sk_produto,
    m.cd_operadora::varchar(6)                 as cd_operadora,
    m.cd_plano::varchar(30)                    as cd_plano,
    m.id_plano::bigint                         as id_plano,
    m.nm_plano::varchar(300)                   as nm_plano,
    m.tp_vigencia_plano::char(1)               as tp_vigencia_plano,
    m.ds_contratacao::varchar(80)                 as ds_contratacao,
    m.ds_grupo_contratacao::varchar(80)              as ds_grupo_contratacao,
    m.ds_segmentacao_assistencial::varchar(120)   as ds_segmentacao_assistencial,
    m.ds_grupo_segmentacao_assistencial::varchar(80) as ds_grupo_segmentacao_assistencial,
    m.ds_cobertura::varchar(40)                   as ds_cobertura,
    m.fl_odontologico::boolean                 as fl_odontologico,
    m.ds_obstetricia::varchar(40)                 as ds_obstetricia,
    m.ds_abrangencia_cobertura::varchar(60)       as ds_abrangencia_cobertura,
    m.tp_financiamento::varchar(40)          as tp_financiamento,
    m.ds_fator_moderador::varchar(60)             as ds_fator_moderador,
    m.ds_acomodacao_hospitalar::varchar(40)       as ds_acomodacao_hospitalar,
    m.ds_livre_escolha::varchar(40)               as ds_livre_escolha,
    m.ds_porte_operadora::varchar(20)             as ds_porte_operadora,
    m.ds_situacao_plano::varchar(30)              as ds_situacao_plano,
    m.dt_situacao::date                        as dt_situacao,
    m.dt_registro_plano::date                  as dt_registro_plano,
    m.tp_origem::varchar(10)                      as tp_origem,
    now()                                      as dh_atualizacao
from mudou m
cross join maior
