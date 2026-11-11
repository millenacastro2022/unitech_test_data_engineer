{{ config(unique_key='sk_data', incremental_strategy='merge') }}
{#-
  Calendário diário de 2000 a 2035. Fatos mensais usam o 1º dia do mês; trimestrais, o 1º dia
  do trimestre. Incremental: só entram datas que ainda não estão na tabela.
-#}
with dias as (
    select g::date as d
    from generate_series(date '2000-01-01', date '2035-12-31', interval '1 day') as g
)
select
    to_char(d, 'YYYYMMDD')::integer                                                      as sk_data,
    d                                                                                    as dt_data,
    extract(year from d)::smallint                                                       as nr_ano,
    (case when extract(month from d) <= 6 then 1 else 2 end)::smallint                   as nr_semestre,
    extract(quarter from d)::smallint                                                    as nr_trimestre,
    extract(month from d)::smallint                                                      as nr_mes,
    extract(day from d)::smallint                                                        as nr_dia,
    ((array['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho', 'Agosto',
             'Setembro', 'Outubro', 'Novembro', 'Dezembro'])[extract(month from d)::int])::varchar(10) as nm_mes,
    ((array['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'])
        [extract(month from d)::int])::char(3)                                           as nm_mes_abrev,
    to_char(d, 'YYYY-MM')::char(7)                                                       as cd_ano_mes,
    (to_char(d, 'YYYY') || '-T' || extract(quarter from d))::char(7)                     as cd_ano_trimestre,
    extract(isodow from d)::smallint                                                     as nr_dia_semana,
    ((array['Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado',
             'Domingo'])[extract(isodow from d)::int])::varchar(13)                      as nm_dia_semana,
    extract(isodow from d) in (6, 7)                                                     as fl_fim_de_semana,
    d = (date_trunc('month', d) + interval '1 month - 1 day')::date                      as fl_ultimo_dia_mes,
    date_trunc('month', d)::date                                                         as dt_primeiro_dia_mes,
    date_trunc('quarter', d)::date                                                       as dt_primeiro_dia_trimestre
from dias
{% if is_incremental() %}
where not exists (select 1 from {{ this }} c where c.dt_data = dias.d)
{% endif %}
