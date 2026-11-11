{{ config(unique_key='sg_uf', incremental_strategy='merge') }}
{#- Unidades da Federação, do seed tb_ufs.csv; XX = UF não identificada pela ANS. -#}
select
    u.sg_uf::char(2)      as sg_uf,
    u.nm_uf::varchar(40)  as nm_uf,
    u.nm_regiao::varchar(20) as nm_regiao
from {{ ref('tb_ufs') }} u
{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} d
    where d.sg_uf = u.sg_uf and d.nm_uf = u.nm_uf and d.nm_regiao = u.nm_regiao
)
{% endif %}
