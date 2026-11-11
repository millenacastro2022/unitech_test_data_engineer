{{ config(severity='warn') }}
-- Aviso (não bloqueia): competência carregada sem as 28 UFs (27 + XX). Acontece numa carga
-- de teste com poucas UFs; numa carga completa indica arquivo faltando na ANS.
select sk_competencia, count(distinct sg_uf) as ufs
from {{ ref('ft_beneficiarios') }}
group by sk_competencia
having count(distinct sg_uf) < 28
