-- Nenhuma quantidade de beneficiários pode ser negativa (a Silver já manda essas linhas para a quarentena).
select *
from {{ ref('ft_beneficiarios') }}
where qt_beneficiario_ativo < 0 or qt_beneficiario_aderido < 0 or qt_beneficiario_cancelado < 0
