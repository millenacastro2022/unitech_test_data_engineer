-- O valor do trimestre é o saldo final menos o inicial (contas de resultado do DIOPS são acumuladas no ano).
select *
from {{ ref('ft_financeiro') }}
where vl_movimento_trimestre <> vl_saldo_final - vl_saldo_inicial
