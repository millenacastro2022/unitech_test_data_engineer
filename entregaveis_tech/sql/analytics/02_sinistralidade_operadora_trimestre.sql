-- =============================================================================
-- RELATÓRIO 2 | Receita x Despesa Assistencial (Sinistralidade) por operadora,
--              por ano/trimestre
-- -----------------------------------------------------------------------------
-- Contas do plano de contas padrão ANS (DIOPS):
--   31 = Receitas com operações de assistência à saúde (contraprestações efetivas)
--   41 = Eventos indenizáveis líquidos / sinistros retidos (despesa assistencial)
-- Sinistralidade = despesa assistencial / receita assistencial x 100
--
-- As contas de resultado são acumuladas no ano. Por isso o relatório traz:
--   * a visão do TRIMESTRE    (vl_movimento_trimestre = saldo final - saldo inicial) e
--   * a visão ACUMULADA NO ANO (vl_saldo_final), que é a usada nos balanços.
-- Também traz o benchmark da modalidade no trimestre (sinistralidade agregada de todas
-- as operadoras da mesma modalidade) e a variação contra o trimestre anterior.
-- Ficam de fora operadoras sem receita assistencial no trimestre e administradoras de
-- benefícios (não assumem o risco assistencial, logo não têm sinistralidade).
-- =============================================================================
WITH contas AS (
    SELECT
        f.sk_trimestre,
        f.sk_operadora,
        SUM(f.vl_movimento_trimestre) FILTER (WHERE f.cd_conta = '31') AS receita_trimestre,
        SUM(f.vl_movimento_trimestre) FILTER (WHERE f.cd_conta = '41') AS despesa_trimestre,
        SUM(f.vl_saldo_final)         FILTER (WHERE f.cd_conta = '31') AS receita_acumulada_ano,
        SUM(f.vl_saldo_final)         FILTER (WHERE f.cd_conta = '41') AS despesa_acumulada_ano
    FROM gold.ft_financeiro f
    WHERE f.cd_conta IN ('31', '41')
    GROUP BY f.sk_trimestre, f.sk_operadora
),
indicadores AS (
    SELECT
        ct.*,
        o.ds_modalidade,
        100.0 * ct.despesa_trimestre     / NULLIF(ct.receita_trimestre, 0)     AS sinistralidade_trimestre,
        100.0 * ct.despesa_acumulada_ano / NULLIF(ct.receita_acumulada_ano, 0) AS sinistralidade_acumulada,
        100.0 * SUM(ct.despesa_trimestre) OVER modalidade_trimestre
              / NULLIF(SUM(ct.receita_trimestre) OVER modalidade_trimestre, 0) AS sinistralidade_modalidade
    FROM contas ct
    JOIN gold.dm_operadora o ON o.sk_operadora = ct.sk_operadora
    WHERE ct.receita_trimestre > 0
      AND o.ds_modalidade <> 'ADMINISTRADORA DE BENEFÍCIOS'
    WINDOW modalidade_trimestre AS (PARTITION BY ct.sk_trimestre, o.ds_modalidade)
),
com_historico AS (
    SELECT
        i.*,
        LAG(i.sk_trimestre)             OVER historico AS sk_trimestre_anterior,
        LAG(i.sinistralidade_trimestre) OVER historico AS sinistralidade_trimestre_anterior
    FROM indicadores i
    WINDOW historico AS (PARTITION BY i.sk_operadora ORDER BY i.sk_trimestre)
)
SELECT
    c.nr_ano,
    c.nr_trimestre,
    c.cd_ano_trimestre,
    o.cd_operadora,
    o.nm_razao_social,
    h.ds_modalidade,
    o.sg_uf_sede,
    ROUND(h.receita_trimestre, 2)                                    AS receita_assistencial_trimestre,
    ROUND(h.despesa_trimestre, 2)                                    AS despesa_assistencial_trimestre,
    ROUND(h.receita_trimestre - COALESCE(h.despesa_trimestre, 0), 2) AS resultado_assistencial_trimestre,
    ROUND(h.sinistralidade_trimestre, 2)                             AS sinistralidade_trimestre_pct,
    ROUND(h.sinistralidade_acumulada, 2)                             AS sinistralidade_acumulada_ano_pct,
    ROUND(h.sinistralidade_modalidade, 2)                            AS sinistralidade_modalidade_pct,
    ROUND(h.sinistralidade_trimestre - h.sinistralidade_modalidade, 2) AS diferenca_vs_modalidade_pp,
    -- variação só contra o trimestre imediatamente anterior (NULL se ele não foi carregado)
    CASE WHEN h.sk_trimestre_anterior = to_char(c.dt_data - INTERVAL '3 months', 'YYYYMMDD')::INTEGER
         THEN ROUND(h.sinistralidade_trimestre - h.sinistralidade_trimestre_anterior, 2)
    END                                                              AS variacao_vs_trimestre_anterior_pp
FROM com_historico h
JOIN gold.dm_calendario c ON c.sk_data = h.sk_trimestre
JOIN gold.dm_operadora  o ON o.sk_operadora = h.sk_operadora
ORDER BY c.nr_ano DESC, c.nr_trimestre DESC, h.receita_trimestre DESC;
