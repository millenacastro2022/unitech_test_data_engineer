-- =============================================================================
-- RELATÓRIO 1 | Market share de beneficiários por modalidade de operadora em cada UF
-- -----------------------------------------------------------------------------
-- Pergunta : em cada UF, qual a participação de cada modalidade (Medicina de Grupo,
--            Cooperativa Médica, Seguradora, Autogestão, ...) no total de beneficiários?
-- Métrica  : market_share_pct = beneficiários ativos da modalidade na UF
--                               / beneficiários ativos da UF  x 100
-- Recortes : - por cobertura assistencial: planos médico-hospitalares e exclusivamente
--              odontológicos são mercados distintos (a ANS os reporta separadamente e
--              somá-los contaria a mesma pessoa duas vezes);
--            - exclui UF "XX" (beneficiários sem UF identificada).
-- Parâmetro: competência analisada (padrão = última carregada) na CTE "parametros".
-- Colunas extras para o relatório: total da UF, ranking da modalidade na UF e variação
-- do share (p.p.) contra o mês anterior.
-- =============================================================================
WITH parametros AS (
    SELECT MAX(sk_competencia) AS sk_competencia      -- ex.: troque por 20250601
    FROM gold.ft_beneficiarios
),
beneficiarios_modalidade AS (
    SELECT
        f.sk_competencia,
        f.sg_uf,
        f.ds_cobertura_assistencial,
        o.ds_modalidade,
        SUM(f.qt_beneficiario_ativo) AS qt_beneficiarios
    FROM gold.ft_beneficiarios f
    JOIN gold.dm_operadora o ON o.sk_operadora = f.sk_operadora
    WHERE f.sg_uf <> 'XX'
    GROUP BY f.sk_competencia, f.sg_uf, f.ds_cobertura_assistencial, o.ds_modalidade
),
market_share AS (
    SELECT
        b.*,
        SUM(b.qt_beneficiarios) OVER uf_cobertura                                      AS qt_beneficiarios_uf,
        100.0 * b.qt_beneficiarios / NULLIF(SUM(b.qt_beneficiarios) OVER uf_cobertura, 0) AS share
    FROM beneficiarios_modalidade b
    WINDOW uf_cobertura AS (PARTITION BY b.sk_competencia, b.sg_uf, b.ds_cobertura_assistencial)
),
com_historico AS (
    SELECT
        m.*,
        LAG(m.sk_competencia) OVER historico AS sk_competencia_anterior,
        LAG(m.share)          OVER historico AS share_competencia_anterior,
        RANK() OVER (
            PARTITION BY m.sk_competencia, m.sg_uf, m.ds_cobertura_assistencial ORDER BY m.qt_beneficiarios DESC
        ) AS ranking_na_uf
    FROM market_share m
    WINDOW historico AS (PARTITION BY m.sg_uf, m.ds_cobertura_assistencial, m.ds_modalidade ORDER BY m.sk_competencia)
)
SELECT
    c.cd_ano_mes                                   AS competencia,
    u.nm_regiao,
    h.sg_uf,
    u.nm_uf,
    h.ds_cobertura_assistencial,
    h.ds_modalidade,
    h.qt_beneficiarios,
    h.qt_beneficiarios_uf,
    ROUND(h.share, 2)                           AS market_share_pct,
    h.ranking_na_uf,
    -- variação só contra o mês imediatamente anterior (NULL se ele não foi carregado)
    CASE WHEN h.sk_competencia_anterior = to_char(c.dt_data - INTERVAL '1 month', 'YYYYMMDD')::INTEGER
         THEN ROUND(h.share - h.share_competencia_anterior, 2)
    END                                         AS variacao_share_pp_mes_anterior
FROM com_historico h
JOIN parametros p             ON p.sk_competencia = h.sk_competencia
JOIN gold.dm_calendario c    ON c.sk_data = h.sk_competencia
JOIN gold.dm_uf u            ON u.sg_uf = h.sg_uf
ORDER BY u.nm_regiao, h.sg_uf, h.ds_cobertura_assistencial, h.ranking_na_uf;
