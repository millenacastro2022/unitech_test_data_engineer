-- =============================================================================
-- Silver publicada no Data Warehouse (schema `silver` do PostgreSQL).
--
-- A Silver completa fica em Parquet no RustFS. Aqui o Spark (dag_gold_ans.publish_silver)
-- publica o que a Gold consome, para o dbt modelar dentro do PostgreSQL:
--   - operadoras e produtos: o snapshot mais recente, inteiro;
--   - beneficiarios: somados por competência x operadora x plano x UF x cobertura x
--     contratação (o detalhe por município, sexo e faixa etária fica no RustFS);
--   - financeiro: DIOPS até o 4º nível do plano de contas.
-- Cada execução substitui só o que mudou (um snapshot, uma competência, um trimestre), e a
-- retenção apaga daqui o que saiu da janela, como nas outras camadas.
-- Idempotente: aplicado no início de publish_silver.
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS silver;
COMMENT ON SCHEMA silver IS 'Silver publicada no DW pelo Spark: a parte da Silver que a Gold consome (fonte dos modelos dbt).';

CREATE TABLE IF NOT EXISTS silver.tb_operadoras (
    cd_operadora text, nr_cnpj text, nm_razao_social text, nm_fantasia text, ds_modalidade text, sg_uf text,
    nm_cidade text, cd_regiao_comercializacao smallint, dt_registro_ans date
);

CREATE TABLE IF NOT EXISTS silver.tb_produtos (
    cd_operadora text, cd_plano text, id_plano bigint, nm_plano text, tp_vigencia_plano text, ds_contratacao text,
    ds_grupo_contratacao text, ds_segmentacao_assistencial text, ds_grupo_segmentacao_assistencial text, ds_cobertura text,
    fl_odontologico boolean, ds_obstetricia text, ds_abrangencia_cobertura text, tp_financiamento text,
    ds_fator_moderador text, ds_acomodacao_hospitalar text, ds_livre_escolha text, ds_porte_operadora text,
    ds_situacao_plano text, dt_situacao date, dt_registro_plano date
);

CREATE TABLE IF NOT EXISTS silver.tb_beneficiarios (
    dt_competencia date, cd_operadora text, cd_plano text, sg_uf text, ds_cobertura_assistencial text,
    ds_contratacao text, nm_razao_social text, nr_cnpj text, ds_modalidade text, tp_vigencia_plano text, ds_segmentacao text,
    ds_abrangencia_geografica text, qt_beneficiario_ativo bigint, qt_beneficiario_aderido bigint,
    qt_beneficiario_cancelado bigint, dt_carga date
);
CREATE INDEX IF NOT EXISTS ix_silver_tb_beneficiarios_competencia ON silver.tb_beneficiarios (dt_competencia);

CREATE TABLE IF NOT EXISTS silver.tb_financeiro (
    dt_trimestre date, cd_operadora text, cd_conta_contabil text, ds_conta_contabil text,
    vl_saldo_inicial numeric(20, 2), vl_saldo_final numeric(20, 2), vl_movimento_trimestre numeric(20, 2)
);
CREATE INDEX IF NOT EXISTS ix_silver_tb_financeiro_trimestre ON silver.tb_financeiro (dt_trimestre);
