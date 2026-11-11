-- =============================================================================
-- Controle de processamento entre as camadas (schema `controle`, no mesmo PostgreSQL).
--
-- As quatro DAGs rodam em horários diferentes e não trocam XCom entre si: cada camada
-- registra aqui o que produziu, e a camada seguinte lê o que ainda não consumiu.
--
--   dag_landing_ans --grava-->  tb_arquivo_landing  --lida por-->  dag_bronze_ans
--   dag_bronze_ans  --grava-->  tb_particao_bronze  --lida por-->  dag_silver_ans
--   dag_silver_ans  --grava-->  tb_particao_silver  --lida por-->  dag_gold_ans
--   dag_gold_ans    --grava-->  tb_carga_gold       (marca d'água: até onde a Silver foi consumida)
--
-- Tabelas com o prefixo tb_ e colunas com o prefixo de 2 letras do padrão de nomenclatura (cd_, nm_, ds_, qt_, dt_, dh_).
-- Idempotente: aplicado no início das DAGs.
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS controle;

-- Um registro por arquivo gravado na Landing. A Bronze converte o mais recente de cada partição.
CREATE TABLE IF NOT EXISTS controle.tb_arquivo_landing (
    ds_chave                 TEXT PRIMARY KEY,            -- objeto em s3://landing/...
    nm_fonte                 TEXT NOT NULL,               -- operadoras | produtos | beneficiarios | financeiro
    ds_particao              TEXT NOT NULL,               -- snapshot | competencia=AAAAMM/uf=XX | ano=AAAA/trimestre=T
    ds_url_origem            TEXT NOT NULL,
    cd_etag_origem           TEXT,                        -- assinatura da publicação na ANS (HEAD)
    ds_last_modified_origem  TEXT,
    qt_bytes                 BIGINT NOT NULL,
    cd_sha256                CHAR(64) NOT NULL,
    dt_ingestao              DATE NOT NULL,
    dh_ingestao              TIMESTAMPTZ NOT NULL DEFAULT now(),
    dh_verificacao           TIMESTAMPTZ NOT NULL DEFAULT now(), -- última vez que a ANS foi consultada
    cd_dag_run               TEXT
);
CREATE INDEX IF NOT EXISTS ix_tb_arquivo_landing_particao
    ON controle.tb_arquivo_landing (nm_fonte, ds_particao, dh_ingestao DESC);

-- A versão atual de cada partição da Bronze (arquivo da Landing como tabela Parquet).
CREATE TABLE IF NOT EXISTS controle.tb_particao_bronze (
    nm_entidade       TEXT NOT NULL,
    ds_particao       TEXT NOT NULL,                      -- data_snapshot=AAAA-MM-DD | competencia=AAAAMM/uf=XX | ano=AAAA/trimestre=T
    ds_uri            TEXT NOT NULL,
    ds_chave_landing  TEXT NOT NULL REFERENCES controle.tb_arquivo_landing (ds_chave),
    qt_linhas         BIGINT,
    qt_colunas        INTEGER,
    dh_processamento  TIMESTAMPTZ NOT NULL DEFAULT now(),
    cd_dag_run        TEXT,
    PRIMARY KEY (nm_entidade, ds_particao)
);
CREATE INDEX IF NOT EXISTS ix_tb_particao_bronze_chave ON controle.tb_particao_bronze (ds_chave_landing);

-- A versão atual de cada partição da Silver e a versão da Bronze que a gerou.
CREATE TABLE IF NOT EXISTS controle.tb_particao_silver (
    nm_entidade              TEXT NOT NULL,
    ds_particao              TEXT NOT NULL,               -- a mesma partição da Bronze
    ds_uri                   TEXT NOT NULL,
    dh_processamento_bronze  TIMESTAMPTZ NOT NULL,        -- versão da Bronze consumida
    qt_linhas_validas        BIGINT,
    qt_linhas_rejeitadas     BIGINT,
    dh_processamento         TIMESTAMPTZ NOT NULL DEFAULT now(),
    cd_dag_run               TEXT,
    PRIMARY KEY (nm_entidade, ds_particao),
    FOREIGN KEY (nm_entidade, ds_particao) REFERENCES controle.tb_particao_bronze (nm_entidade, ds_particao)
);

-- Última carga de cada unidade da Gold. Uma unidade é recarregada quando alguma partição
-- da Silver que a compõe foi processada depois de `dh_processamento_silver`.
CREATE TABLE IF NOT EXISTS controle.tb_carga_gold (
    cd_unidade               TEXT PRIMARY KEY,            -- dm_operadora | dm_produto | ft_beneficiarios:AAAA-MM | ft_financeiro:AAAA-Tn
    dh_processamento_silver  TIMESTAMPTZ NOT NULL,
    dh_carga                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    ds_detalhes              JSONB,
    cd_dag_run               TEXT
);
