"""Job ETL da Camada Bronze — Datalake de Tracking DadosPro.

Lê os eventos brutos de landing/ em JSON Lines com schema explícito,
adiciona as colunas de proveniência (_arquivo_origem e _ingestao_ts) e
grava em Parquet append-only particionado por event_name e data (year, month, day).

Requisitos atendidos:
- R1: Execução PySpark com s3a:// direto no floci (S3)
- R2: Verificação de dependência (exigir_camada da landing)
- R4: Bronze append-only com schema explícito e proveniência
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Garante import do comum.py ao executar via python pipeline/bronze.py
sys.path.insert(0, str(Path(__file__).resolve().parent))
from comum import criar_sessao, exigir_camada, glob_eventos, gravar_eventos, ler_eventos, titulo

# Schema explícito de todos os 20 eventos e parâmetros do contrato de dados
SCHEMA_LANDING = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("user_pseudo_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_hint", StringType(), True),
    StructField("page", StructType([
        StructField("location", StringType(), True),
        StructField("referrer", StringType(), True),
    ]), True),
    StructField("traffic", StructType([
        StructField("source", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("campaign", StringType(), True),
    ]), True),
    StructField("device", StructType([
        StructField("category", StringType(), True),
        StructField("os", StringType(), True),
        StructField("browser", StringType(), True),
    ]), True),
    StructField("params", StructType([
        StructField("page_title", StringType(), True),
        StructField("percent_scrolled", LongType(), True),
        StructField("link_url", StringType(), True),
        StructField("link_domain", StringType(), True),
        StructField("link_text", StringType(), True),
        StructField("link_id", StringType(), True),
        StructField("outbound", BooleanType(), True),
        StructField("search_term", StringType(), True),
        StructField("search_results", LongType(), True),
        StructField("search_area", StringType(), True),
        StructField("video_title", StringType(), True),
        StructField("video_provider", StringType(), True),
        StructField("video_duration", DoubleType(), True),
        StructField("video_current_time", DoubleType(), True),
        StructField("video_percent", LongType(), True),
        StructField("file_name", StringType(), True),
        StructField("file_extension", StringType(), True),
        StructField("item_list_id", StringType(), True),
        StructField("item_list_name", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("shipping_tier", StringType(), True),
        StructField("shipping", DoubleType(), True),
        StructField("payment_type", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("tax", DoubleType(), True),
        StructField("coupon", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("lead_source", StringType(), True),
        StructField("method", StringType(), True),
        StructField("element_id", StringType(), True),
    ]), True),
    StructField("items", ArrayType(StructType([
        StructField("item_id", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("item_category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", LongType(), True),
    ])), True),
])


def main() -> None:
    titulo("INICIANDO JOB ETL — CAMADA BRONZE")
    spark = criar_sessao("bronze")

    try:
        # R2: Confere se a camada anterior (landing) existe
        exigir_camada(spark, glob_eventos("landing"), "python gerador/gerar_eventos.py")

        # R4: Leitura com schema explícito sem inferência
        print("[>] Lendo eventos brutos da landing com schema explícito...")
        landing = ler_eventos(spark, "landing", "json", schema=SCHEMA_LANDING)

        # R4: Adição das colunas de proveniência (origem e instante de ingestão)
        print("[>] Enriquecendo com metadados de proveniência (_arquivo_origem, _ingestao_ts)...")
        bronze = (
            landing
            .withColumn("_arquivo_origem", F.input_file_name())
            .withColumn("_ingestao_ts", F.current_timestamp())
        )

        # R4: Gravação append-only em Parquet particionado
        print("[>] Gravando camada Bronze particionada por event_name/year/month/day em Parquet...")
        gravar_eventos(bronze, "bronze", "append")

        total = bronze.count()
        print(f"[✔] Sucesso: {total} eventos processados e persistidos na camada Bronze.")
        titulo("JOB BRONZE CONCLUÍDO COM SUCESSO")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
