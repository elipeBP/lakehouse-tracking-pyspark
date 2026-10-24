"""Job ETL da Camada Silver — Datalake de Tracking DadosPro.

Lê a camada Bronze em Parquet, aplica deduplicação por event_id,
extrai os campos de params e element_id para colunas tipadas,
valida as regras de negócio separando eventos rejeitados com motivo,
realiza a sessionização com janela de inatividade de 30 min (sem session_hint),
faz identity stitching (propagação de user_id) e
explode a lista de itens para a tabela silver/itens/.

Requisitos atendidos:
- R1: Execução PySpark com s3a:// direto no floci (S3)
- R2: Verificação de dependência (exigir_camada da bronze)
- R5: Deduplicação por event_id
- R6: Extração e tipagem dos params (incluindo element_id)
- R7: Validação e gravação de silver/eventos_rejeitados/ com motivo_rejeicao
- R8: Sessionização própria com corte em inatividade > 30 min
- R9: Identity stitching (ligação entre user_pseudo_id e user_id)
- R10: Tabela silver/itens/ com items explodido
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

# Garante import do comum.py ao executar via python pipeline/silver.py
sys.path.insert(0, str(Path(__file__).resolve().parent))
from comum import caminho, criar_sessao, exigir_camada, glob_eventos, gravar_eventos, ler_eventos, titulo

# Catálogo oficial dos 22 nomes de eventos válidos (20 eventos do contrato)
EVENTOS_VALIDOS = [
    "page_view", "scroll", "click", "site_search",
    "video_start", "video_progress", "video_complete", "file_download",
    "view_item_list", "select_item", "view_item", "add_to_cart", "remove_from_cart",
    "view_cart", "begin_checkout", "add_shipping_info", "add_payment_info",
    "purchase", "refund",
    "generate_lead", "sign_up", "login",
]


def main() -> None:
    titulo("INICIANDO JOB ETL — CAMADA SILVER")
    spark = criar_sessao("silver")

    try:
        # R2: Confere se a camada Bronze existe
        exigir_camada(spark, glob_eventos("bronze"), "python pipeline/bronze.py")

        print("[>] Lendo eventos da camada Bronze...")
        bronze = ler_eventos(spark, "bronze")

        # R5: Deduplicação por event_id
        print("[>] Deduplicando eventos por event_id (R5)...")
        dedup = bronze.dropDuplicates(["event_id"])

        # R6: Extração dos parâmetros de params para colunas tipadas de primeiro nível
        print("[>] Extraindo campos de params e dimensões aninhadas (R6)...")
        extraido = (
            dedup
            .withColumn("page_title", F.col("params.page_title"))
            .withColumn("percent_scrolled", F.col("params.percent_scrolled"))
            .withColumn("link_url", F.col("params.link_url"))
            .withColumn("link_domain", F.col("params.link_domain"))
            .withColumn("link_text", F.col("params.link_text"))
            .withColumn("link_id", F.col("params.link_id"))
            .withColumn("outbound", F.col("params.outbound"))
            .withColumn("search_term", F.col("params.search_term"))
            .withColumn("search_results", F.col("params.search_results"))
            .withColumn("search_area", F.col("params.search_area"))
            .withColumn("video_title", F.col("params.video_title"))
            .withColumn("video_provider", F.col("params.video_provider"))
            .withColumn("video_duration", F.col("params.video_duration"))
            .withColumn("video_current_time", F.col("params.video_current_time"))
            .withColumn("video_percent", F.col("params.video_percent"))
            .withColumn("file_name", F.col("params.file_name"))
            .withColumn("file_extension", F.col("params.file_extension"))
            .withColumn("item_list_id", F.col("params.item_list_id"))
            .withColumn("item_list_name", F.col("params.item_list_name"))
            .withColumn("currency", F.col("params.currency"))
            .withColumn("value", F.col("params.value"))
            .withColumn("shipping_tier", F.col("params.shipping_tier"))
            .withColumn("shipping", F.col("params.shipping"))
            .withColumn("payment_type", F.col("params.payment_type"))
            .withColumn("transaction_id", F.col("params.transaction_id"))
            .withColumn("tax", F.col("params.tax"))
            .withColumn("coupon", F.col("params.coupon"))
            .withColumn("reason", F.col("params.reason"))
            .withColumn("lead_source", F.col("params.lead_source"))
            .withColumn("method", F.col("params.method"))
            .withColumn("element_id", F.col("params.element_id"))
            # Dimensões da página, tráfego e dispositivo
            .withColumn("page_location", F.col("page.location"))
            .withColumn("page_referrer", F.col("page.referrer"))
            .withColumn("traffic_source", F.col("traffic.source"))
            .withColumn("traffic_medium", F.col("traffic.medium"))
            .withColumn("traffic_campaign", F.col("traffic.campaign"))
            .withColumn("device_category", F.col("device.category"))
            .withColumn("device_os", F.col("device.os"))
            .withColumn("device_browser", F.col("device.browser"))
            .withColumn("ts", F.to_timestamp(F.col("event_ts")))
        )

        # R7: Validação e identificação de motivos de rejeição
        print("[>] Validando dados contra as regras do contrato (R7)...")

        # IDs de compras para checagem de reembolsos órfãos
        purchases_ids = (
            extraido
            .filter((F.col("event_name") == "purchase") & F.col("transaction_id").isNotNull())
            .select("transaction_id")
            .dropDuplicates()
        )

        # Condições de rejeição
        cond_fora_catalogo = ~F.col("event_name").isin(EVENTOS_VALIDOS)
        cond_ts_invalido = F.col("event_ts").isNull() | (F.col("ts") > F.col("_ingestao_ts"))
        cond_sem_transacao = (
            F.col("event_name").isin(["purchase", "refund"])
            & (F.col("transaction_id").isNull() | (F.trim(F.col("transaction_id")) == ""))
        )
        cond_valor_invalido = (
            (F.col("value") < 0)
            | (F.col("value").isNotNull() & (F.col("currency") != "BRL"))
        )
        cond_marco_invalido = (
            (F.col("event_name") == "scroll") & ~F.col("percent_scrolled").isin([25, 50, 75, 90])
        ) | (
            F.col("event_name").startswith("video_")
            & F.col("video_percent").isNotNull()
            & ~F.col("video_percent").isin([0, 10, 25, 50, 75, 100])
        )

        # Junta para validar se o refund tem purchase correspondente
        com_purchase_match = extraido.join(
            purchases_ids.withColumn("purchase_existe", F.lit(True)),
            on="transaction_id",
            how="left"
        )
        cond_refund_orfao = (F.col("event_name") == "refund") & F.col("purchase_existe").isNull()

        # Atribuição dos motivos
        com_rejeicao = com_purchase_match.withColumn(
            "motivo_rejeicao",
            F.when(cond_fora_catalogo, "evento fora do catálogo")
            .when(cond_ts_invalido, "timestamp ausente ou futuro")
            .when(cond_sem_transacao, "transação sem id")
            .when(cond_valor_invalido, "valor monetário inválido")
            .when(cond_marco_invalido, "marco fora do padrão")
            .when(cond_refund_orfao, "reembolso sem compra")
            .otherwise(None)
        ).drop("purchase_existe")

        rejeitados = com_rejeicao.filter(F.col("motivo_rejeicao").isNotNull())
        validos = com_rejeicao.filter(F.col("motivo_rejeicao").isNull()).drop("motivo_rejeicao")

        # Grava eventos_rejeitados
        total_rej = rejeitados.count()
        print(f"[>] Gravando {total_rej} eventos rejeitados em silver/eventos_rejeitados/...")
        (
            rejeitados
            .write
            .mode("overwrite")
            .parquet(caminho("silver/eventos_rejeitados"))
        )

        # R8: Sessionização própria (janela de inatividade > 30 minutos por user_pseudo_id)
        print("[>] Realizando sessionização com janela de inatividade de 30 min (R8)...")
        w_user = Window.partitionBy("user_pseudo_id").orderBy("ts")
        com_sessao = (
            validos
            .withColumn("prev_ts", F.lag("ts").over(w_user))
            .withColumn(
                "diff_sec",
                F.unix_timestamp("ts") - F.unix_timestamp("prev_ts")
            )
            .withColumn(
                "is_new_session",
                F.when(F.col("prev_ts").isNull() | (F.col("diff_sec") > 1800), F.lit(1)).otherwise(F.lit(0))
            )
            .withColumn(
                "session_idx",
                F.sum("is_new_session").over(w_user)
            )
            .withColumn(
                "session_id",
                F.concat(F.col("user_pseudo_id"), F.lit("_s"), F.col("session_idx").cast("string"))
            )
            .drop("prev_ts", "diff_sec", "is_new_session", "session_idx")
        )

        # R9: Identity Stitching (ligar eventos do user_pseudo_id ao user_id pós cadastro/login)
        print("[>] Aplicando identity stitching entre usuário anônimo e cadastrado (R9)...")
        mapa_identidade = (
            com_sessao
            .filter(F.col("user_id").isNotNull())
            .select("user_pseudo_id", F.col("user_id").alias("user_id_resolvido"))
            .dropDuplicates(["user_pseudo_id"])
        )

        silver_final = (
            com_sessao
            .join(mapa_identidade, on="user_pseudo_id", how="left")
            .withColumn("user_id", F.coalesce(F.col("user_id_resolvido"), F.col("user_id")))
            .drop("user_id_resolvido")
        )

        # R10: Tabela silver/itens com items[] explodido
        print("[>] Explodindo items para a tabela silver/itens/ (R10)...")
        itens_df = (
            silver_final
            .filter(F.size(F.col("items")) > 0)
            .select(
                "event_id",
                "event_name",
                "event_ts",
                "session_id",
                "user_pseudo_id",
                "user_id",
                "transaction_id",
                F.explode("items").alias("item"),
                "year",
                "month",
                "day"
            )
            .select(
                "event_id",
                "event_name",
                "event_ts",
                "session_id",
                "user_pseudo_id",
                "user_id",
                "transaction_id",
                F.col("item.item_id").alias("item_id"),
                F.col("item.item_name").alias("item_name"),
                F.col("item.item_category").alias("item_category"),
                F.col("item.price").alias("price"),
                F.col("item.quantity").alias("quantity"),
                "year",
                "month",
                "day"
            )
        )
        total_itens = itens_df.count()
        print(f"[>] Gravando {total_itens} linhas na tabela silver/itens/ particionada por data...")
        (
            itens_df
            .write
            .mode("overwrite")
            .partitionBy("year", "month", "day")
            .parquet(caminho("silver/itens"))
        )

        # Gravação final dos eventos limpos na raiz silver/event_name=.../
        print("[>] Gravando camada Silver particionada por event_name/year/month/day em Parquet...")
        gravar_eventos(silver_final.drop("ts"), "silver", "overwrite")

        total_validos = silver_final.count()
        print(f"[✔] Sucesso: {total_validos} eventos válidos gravados na camada Silver.")
        print(f"[✔] Estatísticas: {total_rej} rejeitados, {total_itens} itens explodidos.")
        titulo("JOB SILVER CONCLUÍDO COM SUCESSO")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
