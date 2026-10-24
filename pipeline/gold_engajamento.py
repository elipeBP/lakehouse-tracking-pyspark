"""Job Gold: Engajamento (R1 a R3, Seção 5.3 e 5.4).

Gera as tabelas:
- gold/sessoes: uma linha por sessão (duração, eventos, scroll máximo, maior video_percent,
  fez lead, comprou, canal, dispositivo).
- gold/retencao_video: percentual de sessões com video_start que atingiram 10, 25, 50, 75 e 100%.
- gold/buscas_faq: termo de busca, total de buscas, buscas sem resultado.
- gold/saidas_downloads: cliques de saída por domínio; downloads por arquivo.
"""

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main():
    titulo("Gold - Engajamento")
    spark = criar_sessao("gold_engajamento")

    exigir_camada(spark, glob_eventos("silver"), "python pipeline/silver.py")

    eventos = ler_eventos(spark, "silver")

    # ---------------------------------------------------------
    # 1. Tabela sessoes
    # Uma linha por sessão: duração, eventos, scroll máximo,
    # maior video_percent, fez lead, comprou, canal, dispositivo.
    # ---------------------------------------------------------
    w_sessao = Window.partitionBy("session_id").orderBy("event_ts")

    canal_expr = F.concat(
        F.coalesce(F.col("traffic_source"), F.lit("(direct)")),
        F.lit(" / "),
        F.coalesce(F.col("traffic_medium"), F.lit("(none)")),
    )

    primeiro_evento = (
        eventos.withColumn("rn", F.row_number().over(w_sessao))
        .filter(F.col("rn") == 1)
        .select(
            "session_id",
            canal_expr.alias("canal"),
            F.coalesce(F.col("device_category"), F.lit("desconhecido")).alias("dispositivo"),
            F.coalesce(F.col("user_id"), F.col("user_pseudo_id")).alias("user_id_resolvido"),
        )
    )

    ts_epoch = F.to_timestamp("event_ts").cast("long")

    agregados_sessao = (
        eventos.groupBy("session_id")
        .agg(
            (F.max(ts_epoch) - F.min(ts_epoch)).alias("duracao_segundos"),
            F.count("event_id").alias("quantidade_eventos"),
            F.coalesce(F.max("percent_scrolled"), F.lit(0)).alias("scroll_maximo"),
            F.coalesce(F.max("video_percent"), F.lit(0)).alias("maior_video_percent"),
            F.when(F.max(F.when(F.col("event_name") == "generate_lead", 1).otherwise(0)) == 1, True).otherwise(False).alias("fez_lead"),
            F.when(F.max(F.when(F.col("event_name") == "purchase", 1).otherwise(0)) == 1, True).otherwise(False).alias("comprou"),
        )
    )

    tabela_sessoes = (
        primeiro_evento.join(agregados_sessao, on="session_id", how="inner")
        .select(
            "session_id",
            "user_id_resolvido",
            "canal",
            "dispositivo",
            "duracao_segundos",
            "quantidade_eventos",
            "scroll_maximo",
            "maior_video_percent",
            "fez_lead",
            "comprou",
        )
    )

    tabela_sessoes.write.mode("overwrite").parquet(caminho("gold/sessoes"))
    print(f"[✔] gold/sessoes gravado com {tabela_sessoes.count()} linhas.")

    # ---------------------------------------------------------
    # 2. Tabela retencao_video
    # Das sessões com video_start, percentual que atingiu 10, 25, 50, 75 e 100%.
    # ---------------------------------------------------------
    sessoes_video = (
        eventos.filter(
            (F.col("event_name").isin("video_start", "video_progress", "video_complete")) |
            (F.col("video_percent").isNotNull())
        )
        .groupBy("session_id")
        .agg(
            F.max(F.when(F.col("event_name") == "video_start", 1).otherwise(0)).alias("iniciou"),
            F.max(F.coalesce(F.col("video_percent"), F.lit(0))).alias("max_pct"),
            F.max(F.when(F.col("event_name") == "video_complete", 1).otherwise(0)).alias("completou"),
        )
        .filter((F.col("iniciou") == 1) | (F.col("max_pct") > 0))
    )

    retencao = (
        sessoes_video.agg(
            F.count("session_id").alias("sessoes_iniciaram"),
            F.sum(F.when(F.col("max_pct") >= 10, 1).otherwise(0)).alias("atingiram_10"),
            F.sum(F.when(F.col("max_pct") >= 25, 1).otherwise(0)).alias("atingiram_25"),
            F.sum(F.when(F.col("max_pct") >= 50, 1).otherwise(0)).alias("atingiram_50"),
            F.sum(F.when(F.col("max_pct") >= 75, 1).otherwise(0)).alias("atingiram_75"),
            F.sum(F.when((F.col("max_pct") >= 100) | (F.col("completou") == 1), 1).otherwise(0)).alias("atingiram_100"),
        )
        .withColumn("pct_10", F.round(F.try_divide(F.col("atingiram_10"), F.col("sessoes_iniciaram")), 4))
        .withColumn("pct_25", F.round(F.try_divide(F.col("atingiram_25"), F.col("sessoes_iniciaram")), 4))
        .withColumn("pct_50", F.round(F.try_divide(F.col("atingiram_50"), F.col("sessoes_iniciaram")), 4))
        .withColumn("pct_75", F.round(F.try_divide(F.col("atingiram_75"), F.col("sessoes_iniciaram")), 4))
        .withColumn("pct_100", F.round(F.try_divide(F.col("atingiram_100"), F.col("sessoes_iniciaram")), 4))
    )

    retencao.write.mode("overwrite").parquet(caminho("gold/retencao_video"))
    print(f"[✔] gold/retencao_video gravado com {retencao.count()} linhas.")

    # ---------------------------------------------------------
    # 3. Tabela buscas_faq
    # Termo, total de buscas, buscas sem resultado.
    # ---------------------------------------------------------
    faq = (
        eventos.filter(
            (F.col("event_name") == "faq_search") |
            (F.col("search_term").isNotNull())
        )
        .groupBy(F.lower(F.trim(F.col("search_term"))).alias("termo"))
        .agg(
            F.count("event_id").alias("total_buscas"),
            F.sum(F.when(F.coalesce(F.col("search_results"), F.lit(0)) == 0, 1).otherwise(0)).alias("buscas_sem_resultado"),
        )
        .filter(F.col("termo").isNotNull() & (F.col("termo") != ""))
        .orderBy(F.col("total_buscas").desc())
    )

    faq.write.mode("overwrite").parquet(caminho("gold/buscas_faq"))
    print(f"[✔] gold/buscas_faq gravado com {faq.count()} linhas.")

    # ---------------------------------------------------------
    # 4. Tabela saidas_downloads
    # Cliques de saída por domínio; downloads por arquivo.
    # ---------------------------------------------------------
    saidas = (
        eventos.filter(
            (F.col("event_name") == "outbound_click") |
            (F.col("link_domain").isNotNull())
        )
        .groupBy(F.col("link_domain").alias("destino"))
        .agg(
            F.count("event_id").alias("total_ocorrencias"),
            F.countDistinct("session_id").alias("sessoes_distintas"),
        )
        .filter(F.col("destino").isNotNull())
        .withColumn("tipo", F.lit("saida_dominio"))
    )

    downloads = (
        eventos.filter(
            (F.col("event_name") == "file_download") |
            (F.col("file_name").isNotNull())
        )
        .groupBy(F.col("file_name").alias("destino"))
        .agg(
            F.count("event_id").alias("total_ocorrencias"),
            F.countDistinct("session_id").alias("sessoes_distintas"),
        )
        .filter(F.col("destino").isNotNull())
        .withColumn("tipo", F.lit("download_arquivo"))
    )

    saidas_downloads = (
        saidas.unionByName(downloads)
        .select("tipo", "destino", "total_ocorrencias", "sessoes_distintas")
        .orderBy(F.col("tipo"), F.col("total_ocorrencias").desc())
    )

    saidas_downloads.write.mode("overwrite").parquet(caminho("gold/saidas_downloads"))
    print(f"[✔] gold/saidas_downloads gravado com {saidas_downloads.count()} linhas.")

    spark.stop()
    print("[✔] Job Gold Engajamento concluído com sucesso.")


if __name__ == "__main__":
    main()
