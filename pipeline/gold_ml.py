"""Job Gold: Machine Learning (R1 a R3, Seção 5.3 e 5.4).

Gera as tabelas:
- gold/propensao_sessao: probabilidade de compra por sessão (classificação sem vazamento).
- gold/metricas_modelo: métricas (AUC, F1, precisão, recall) e lista de features.
- gold/segmentos_visitantes: clusterização (KMeans) de visitantes e perfil de cada grupo.
"""

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StandardScaler, StringIndexer, VectorAssembler
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window


def main():
    titulo("Gold - Machine Learning")
    spark = criar_sessao("gold_ml")

    exigir_camada(spark, glob_eventos("silver"), "python pipeline/silver.py")

    eventos = ler_eventos(spark, "silver")

    # ---------------------------------------------------------
    # Restrição de Vazamento (Seção 5.3 e 5.4.6):
    # As features só podem usar o comportamento na landing ANTES
    # do primeiro add_to_cart da sessão!
    # Eventos a partir de view_cart/checkout são estritamente proibidos.
    # ---------------------------------------------------------
    # 1. Identificar o timestamp do primeiro add_to_cart por sessão
    primeiro_carrinho = (
        eventos.filter(F.col("event_name") == "add_to_cart")
        .groupBy("session_id")
        .agg(F.min("event_ts").alias("ts_primeiro_carrinho"))
    )

    # 2. Rótulo da sessão: comprou ou não (target = 1.0 se tem purchase, 0.0 caso contrário)
    rotulos = (
        eventos.groupBy("session_id")
        .agg(
            F.max(F.when(F.col("event_name") == "purchase", 1.0).otherwise(0.0)).alias("comprou")
        )
    )

    # 3. Metadados iniciais da sessão (canal e dispositivo do primeiro evento)
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
            F.coalesce(F.col("user_id"), F.col("user_pseudo_id")).alias("pessoa_id"),
        )
    )

    # 4. Filtrar eventos estritamente ANTES do primeiro add_to_cart
    eventos_com_limite = (
        eventos.join(primeiro_carrinho, on="session_id", how="left")
        .filter(
            F.col("ts_primeiro_carrinho").isNull() |
            (F.col("event_ts") < F.col("ts_primeiro_carrinho"))
        )
    )

    ts_epoch = F.to_timestamp("event_ts").cast("long")

    # 5. Engenharia de features pré-checkout na landing
    features_sessao = (
        eventos_com_limite.groupBy("session_id")
        .agg(
            F.count("event_id").alias("eventos_pre_carrinho"),
            (F.max(ts_epoch) - F.min(ts_epoch)).alias("duracao_pre_carrinho"),
            F.coalesce(F.max("percent_scrolled"), F.lit(0)).cast("double").alias("scroll_maximo"),
            F.coalesce(F.max("video_percent"), F.lit(0)).cast("double").alias("maior_video_percent"),
            F.max(F.when(F.col("event_name") == "view_item_list", 1.0).otherwise(0.0)).alias("viu_planos"),
            F.max(F.when(F.col("event_name") == "select_item", 1.0).otherwise(0.0)).alias("selecionou_plano"),
            F.max(F.when(F.col("event_name") == "view_item", 1.0).otherwise(0.0)).alias("viu_detalhe_plano"),
            F.max(F.when(F.col("event_name") == "faq_search", 1.0).otherwise(0.0)).alias("buscou_faq"),
            F.max(F.when(F.col("event_name") == "file_download", 1.0).otherwise(0.0)).alias("baixou_ebook"),
            F.max(F.when(F.col("event_name") == "generate_lead", 1.0).otherwise(0.0)).alias("gerou_lead"),
            F.max(F.when(F.col("event_name") == "outbound_click", 1.0).otherwise(0.0)).alias("clique_saida"),
        )
    )

    # Dataset completo para ML
    dataset_ml = (
        rotulos.join(primeiro_evento, on="session_id", how="inner")
        .join(features_sessao, on="session_id", how="left")
        .fillna({
            "eventos_pre_carrinho": 0,
            "duracao_pre_carrinho": 0,
            "scroll_maximo": 0.0,
            "maior_video_percent": 0.0,
            "viu_planos": 0.0,
            "selecionou_plano": 0.0,
            "viu_detalhe_plano": 0.0,
            "buscou_faq": 0.0,
            "baixou_ebook": 0.0,
            "gerou_lead": 0.0,
            "clique_saida": 0.0,
            "canal": "(direct) / (none)",
            "dispositivo": "desconhecido",
        })
    )

    # ---------------------------------------------------------
    # Treinamento e Avaliação do Modelo de Propensão
    # ---------------------------------------------------------
    indexer_canal = StringIndexer(inputCol="canal", outputCol="canal_idx", handleInvalid="keep")
    indexer_disp = StringIndexer(inputCol="dispositivo", outputCol="dispositivo_idx", handleInvalid="keep")

    feature_cols = [
        "eventos_pre_carrinho",
        "duracao_pre_carrinho",
        "scroll_maximo",
        "maior_video_percent",
        "viu_planos",
        "selecionou_plano",
        "viu_detalhe_plano",
        "buscou_faq",
        "baixou_ebook",
        "gerou_lead",
        "clique_saida",
        "canal_idx",
        "dispositivo_idx",
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="comprou", maxIter=20, regParam=0.01)

    pipeline_ml = Pipeline(stages=[indexer_canal, indexer_disp, assembler, lr])

    # Split treino / teste (80/20) com seed reprodutível
    treino, teste = dataset_ml.randomSplit([0.8, 0.2], seed=42)

    modelo = pipeline_ml.fit(treino)
    previsoes_teste = modelo.transform(teste)

    # Avaliação
    eval_auc = BinaryClassificationEvaluator(labelCol="comprou", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc_val = round(float(eval_auc.evaluate(previsoes_teste)), 4)

    eval_f1 = MulticlassClassificationEvaluator(labelCol="comprou", predictionCol="prediction", metricName="f1")
    f1_val = round(float(eval_f1.evaluate(previsoes_teste)), 4)

    eval_prec = MulticlassClassificationEvaluator(labelCol="comprou", predictionCol="prediction", metricName="precisionByLabel", metricLabel=1.0)
    prec_val = round(float(eval_prec.evaluate(previsoes_teste)), 4)

    eval_rec = MulticlassClassificationEvaluator(labelCol="comprou", predictionCol="prediction", metricName="recallByLabel", metricLabel=1.0)
    rec_val = round(float(eval_rec.evaluate(previsoes_teste)), 4)

    print(f"[ML] Métricas no teste: AUC={auc_val}, F1={f1_val}, Precisão={prec_val}, Recall={rec_val}")

    # Tabela 1: metricas_modelo
    metricas_df = spark.createDataFrame([
        (
            "RegressaoLogistica_PreCheckout",
            auc_val,
            f1_val,
            prec_val,
            rec_val,
            ", ".join(feature_cols),
        )
    ], ["modelo", "auc", "f1", "precisao", "recall", "features_utilizadas"])

    metricas_df.write.mode("overwrite").parquet(caminho("gold/metricas_modelo"))
    print(f"[✔] gold/metricas_modelo gravado.")

    # Tabela 2: propensao_sessao (todas as sessões da Silver)
    previsoes_todas = modelo.transform(dataset_ml)

    # Extrair probabilidade da classe 1 (compra)
    extrair_prob = F.udf(lambda v: float(v[1]) if v is not None and len(v) > 1 else 0.0, DoubleType())

    propensao_df = (
        previsoes_todas.select(
            "session_id",
            F.col("comprou").cast("int").alias("comprou_real"),
            F.round(extrair_prob("probability"), 4).alias("probabilidade_compra"),
            F.col("prediction").cast("int").alias("previsao_compra"),
        )
    )

    propensao_df.write.mode("overwrite").parquet(caminho("gold/propensao_sessao"))
    print(f"[✔] gold/propensao_sessao gravado com {propensao_df.count()} linhas.")

    # ---------------------------------------------------------
    # 3. Tabela segmentos_visitantes (Clusterização KMeans)
    # Perfil comportamental agregado por pessoa (identity stitching)
    # ---------------------------------------------------------
    perfil_visitantes = (
        dataset_ml.groupBy("pessoa_id")
        .agg(
            F.count("session_id").cast("double").alias("total_sessoes"),
            F.round(F.avg("eventos_pre_carrinho"), 2).cast("double").alias("media_eventos"),
            F.max("scroll_maximo").cast("double").alias("max_scroll"),
            F.max("maior_video_percent").cast("double").alias("max_video"),
            F.max("gerou_lead").cast("double").alias("taxa_lead"),
            F.max("comprou").cast("double").alias("taxa_compra"),
        )
    )

    cluster_features = ["total_sessoes", "media_eventos", "max_scroll", "max_video", "taxa_lead", "taxa_compra"]
    cluster_assembler = VectorAssembler(inputCols=cluster_features, outputCol="features_cluster_raw")
    scaler = StandardScaler(inputCol="features_cluster_raw", outputCol="features_cluster", withStd=True, withMean=True)
    kmeans = KMeans(featuresCol="features_cluster", k=3, seed=42, predictionCol="cluster")

    pipeline_cluster = Pipeline(stages=[cluster_assembler, scaler, kmeans])
    modelo_cluster = pipeline_cluster.fit(perfil_visitantes)
    visitantes_clusterizados = modelo_cluster.transform(perfil_visitantes)

    # Perfil médio de cada cluster
    segmentos_df = (
        visitantes_clusterizados.groupBy("cluster")
        .agg(
            F.count("pessoa_id").alias("total_visitantes"),
            F.round(F.avg("total_sessoes"), 2).alias("media_sessoes"),
            F.round(F.avg("media_eventos"), 2).alias("media_eventos"),
            F.round(F.avg("max_scroll"), 2).alias("media_scroll_max"),
            F.round(F.avg("max_video"), 2).alias("media_video_max"),
            F.round(F.avg("taxa_lead"), 4).alias("taxa_conversao_lead"),
            F.round(F.avg("taxa_compra"), 4).alias("taxa_conversao_compra"),
        )
        .withColumn(
            "perfil_interpretado",
            F.when(F.col("taxa_conversao_compra") > 0.15, "Compradores e Alta Intenção")
            .when(F.col("taxa_conversao_lead") > 0.15, "Leads Engajados / Conteúdo")
            .otherwise("Visitantes Casuais / Baixo Engajamento")
        )
        .orderBy("cluster")
    )

    segmentos_df.write.mode("overwrite").parquet(caminho("gold/segmentos_visitantes"))
    print(f"[✔] gold/segmentos_visitantes gravado com {segmentos_df.count()} clusters.")

    spark.stop()
    print("[✔] Job Gold ML concluído com sucesso.")


if __name__ == "__main__":
    main()
