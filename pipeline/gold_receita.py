"""Job Gold: Receita e Aquisição (R1 a R3, Seção 5.3 e 5.4).

Gera as tabelas:
- gold/receita_diaria: por data da compra (UTC), plano e canal: receita bruta,
  reembolsada e líquida, pedidos, ticket médio.
- gold/taxa_reembolso: por plano e canal da compra: pedidos, pedidos com reembolso, taxa.
- gold/adesao_order_bump: percentual de compras com BUMP-MENTORIA.
- gold/lead_para_compra: leads, quantos compraram depois, mediana de dias entre lead e compra
  (total e por canal do lead).
"""

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main():
    titulo("Gold - Receita e Aquisição")
    spark = criar_sessao("gold_receita")

    exigir_camada(spark, glob_eventos("silver"), "python pipeline/silver.py")

    eventos = ler_eventos(spark, "silver")
    itens = spark.read.parquet(caminho("silver/itens"))

    # ---------------------------------------------------------
    # Canal de cada sessão (primeiro evento da sessão)
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
        .select("session_id", canal_expr.alias("canal"))
    )

    # ---------------------------------------------------------
    # Purchases e Refunds
    # ---------------------------------------------------------
    # Plano da compra: o item de categoria 'plano' no items do purchase (Seção 5.4.3)
    itens_plano = (
        itens.filter((F.col("event_name") == "purchase") & (F.col("item_category") == "plano"))
        .select(
            "transaction_id",
            F.col("item_id").alias("plano"),
        )
        .dropDuplicates(["transaction_id"])
    )

    # Order bump: compras que incluem BUMP-MENTORIA
    itens_bump = (
        itens.filter((F.col("event_name") == "purchase") & (F.col("item_id") == "BUMP-MENTORIA"))
        .select("transaction_id")
        .dropDuplicates()
        .withColumn("tem_bump", F.lit(1))
    )

    # Compras limpas
    purchases = (
        eventos.filter(F.col("event_name") == "purchase")
        .select(
            "transaction_id",
            "session_id",
            F.coalesce(F.col("user_id"), F.col("user_pseudo_id")).alias("pessoa_id"),
            "event_ts",
            F.to_date(F.col("event_ts")).alias("data_compra"),
            F.col("value").cast("double").alias("valor_compra"),
        )
        .dropDuplicates(["transaction_id"])
        .join(primeiro_evento, on="session_id", how="left")
        .fillna({"canal": "(direct) / (none)"})
        .join(itens_plano, on="transaction_id", how="left")
        .fillna({"plano": "PLANO-DESCONHECIDO"})
        .join(itens_bump, on="transaction_id", how="left")
        .fillna({"tem_bump": 0})
    )

    # Reembolsos válidos agrupados por transaction_id
    refunds = (
        eventos.filter(F.col("event_name") == "refund")
        .groupBy("transaction_id")
        .agg(
            F.sum(F.col("value").cast("double")).alias("valor_reembolsado"),
            F.lit(1).alias("teve_reembolso"),
        )
    )

    # Junção de purchases com refunds atribuídos pelo transaction_id (Seção 5.4.2)
    compras_consolidadas = (
        purchases.join(refunds, on="transaction_id", how="left")
        .fillna({"valor_reembolsado": 0.0, "teve_reembolso": 0})
        .withColumn("valor_liquido", F.round(F.col("valor_compra") - F.col("valor_reembolsado"), 2))
    )

    # ---------------------------------------------------------
    # 1. Tabela receita_diaria
    # Por data da compra, plano e canal: receita bruta, reembolsada,
    # líquida, pedidos, ticket médio.
    # ---------------------------------------------------------
    receita_diaria = (
        compras_consolidadas.groupBy("data_compra", "plano", "canal")
        .agg(
            F.round(F.sum("valor_compra"), 2).alias("receita_bruta"),
            F.round(F.sum("valor_reembolsado"), 2).alias("receita_reembolsada"),
            F.round(F.sum("valor_liquido"), 2).alias("receita_liquida"),
            F.count("transaction_id").alias("pedidos"),
        )
        .withColumn("ticket_medio", F.round(F.try_divide(F.col("receita_bruta"), F.col("pedidos")), 2))
        .orderBy("data_compra", "plano", "canal")
    )

    receita_diaria.write.mode("overwrite").parquet(caminho("gold/receita_diaria"))
    print(f"[✔] gold/receita_diaria gravado com {receita_diaria.count()} linhas.")

    # ---------------------------------------------------------
    # 2. Tabela taxa_reembolso
    # Por plano e canal: pedidos, pedidos com reembolso, taxa.
    # ---------------------------------------------------------
    taxa_reembolso_detalhada = (
        compras_consolidadas.groupBy("plano", "canal")
        .agg(
            F.count("transaction_id").alias("pedidos"),
            F.sum("teve_reembolso").alias("pedidos_com_reembolso"),
        )
        .withColumn("taxa_reembolso", F.round(F.try_divide(F.col("pedidos_com_reembolso"), F.col("pedidos")), 4))
    )

    taxa_reembolso_total = (
        compras_consolidadas.groupBy()
        .agg(
            F.count("transaction_id").alias("pedidos"),
            F.sum("teve_reembolso").alias("pedidos_com_reembolso"),
        )
        .withColumn("plano", F.lit("TODOS"))
        .withColumn("canal", F.lit("TODOS"))
        .withColumn("taxa_reembolso", F.round(F.try_divide(F.col("pedidos_com_reembolso"), F.col("pedidos")), 4))
        .select(taxa_reembolso_detalhada.columns)
    )

    taxa_reembolso_final = taxa_reembolso_total.unionByName(taxa_reembolso_detalhada)
    taxa_reembolso_final.write.mode("overwrite").parquet(caminho("gold/taxa_reembolso"))
    print(f"[✔] gold/taxa_reembolso gravado com {taxa_reembolso_final.count()} linhas.")

    # ---------------------------------------------------------
    # 3. Tabela adesao_order_bump
    # Percentual de compras com BUMP-MENTORIA
    # ---------------------------------------------------------
    adesao_bump = (
        compras_consolidadas.groupBy("plano")
        .agg(
            F.count("transaction_id").alias("total_compras"),
            F.sum("tem_bump").alias("compras_com_bump"),
        )
        .withColumn("taxa_adesao_bump", F.round(F.try_divide(F.col("compras_com_bump"), F.col("total_compras")), 4))
    )

    adesao_bump_total = (
        compras_consolidadas.groupBy()
        .agg(
            F.count("transaction_id").alias("total_compras"),
            F.sum("tem_bump").alias("compras_com_bump"),
        )
        .withColumn("plano", F.lit("TODOS"))
        .withColumn("taxa_adesao_bump", F.round(F.try_divide(F.col("compras_com_bump"), F.col("total_compras")), 4))
        .select(adesao_bump.columns)
    )

    adesao_bump_final = adesao_bump_total.unionByName(adesao_bump)
    adesao_bump_final.write.mode("overwrite").parquet(caminho("gold/adesao_order_bump"))
    print(f"[✔] gold/adesao_order_bump gravado com {adesao_bump_final.count()} linhas.")

    # ---------------------------------------------------------
    # 4. Tabela lead_para_compra
    # Pessoa: user_id resolvido (ou user_pseudo_id).
    # Primeiro generate_lead da pessoa e primeira compra estritamente depois dele.
    # Dias = diferença em segundos / 86400 (fracionário); mediana desses dias.
    # Canal do lead = canal da sessão do primeiro generate_lead.
    # Total e por canal do lead. (Seção 5.4.5, 5.4.8, 5.4.9)
    # ---------------------------------------------------------
    leads = (
        eventos.filter(F.col("event_name") == "generate_lead")
        .select(
            F.coalesce(F.col("user_id"), F.col("user_pseudo_id")).alias("pessoa_id"),
            F.col("event_ts").alias("ts_lead"),
            "session_id",
        )
        .withColumn("rn", F.row_number().over(Window.partitionBy("pessoa_id").orderBy("ts_lead")))
        .filter(F.col("rn") == 1)
        .join(primeiro_evento, on="session_id", how="left")
        .fillna({"canal": "(direct) / (none)"})
        .select("pessoa_id", "ts_lead", F.col("canal").alias("canal_lead"))
    )

    # Compras por pessoa
    compras_pessoa = (
        purchases.select("pessoa_id", F.col("event_ts").alias("ts_compra"))
    )

    # Lead com compra estritamente depois (ts_compra > ts_lead)
    leads_com_compra = (
        leads.join(compras_pessoa, on="pessoa_id", how="inner")
        .filter(F.col("ts_compra") > F.col("ts_lead"))
        .withColumn("rn_compra", F.row_number().over(Window.partitionBy("pessoa_id").orderBy("ts_compra")))
        .filter(F.col("rn_compra") == 1)
        .withColumn(
            "dias_ate_compra",
            (F.to_timestamp("ts_compra").cast("long") - F.to_timestamp("ts_lead").cast("long")) / 86400.0
        )
    )

    # Consolidação por canal do lead
    leads_por_canal = (
        leads.groupBy("canal_lead")
        .agg(F.count("pessoa_id").alias("total_leads"))
    )

    conversao_leads_canal = (
        leads_com_compra.groupBy("canal_lead")
        .agg(
            F.count("pessoa_id").alias("leads_compraram"),
            F.round(F.percentile_approx("dias_ate_compra", 0.5), 2).alias("mediana_dias_compra"),
        )
    )

    lead_compra_detalhado = (
        leads_por_canal.join(conversao_leads_canal, on="canal_lead", how="left")
        .fillna({"leads_compraram": 0, "mediana_dias_compra": 0.0})
        .withColumn("taxa_conversao_lead", F.round(F.try_divide(F.col("leads_compraram"), F.col("total_leads")), 4))
    )

    # Total geral
    lead_compra_total = (
        leads.agg(F.count("pessoa_id").alias("total_leads"))
        .crossJoin(
            leads_com_compra.agg(
                F.count("pessoa_id").alias("leads_compraram"),
                F.round(F.percentile_approx("dias_ate_compra", 0.5), 2).alias("mediana_dias_compra"),
            )
        )
        .withColumn("canal_lead", F.lit("TODOS"))
        .withColumn("taxa_conversao_lead", F.round(F.try_divide(F.col("leads_compraram"), F.col("total_leads")), 4))
        .select(lead_compra_detalhado.columns)
    )

    lead_compra_final = lead_compra_total.unionByName(lead_compra_detalhado)
    lead_compra_final.write.mode("overwrite").parquet(caminho("gold/lead_para_compra"))
    print(f"[✔] gold/lead_para_compra gravado com {lead_compra_final.count()} linhas.")

    spark.stop()
    print("[✔] Job Gold Receita e Aquisição concluído com sucesso.")


if __name__ == "__main__":
    main()
