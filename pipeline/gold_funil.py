"""Job Gold: Funil de Conversão (R1 a R3, Seção 5.3 e 5.4).

Gera as tabelas:
- gold/funil_compra: etapas do funil de compra (view_item_list -> select_item ->
  add_to_cart -> view_cart -> begin_checkout -> add_payment_info -> purchase),
  conversões sobre etapa anterior e sobre o topo, taxa de view_item,
  por plano x canal e total.
- gold/funil_lead: page_view da landing -> scroll >= 50% -> generate_lead, por canal.
"""

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main():
    titulo("Gold - Funil")
    spark = criar_sessao("gold_funil")

    exigir_camada(spark, glob_eventos("silver"), "python pipeline/silver.py")

    eventos = ler_eventos(spark, "silver")
    itens = spark.read.parquet(caminho("silver/itens"))

    # ---------------------------------------------------------
    # 1. Atributos da Sessão: Canal e Plano
    # Canal: "<source> / <medium>" do primeiro evento da sessão
    # Plano: item_id do primeiro select_item da sessão ('(nenhum)' se não houver)
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
        )
    )

    # Plano da sessão: primeiro select_item
    select_items = (
        itens.filter(F.col("event_name") == "select_item")
        .withColumn("rn", F.row_number().over(w_sessao))
        .filter(F.col("rn") == 1)
        .select("session_id", F.col("item_id").alias("plano"))
    )

    sessoes_meta = (
        primeiro_evento.join(select_items, on="session_id", how="left")
        .fillna({"plano": "(nenhum)"})
    )

    # ---------------------------------------------------------
    # 2. Marcação de Etapas por Sessão
    # view_item_list, select_item, add_to_cart, view_cart,
    # begin_checkout, add_payment_info, purchase, view_item
    # ---------------------------------------------------------
    etapas_sessao = (
        eventos.groupBy("session_id")
        .agg(
            F.max(F.when(F.col("event_name") == "view_item_list", 1).otherwise(0)).alias("tem_view_item_list"),
            F.max(F.when(F.col("event_name") == "select_item", 1).otherwise(0)).alias("tem_select_item"),
            F.max(F.when(F.col("event_name") == "add_to_cart", 1).otherwise(0)).alias("tem_add_to_cart"),
            F.max(F.when(F.col("event_name") == "view_cart", 1).otherwise(0)).alias("tem_view_cart"),
            F.max(F.when(F.col("event_name") == "begin_checkout", 1).otherwise(0)).alias("tem_begin_checkout"),
            F.max(F.when(F.col("event_name") == "add_payment_info", 1).otherwise(0)).alias("tem_add_payment_info"),
            F.max(F.when(F.col("event_name") == "purchase", 1).otherwise(0)).alias("tem_purchase"),
            F.max(F.when(F.col("event_name") == "view_item", 1).otherwise(0)).alias("tem_view_item"),
        )
    )

    funil_base = sessoes_meta.join(etapas_sessao, on="session_id", how="inner")

    # ---------------------------------------------------------
    # 3. Tabela funil_compra (Total e por plano x canal)
    # ---------------------------------------------------------
    def agregar_funil(df, colunas_agrupamento):
        return (
            df.groupBy(*colunas_agrupamento)
            .agg(
                F.count("session_id").alias("sessoes_total"),
                F.sum("tem_view_item_list").alias("view_item_list"),
                F.sum("tem_select_item").alias("select_item"),
                F.sum("tem_add_to_cart").alias("add_to_cart"),
                F.sum("tem_view_cart").alias("view_cart"),
                F.sum("tem_begin_checkout").alias("begin_checkout"),
                F.sum("tem_add_payment_info").alias("add_payment_info"),
                F.sum("tem_purchase").alias("purchase"),
                F.sum("tem_view_item").alias("view_item"),
            )
            .withColumn("taxa_view_item", F.round(F.try_divide(F.col("view_item"), F.col("select_item")), 4))
            # Conversão sobre etapa anterior
            .withColumn("conv_select_item", F.round(F.try_divide(F.col("select_item"), F.col("view_item_list")), 4))
            .withColumn("conv_add_to_cart", F.round(F.try_divide(F.col("add_to_cart"), F.col("select_item")), 4))
            .withColumn("conv_view_cart", F.round(F.try_divide(F.col("view_cart"), F.col("add_to_cart")), 4))
            .withColumn("conv_begin_checkout", F.round(F.try_divide(F.col("begin_checkout"), F.col("view_cart")), 4))
            .withColumn("conv_add_payment_info", F.round(F.try_divide(F.col("add_payment_info"), F.col("begin_checkout")), 4))
            .withColumn("conv_purchase", F.round(F.try_divide(F.col("purchase"), F.col("add_payment_info")), 4))
            # Conversão sobre o topo (view_item_list)
            .withColumn("conv_topo_purchase", F.round(F.try_divide(F.col("purchase"), F.col("view_item_list")), 4))
        )

    funil_detalhado = agregar_funil(funil_base, ["plano", "canal"])
    funil_total = (
        agregar_funil(funil_base, [])
        .withColumn("plano", F.lit("TODOS"))
        .withColumn("canal", F.lit("TODOS"))
        .select(funil_detalhado.columns)
    )

    funil_compra_final = funil_total.unionByName(funil_detalhado)
    (
        funil_compra_final.write.mode("overwrite")
        .parquet(caminho("gold/funil_compra"))
    )
    print(f"[✔] gold/funil_compra gravado com {funil_compra_final.count()} linhas.")

    # ---------------------------------------------------------
    # 4. Tabela funil_lead (page_view -> scroll >= 50% -> generate_lead por canal)
    # ---------------------------------------------------------
    lead_etapas = (
        eventos.groupBy("session_id")
        .agg(
            # page_view da landing (page_location = https://loja.exemplo/ ou raiz)
            F.max(
                F.when(
                    (F.col("event_name") == "page_view") &
                    (F.col("page_location").isNull() | (F.col("page_location") == "https://loja.exemplo/") | (F.col("page_location") == "https://loja.exemplo")),
                    1
                ).otherwise(0)
            ).alias("tem_page_view_landing"),
            F.max(
                F.when((F.col("event_name") == "scroll") & (F.col("percent_scrolled") >= 50), 1).otherwise(0)
            ).alias("tem_scroll_50"),
            F.max(
                F.when(F.col("event_name") == "generate_lead", 1).otherwise(0)
            ).alias("tem_generate_lead"),
        )
    )

    funil_lead_base = primeiro_evento.join(lead_etapas, on="session_id", how="inner")

    funil_lead_detalhado = (
        funil_lead_base.groupBy("canal")
        .agg(
            F.count("session_id").alias("sessoes_total"),
            F.sum("tem_page_view_landing").alias("sessoes_page_view"),
            F.sum("tem_scroll_50").alias("sessoes_scroll_50"),
            F.sum("tem_generate_lead").alias("sessoes_generate_lead"),
        )
        .withColumn("conv_scroll_sobre_pv", F.round(F.try_divide(F.col("sessoes_scroll_50"), F.col("sessoes_page_view")), 4))
        .withColumn("conv_lead_sobre_scroll", F.round(F.try_divide(F.col("sessoes_generate_lead"), F.col("sessoes_scroll_50")), 4))
        .withColumn("conv_lead_sobre_topo", F.round(F.try_divide(F.col("sessoes_generate_lead"), F.col("sessoes_page_view")), 4))
    )

    funil_lead_total = (
        funil_lead_base.groupBy()
        .agg(
            F.count("session_id").alias("sessoes_total"),
            F.sum("tem_page_view_landing").alias("sessoes_page_view"),
            F.sum("tem_scroll_50").alias("sessoes_scroll_50"),
            F.sum("tem_generate_lead").alias("sessoes_generate_lead"),
        )
        .withColumn("canal", F.lit("TODOS"))
        .withColumn("conv_scroll_sobre_pv", F.round(F.try_divide(F.col("sessoes_scroll_50"), F.col("sessoes_page_view")), 4))
        .withColumn("conv_lead_sobre_scroll", F.round(F.try_divide(F.col("sessoes_generate_lead"), F.col("sessoes_scroll_50")), 4))
        .withColumn("conv_lead_sobre_topo", F.round(F.try_divide(F.col("sessoes_generate_lead"), F.col("sessoes_page_view")), 4))
        .select(funil_lead_detalhado.columns)
    )

    funil_lead_final = funil_lead_total.unionByName(funil_lead_detalhado)
    (
        funil_lead_final.write.mode("overwrite")
        .parquet(caminho("gold/funil_lead"))
    )
    print(f"[✔] gold/funil_lead gravado com {funil_lead_final.count()} linhas.")

    spark.stop()
    print("[✔] Job Gold Funil concluído com sucesso.")


if __name__ == "__main__":
    main()
