"""Job Gold: Mapa de Calor (R1 a R3, Seção 5.3, 5.4 e 5.5).

Gera as tabelas:
- gold/mapa_calor_cliques: métricas de cliques por pagina e element_id.
  Colunas exatas: pagina, element_id, cliques, sessoes, sessoes_na_pagina, taxa_clique, intensidade.
- gold/mapa_calor_rolagem: marcos de rolagem (0, 25, 50, 75, 90) na landing (pagina = "/").
  Colunas exatas: pagina, marco, sessoes_que_chegaram, pct_sessoes.
"""

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def extrair_caminho_pagina(col_url):
    """Extrai o caminho sem protocolo/domínio, fragmento (#) ou query (?), mapeando raiz para '/'."""
    # Remove https://.../ ou http://.../
    sem_protocolo = F.regexp_replace(col_url, r"^https?://[^/]+", "")
    # Remove tudo a partir de '?' ou '#'
    sem_query = F.regexp_replace(sem_protocolo, r"[?#].*$", "")
    # Se vazio, vira '/'
    return F.when((sem_query == "") | (sem_query.isNull()), F.lit("/")).otherwise(sem_query)


def main():
    titulo("Gold - Mapa de Calor")
    spark = criar_sessao("gold_mapa_calor")

    exigir_camada(spark, glob_eventos("silver"), "python pipeline/silver.py")

    eventos = ler_eventos(spark, "silver")

    # Adiciona coluna 'pagina' normalizada
    eventos_com_pagina = eventos.withColumn("pagina", extrair_caminho_pagina(F.col("page_location")))

    # ---------------------------------------------------------
    # 1. Tabela mapa_calor_cliques
    # Colunas exatas: pagina, element_id, cliques, sessoes, sessoes_na_pagina, taxa_clique, intensidade
    # ---------------------------------------------------------
    # Sessões distintas com page_view em cada página
    sessoes_por_pagina = (
        eventos_com_pagina.filter(
            (F.col("event_name") == "page_view") &
            F.col("page_location").isNotNull()
        )
        .groupBy("pagina")
        .agg(F.countDistinct("session_id").alias("sessoes_na_pagina"))
    )

    # Eventos de clique com element_id e page_location preenchidos
    cliques_base = (
        eventos_com_pagina.filter(
            F.col("element_id").isNotNull() &
            (F.col("element_id") != "") &
            F.col("page_location").isNotNull()
        )
        .groupBy("pagina", "element_id")
        .agg(
            F.count("event_id").alias("cliques"),
            F.countDistinct("session_id").alias("sessoes"),
        )
    )

    # Maior quantidade de cliques da mesma página para calcular intensidade (0 a 1)
    w_pagina = Window.partitionBy("pagina")

    mapa_cliques = (
        cliques_base.join(sessoes_por_pagina, on="pagina", how="left")
        .fillna({"sessoes_na_pagina": 0})
        .withColumn("max_cliques_pagina", F.max("cliques").over(w_pagina))
        .withColumn("taxa_clique", F.round(F.try_divide(F.col("sessoes"), F.col("sessoes_na_pagina")), 4))
        .withColumn("intensidade", F.round(F.try_divide(F.col("cliques"), F.col("max_cliques_pagina")), 4))
        .select(
            "pagina",
            "element_id",
            "cliques",
            "sessoes",
            "sessoes_na_pagina",
            "taxa_clique",
            "intensidade",
        )
        .orderBy("pagina", F.col("cliques").desc())
    )

    mapa_cliques.write.mode("overwrite").parquet(caminho("gold/mapa_calor_cliques"))
    print(f"[✔] gold/mapa_calor_cliques gravado com {mapa_cliques.count()} linhas.")

    # ---------------------------------------------------------
    # 2. Tabela mapa_calor_rolagem
    # Marcos (0, 25, 50, 75, 90) da landing (pagina = "/")
    # Colunas exatas: pagina, marco, sessoes_que_chegaram, pct_sessoes
    # ---------------------------------------------------------
    eventos_landing = eventos_com_pagina.filter(F.col("pagina") == "/")

    sessoes_landing_pv = (
        eventos_landing.filter(F.col("event_name") == "page_view")
        .select("session_id")
        .distinct()
    )
    total_marco_0 = sessoes_landing_pv.count()

    marcos = [0, 25, 50, 75, 90]
    linhas_marcos = []

    for m in marcos:
        if m == 0:
            qtd = total_marco_0
        else:
            qtd = (
                eventos_landing.filter(
                    (F.col("event_name") == "scroll") &
                    (F.col("percent_scrolled") >= m)
                )
                .join(sessoes_landing_pv, on="session_id", how="inner")
                .select("session_id")
                .distinct()
                .count()
            )
        pct = round(qtd / total_marco_0, 4) if total_marco_0 > 0 else 0.0
        linhas_marcos.append(("/", int(m), int(qtd), float(pct)))

    mapa_rolagem = spark.createDataFrame(
        linhas_marcos,
        ["pagina", "marco", "sessoes_que_chegaram", "pct_sessoes"]
    )

    mapa_rolagem.write.mode("overwrite").parquet(caminho("gold/mapa_calor_rolagem"))
    print(f"[✔] gold/mapa_calor_rolagem gravado com {mapa_rolagem.count()} linhas.")

    spark.stop()
    print("[✔] Job Gold Mapa de Calor concluído com sucesso.")


if __name__ == "__main__":
    main()
