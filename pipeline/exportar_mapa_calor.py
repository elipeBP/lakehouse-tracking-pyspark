"""Job de Exportação do Mapa de Calor para o Site (R11, Seção 5.5).

Lê as duas tabelas da Gold:
- gold/mapa_calor_cliques
- gold/mapa_calor_rolagem

Gera o arquivo local site/mapa_calor.js com a linha:
window.MAPA_CALOR = {"gerado_em": "...", "cliques": [...], "rolagem": [...]};
"""

import json
from datetime import datetime, timezone

from comum import arquivo_do_projeto, caminho, criar_sessao, exigir_camada, titulo


def main():
    titulo("Exportar Mapa de Calor")
    spark = criar_sessao("exportar_mapa_calor")

    exigir_camada(spark, "gold/mapa_calor_cliques", "python pipeline/gold_mapa_calor.py")
    exigir_camada(spark, "gold/mapa_calor_rolagem", "python pipeline/gold_mapa_calor.py")

    df_cliques = spark.read.parquet(caminho("gold/mapa_calor_cliques"))
    df_rolagem = spark.read.parquet(caminho("gold/mapa_calor_rolagem"))

    cliques = [linha.asDict() for linha in df_cliques.collect()]
    rolagem = [linha.asDict() for linha in df_rolagem.collect()]

    agora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    dados = {
        "gerado_em": agora,
        "cliques": cliques,
        "rolagem": rolagem,
    }

    destino = arquivo_do_projeto("site/mapa_calor.js")
    texto = json.dumps(dados, ensure_ascii=False, default=float)
    destino.write_text("window.MAPA_CALOR = " + texto + ";\n", encoding="utf-8")

    print(f"[✔] Mapa de calor exportado com sucesso para: {destino}")
    print(f"    - {len(cliques)} elementos com cliques mapeados")
    print(f"    - {len(rolagem)} marcos de rolagem mapeados")

    spark.stop()


if __name__ == "__main__":
    main()
