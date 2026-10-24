"""Script de Sequência Completa do Pipeline (Requisito R3).

Executa os 8 jobs em sequência estrita de dependências:
1. bronze.py (Landing -> Bronze)
2. silver.py (Bronze -> Silver)
3. gold_funil.py (Silver -> Gold Funil de Conversão)
4. gold_engajamento.py (Silver -> Gold Engajamento e FAQ)
5. gold_receita.py (Silver -> Gold Receita e Aquisição)
6. gold_ml.py (Silver -> Gold Propensão e Segmentação)
7. gold_mapa_calor.py (Silver -> Gold Mapa de Calor)
8. exportar_mapa_calor.py (Gold -> site/mapa_calor.js)
"""

import subprocess
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent

JOBS = [
    ("Bronze", "bronze.py"),
    ("Silver", "silver.py"),
    ("Gold Funil", "gold_funil.py"),
    ("Gold Engajamento", "gold_engajamento.py"),
    ("Gold Receita", "gold_receita.py"),
    ("Gold ML", "gold_ml.py"),
    ("Gold Mapa de Calor", "gold_mapa_calor.py"),
    ("Exportar Mapa de Calor", "exportar_mapa_calor.py"),
]


def main():
    print("=" * 70)
    print("INICIANDO EXECUÇÃO COMPLETA DO DATALAKE TRACKING (R3)")
    print("=" * 70)

    inicio_total = time.time()
    resultados = []

    for nome, script in JOBS:
        caminho_script = ROOT_DIR / script
        print(f"\n[>] Executando job: {nome} ({script})...")
        t0 = time.time()

        proc = subprocess.run([sys.executable, str(caminho_script)])
        duracao = time.time() - t0

        if proc.returncode != 0:
            print(f"[X] FALHA no job {nome} (código {proc.returncode}) após {duracao:.1f}s.")
            resultados.append((nome, "FALHOU", duracao))
            sys.exit(proc.returncode)
        else:
            print(f"[✔] SUCESSO no job {nome} em {duracao:.1f}s.")
            resultados.append((nome, "SUCESSO", duracao))

    tempo_total = time.time() - inicio_total
    print("\n" + "=" * 70)
    print("RESUMO DA EXECUÇÃO DO DATALAKE (R3)")
    print("=" * 70)
    for nome, status, duracao in resultados:
        print(f"  - {nome:<25} [{status}] em {duracao:>6.1f}s")
    print(f"\nTempo total decorrido: {tempo_total:.1f}s")
    print("=" * 70)


if __name__ == "__main__":
    main()
