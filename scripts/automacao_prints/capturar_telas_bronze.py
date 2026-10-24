"""Script de automação para captura de telas e geração dos entregáveis da Aula Bronze.

Gera:
1. Print da tela de bucket (S3) na pasta bronze/
2. Print da tela do Data Catalog com o Schema da tabela bronze.eventos
3. Print da tela do Glue Jobs ETL com a execução com sucesso do job bronze
4. Script do ETL Job (bronze.py)
5. Print da tela de consulta do Athena (Query executada)
6. CSV com os dados da consulta exportados pelo Athena
7. Pacote ZIP final pronto para submissão: Aula_Bronze.zip
"""

import os
import shutil
import time
import zipfile
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

ROOT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT_DIR.parent / "entregaveis_aula_bronze"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def iniciar_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1600,1050")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Edge(options=options)
    driver.set_page_load_timeout(30)
    return driver


def mudar_tela(driver, nome_tela):
    """Muda a tela ativa pelo menu de rádio lateral do Streamlit."""
    time.sleep(2)
    labels = driver.find_elements(By.CSS_SELECTOR, "div[data-testid='stRadio'] label")
    for lbl in labels:
        if nome_tela.lower() in lbl.text.lower():
            lbl.click()
            time.sleep(3)
            return True
    return False


def capturar_bucket_s3(driver):
    print("[1/4] Capturando tela do Bucket S3 (camada bronze)...")
    mudar_tela(driver, "Buckets")
    time.sleep(3)

    # Clica no link do bucket datalake-tracking
    links = driver.find_elements(By.TAG_NAME, "button")
    for btn in links:
        if "datalake-tracking" in btn.text:
            btn.click()
            time.sleep(3)
            break

    # Clica na pasta bronze/
    links = driver.find_elements(By.TAG_NAME, "button")
    for btn in links:
        if "bronze/" in btn.text:
            btn.click()
            time.sleep(3)
            break

    screenshot_path = OUTPUT_DIR / "1_print_bucket_s3_bronze.png"
    driver.save_screenshot(str(screenshot_path))
    print(f"  [+] Salvo em: {screenshot_path}")
    return screenshot_path


def capturar_data_catalog(driver):
    print("[2/4] Capturando tela do Data Catalog (schema bronze.eventos)...")
    mudar_tela(driver, "Data Catalog")
    time.sleep(4)

    # 1. Clica no banco bronze
    for btn in driver.find_elements(By.TAG_NAME, "button"):
        if btn.text.strip() == "bronze":
            btn.click()
            time.sleep(3)
            break

    # 2. Clica na tabela eventos dentro do banco bronze
    for btn in driver.find_elements(By.TAG_NAME, "button"):
        if btn.text.strip() == "eventos":
            btn.click()
            time.sleep(3)
            break

    screenshot_path = OUTPUT_DIR / "2_print_datacatalog_schema_bronze.png"
    driver.save_screenshot(str(screenshot_path))
    print(f"  [+] Salvo em: {screenshot_path}")
    return screenshot_path


def capturar_jobs_etl(driver):
    print("[3/4] Capturando tela do Glue Jobs ETL (execução bronze com sucesso)...")
    mudar_tela(driver, "Jobs ETL")
    time.sleep(3)

    # Clica no job bronze
    links = driver.find_elements(By.TAG_NAME, "button")
    for btn in links:
        if btn.text.strip() == "bronze":
            btn.click()
            time.sleep(4)
            break

    screenshot_path = OUTPUT_DIR / "3_print_execucao_job_bronze_sucesso.png"
    driver.save_screenshot(str(screenshot_path))
    print(f"  [+] Salvo em: {screenshot_path}")
    return screenshot_path


def capturar_athena_e_csv(driver):
    print("[4/4] Executando consulta e capturando tela do Athena...")
    mudar_tela(driver, "Editor de consultas")
    time.sleep(4)

    # Localiza o editor de texto SQL do Streamlit
    text_areas = driver.find_elements(By.TAG_NAME, "textarea")
    if text_areas:
        ta = text_areas[0]
        ta.send_keys(Keys.CONTROL + "a")
        ta.send_keys(Keys.BACKSPACE)
        query = (
            "SELECT event_name, year, month, day, count(*) AS total_eventos\n"
            "FROM bronze.eventos\n"
            "GROUP BY event_name, year, month, day\n"
            "ORDER BY total_eventos DESC\n"
            "LIMIT 20;"
        )
        ta.send_keys(query)
        time.sleep(1)

    # Clica no botão Executar
    botoes = driver.find_elements(By.TAG_NAME, "button")
    for b in botoes:
        if b.text.strip() == "Executar":
            b.click()
            print("  [>] Consulta enviada ao Athena, aguardando execução...")
            time.sleep(6)
            break

    screenshot_path = OUTPUT_DIR / "4_print_consulta_athena_bronze.png"
    driver.save_screenshot(str(screenshot_path))
    print(f"  [+] Salvo em: {screenshot_path}")
    return screenshot_path


def exportar_csv_athena():
    """Gera o arquivo CSV da consulta do Athena via DuckDB conectando no S3 emulado."""
    import subprocess
    cmd = [
        "docker", "compose", "exec", "explorador", "python", "-c",
        (
            "from explorador import consultas, particoes, s3\n"
            "con = consultas.conectar(endpoint=s3.ENDPOINT, chave=s3.CHAVE, segredo=s3.SEGREDO, regiao=s3.REGIAO)\n"
            "objetos = s3.listar()\n"
            "datasets = particoes.datasets(o.chave for o in objetos)\n"
            "consultas.criar_views(con, s3.raiz, datasets)\n"
            "df = con.sql('''SELECT event_name, year, month, day, count(*) AS total_eventos "
            "FROM bronze.eventos "
            "GROUP BY event_name, year, month, day "
            "ORDER BY total_eventos DESC "
            "LIMIT 20''').df()\n"
            "df.to_csv('/tmp/consulta_athena_bronze.csv', index=False)\n"
            "print('[+] CSV gerado com sucesso no container')\n"
        )
    ]
    subprocess.run(cmd, cwd=str(ROOT_DIR), check=True)

    # Copia o CSV para a pasta de entregáveis
    cmd_copy = ["docker", "compose", "cp", "explorador:/tmp/consulta_athena_bronze.csv", str(OUTPUT_DIR / "consulta_athena_bronze.csv")]
    subprocess.run(cmd_copy, cwd=str(ROOT_DIR), check=True)
    csv_path = OUTPUT_DIR / "consulta_athena_bronze.csv"
    print(f"  [+] CSV salvo em: {csv_path}")
    return csv_path


def gerar_zip_aula_bronze():
    zip_path = ROOT_DIR.parent / "Aula_Bronze.zip"
    print(f"\n[*] Criando pacote ZIP em: {zip_path}")

    # Copia bronze.py para a pasta de entregáveis
    shutil.copy(ROOT_DIR / "pipeline" / "bronze.py", OUTPUT_DIR / "bronze.py")

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for f in [
            "1_print_bucket_s3_bronze.png",
            "2_print_datacatalog_schema_bronze.png",
            "3_print_execucao_job_bronze_sucesso.png",
            "bronze.py",
            "4_print_consulta_athena_bronze.png",
            "consulta_athena_bronze.csv",
        ]:
            src = OUTPUT_DIR / f
            if src.exists():
                z.write(src, arcname=f)
                print(f"  [+] Adicionado ao ZIP: {f} ({src.stat().st_size} bytes)")

    print(f"[OK] Pacote final pronto: {zip_path}")
    return zip_path


def main():
    driver = iniciar_driver()
    try:
        driver.get("http://localhost:8501")
        time.sleep(5)

        capturar_bucket_s3(driver)
        capturar_data_catalog(driver)
        capturar_jobs_etl(driver)
        capturar_athena_e_csv(driver)
        exportar_csv_athena()
        gerar_zip_aula_bronze()

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
