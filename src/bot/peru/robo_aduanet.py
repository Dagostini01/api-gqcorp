import os
import sys
import json
import time
from datetime import datetime
from typing import List, Dict, Optional

# Forçar UTF-8 na saída padrão (evita problemas em Windows/PowerShell)
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

URL = "http://www.aduanet.gob.pe/cl-ad-consdepa/ConsImpoIAServlet?accion=cargarConsulta&tipoConsulta=14"

CAMPOS = [
    "declaracao", "importador", "fecNumeracao", "agencia", "series", "fobUsd",
    "fleteUsd", "seguro", "armazen", "canal", "pesoNeto", "nroBultos", "serie",
    "partida", "descComer", "descPresent", "descMatConst", "descUso",
    "descOutros", "quantidade", "unid", "paisAdq", "paisOrig", "pesoNeto2", "fob",
    "flete", "seguro2", "adv", "igv", "isc", "ipm", "derEsp", "derAnt",
    "ipmAdic", "commod", "state"
]

TIPO_MAPPING = {
    "importacao": "IMPORTADOR",
    "importador": "IMPORTADOR",
    "IMPORTADOR": "IMPORTADOR",
    "partida": "PARTIDA ARANCELARIA",
    "PARTIDA": "PARTIDA ARANCELARIA",
    "PARTIDA ARANCELARIA": "PARTIDA ARANCELARIA",
}

def ymd_to_dmy(s: str) -> str:
    try:
        d = datetime.strptime(s, "%Y-%m-%d")
        return d.strftime("%d/%m/%Y")
    except Exception:
        return s

def criar_driver(headless: bool = True) -> webdriver.Chrome:
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def escolher_tabela_certa(driver: webdriver.Chrome) -> Optional[object]:
    tabelas = driver.find_elements(By.TAG_NAME, "table")
    melhor = None
    melhor_score = -1
    for t in tabelas:
        txt = (t.text or "").lower()
        score = sum(ch in txt for ch in ["declar", "import", "partida", "fob"])
        comprimento = len(txt)
        score_total = score * 10000 + comprimento
        if score_total > melhor_score:
            melhor = t
            melhor_score = score_total
    return melhor

def extrair_tabela(driver: webdriver.Chrome) -> List[Dict]:
    registros = []
    tabela = escolher_tabela_certa(driver)
    if not tabela:
        return registros
    linhas = tabela.find_elements(By.TAG_NAME, "tr")
    for tr in linhas:
        tds = tr.find_elements(By.TAG_NAME, "td")
        if len(tds) >= len(CAMPOS) - 1:
            registro = {}
            for i in range(len(CAMPOS) - 1):
                registro[CAMPOS[i]] = (tds[i].text or "").strip()
            registro["state"] = "PE"
            registro["country_code"] = "PE"
            registros.append(registro)
    return registros

def paginar_e_coletar(driver: webdriver.Chrome, max_segundos: int = 300) -> List[Dict]:
    resultados: List[Dict] = []
    inicio = time.time()
    while True:
        if time.time() - inicio > max_segundos:
            break
        try:
            WebDriverWait(driver, 25).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
        except Exception:
            break
        pagina = extrair_tabela(driver)
        if pagina:
            resultados.extend(pagina)
        else:
            body_txt = (driver.page_source or "").lower()
            if "no existen" in body_txt and "registros" in body_txt:
                break
        try:
            botao = driver.find_element(By.XPATH, "//a[contains(., 'Siguiente')]")
            if botao.is_enabled():
                botao.click()
                WebDriverWait(driver, 10).until(EC.staleness_of(botao))
            else:
                break
        except Exception:
            break
    return resultados

def main():
    if len(sys.argv) < 5:
        print(json.dumps({"descricao": "Nenhum argumento fornecido", "total": 0, "resultados": []}, ensure_ascii=False))
        return

    DATA_INICIO_RAW = sys.argv[1]
    DATA_FIM_RAW = sys.argv[2]
    TIPO = sys.argv[3]
    DOCUMENTO = sys.argv[4]

    DATA_INICIO = ymd_to_dmy(DATA_INICIO_RAW)
    DATA_FIM = ymd_to_dmy(DATA_FIM_RAW)
    tipo_correto = TIPO_MAPPING.get(TIPO, TIPO)

    driver = None
    dados_totais: List[Dict] = []
    try:
        driver = criar_driver(headless=True)
        driver.get(URL)

        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.NAME, "fec_inicio"))
        ).send_keys(DATA_INICIO)
        driver.find_element(By.NAME, "fec_fin").send_keys(DATA_FIM)
        Select(driver.find_element(By.NAME, "tipo")).select_by_visible_text(tipo_correto)
        driver.find_element(By.NAME, "documento").send_keys(DOCUMENTO)
        driver.find_element(By.NAME, "btnConsultar").click()

        dados_totais = paginar_e_coletar(driver, max_segundos=300)

    except Exception:
        dados_totais = []
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    total = len(dados_totais)
    descricao = (
        f"Foram encontradas {total} importações no período de {DATA_INICIO} a {DATA_FIM} "
        f"para o CNPJ {DOCUMENTO}."
    )

    resultado_final = {
        "descricao": descricao,
        "total": total,
        "resultados": dados_totais
    }

    print(json.dumps(resultado_final, ensure_ascii=False))

if __name__ == "__main__":
    main()
