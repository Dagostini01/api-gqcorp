# robo_comex.py
import os, sys, json, time, urllib.parse
from datetime import datetime
from typing import List, Dict, Any, Tuple
import requests

COMEX_POST_URL = "https://api-comexstat.mdic.gov.br/general?language=pt"
COMEX_LEGACY_BASE = "http://api.comexstat.mdic.gov.br/general?filter="

# Garantir saída em UTF-8 mesmo no Windows/PowerShell
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

def debug_enabled() -> bool:
    return any(a in ("--debug","-d") for a in sys.argv[1:])

def eprint(*args, **kw):
    if debug_enabled():
        print(*args, file=sys.stderr, **kw)

def normalize_period_to_yyyy_mm(p: str) -> str:
    p = p.strip()
    for fmt in ("%Y-%m-%d", "%Y-%m"):
        try:
            return datetime.strptime(p, fmt).strftime("%Y-%m")
        except Exception:
            pass
    if len(p) == 6 and p.isdigit():
        return f"{p[:4]}-{p[4:]}"
    return p

def pad8(ncm: str) -> str:
    s = "".join(ch for ch in str(ncm).strip() if ch.isdigit())
    return s.zfill(8)

def to_float_or_none(v):
    if v is None: return None
    s = str(v).strip().replace(" ", "").replace(",", ".")
    try: return float(s)
    except: return None

def build_fec_numeracao(year, monthNumber) -> str:
    y = str(year).zfill(4); m = str(monthNumber).zfill(2)
    return f"01/{m}/{y}"

def transformar_registro(it: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "country_code": "BR",
        "importador": "COMEXSTAT",
        "declaracao": "COMEXSTAT",
        "serie": "BR",
        "partida": (str(it.get("coNcm")).strip() if it.get("coNcm") is not None else None),
        "fecNumeracao": build_fec_numeracao(it.get("year"), it.get("monthNumber")),
        "paisOrig": it.get("country"),
        "state": it.get("state"),
        "descComer": it.get("ncm"),
        "fobUsd": to_float_or_none(it.get("metricFOB")),
        "fleteUsd": to_float_or_none(it.get("metricFreight")),
        "seguro": to_float_or_none(it.get("metricInsurance")),
        "cif": to_float_or_none(it.get("metricCIF")),
        "pesoNeto": to_float_or_none(it.get("metricKG")),
    }

def tls_verify():
    insecure = os.environ.get("COMEX_INSECURE", "0") == "1"
    ca_bundle = os.environ.get("COMEX_CA_BUNDLE")
    if insecure:
        return False
    if ca_bundle:
        return ca_bundle
    return True

def get_session() -> requests.Session:
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=0)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

def post_general(payload: Dict[str, Any]) -> Tuple[List[Dict[str,Any]], int, str]:
    verify = tls_verify()
    sess = get_session()
    tries = 3
    last_status = 0
    last_text = ""
    for i in range(tries):
        try:
            r = sess.post(
                COMEX_POST_URL,
                json=payload,
                timeout=60,
                verify=verify,
                headers={"Content-Type": "application/json"},
            )
            last_status = r.status_code
            last_text = r.text[:4000]
            if r.status_code == 429:
                eprint(f"[POST try {i+1}/{tries}] rate limited 429; dormindo 2s")
                time.sleep(2.0); continue
            r.raise_for_status()
            data = r.json()
            lst = data.get("data", {}).get("list", [])
            if isinstance(lst, list):
                return lst, last_status, last_text
            return [], last_status, last_text
        except requests.exceptions.SSLError as e:
            eprint(f"[TLS] SSLError no POST: {e}")
            time.sleep(1.0)
        except Exception as e:
            eprint(f"[POST try {i+1}/{tries}] err={type(e).__name__} status={last_status}")
            time.sleep(1.0)
    return [], last_status, last_text

def montar_payload_post(ncm_values, p_from, p_to, details=None, metrics=None):
    return {
        "flow": "import",
        "monthDetail": True,
        "period": {"from": p_from, "to": p_to},
        "filters": [{"filter": "ncm", "values": ncm_values}],
        "details": details or ["ncm"],
        "metrics": metrics or ["metricFOB","metricFreight","metricInsurance","metricCIF","metricKG","metricStatistic"],
    }

# -------- legado (GET ?filter=) --------
def build_legacy_filter_json(ncms: List[str], y_from: str, y_to: str, m_from: str, m_to: str, month_detail: bool) -> Dict[str, Any]:
    detail_ids = []
    for d in ("country","state","ncm"):
        detail_ids.append({"id": {
            "country":"noPaispt",
            "state":"noUf",
            "ncm":"noNcmpt"
        }[d], "text": ""})

    legacy = {
        "yearStart": y_from, "yearEnd": y_to,
        "monthStart": m_from, "monthEnd": m_to,
        "typeForm": 2,
        "typeOrder": 1,
        "filterList": [{"id":"noNcmpt"}],
        "filterArray": [{"item": [pad8(x) for x in ncms], "idInput":"noNcmpt"}],
        "detailDatabase": detail_ids,
        "monthDetail": bool(month_detail),
        "metricFOB": True, "metricKG": True, "metricStatistic": False,
        "formQueue":"general", "langDefault":"pt",
    }
    return legacy

def get_legacy(ncms_raw: List[str], p_from: str, p_to: str) -> List[Dict[str, Any]]:
    y_from, m_from = p_from.split("-")
    y_to, m_to     = p_to.split("-")
    filt = build_legacy_filter_json(ncms_raw, y_from, y_to, m_from, m_to, True)
    filt_str = json.dumps(filt, ensure_ascii=False)
    url = COMEX_LEGACY_BASE + urllib.parse.quote(filt_str, safe="")
    verify = tls_verify()
    sess = get_session()
    tries = 3
    last_status = 0
    last_text = ""
    for i in range(tries):
        try:
            r = sess.get(url, timeout=60, verify=verify)
            last_status = r.status_code
            last_text = r.text[:4000]
            if r.status_code == 429:
                eprint(f"[LEGACY try {i+1}/{tries}] 429; dormindo 2s")
                time.sleep(2.0); continue
            r.raise_for_status()
            data = r.json()
            lst = None
            if isinstance(data, dict):
                d = data.get("data")
                if isinstance(d, list) and d and isinstance(d[0], list):
                    lst = d[0]
            if not isinstance(lst, list):
                lst = data.get("data", {}).get("list", [])
            return lst if isinstance(lst, list) else []
        except Exception as e:
            eprint(f"[LEGACY try {i+1}/{tries}] err={type(e).__name__} status={last_status}")
            time.sleep(1.0)
    return []

# >>>>>>> CORRIGIDO: agora recebe 'details' e desempacota 3 valores
def tentar_variantes_post(ncms_raw: List[str], p_from: str, p_to: str, details: List[str]) -> List[Dict[str,Any]]:
    # 1) NCM como string 8 dígitos
    vals_str = [pad8(x) for x in ncms_raw]
    lst, st, _ = post_general(montar_payload_post(vals_str, p_from, p_to, details))
    eprint(f"[POST A] ncm=str8 -> status={st} rows={len(lst)}")
    if lst: return lst

    # 2) subHeading (HS6)
    hs6 = list({pad8(x)[:6] for x in ncms_raw})
    payload_hs6 = montar_payload_post(hs6, p_from, p_to, details=["subHeading","ncm"])
    lst, st, _ = post_general(payload_hs6)
    eprint(f"[POST B] subHeading -> status={st} rows={len(lst)}")
    if lst: return lst

    # 3) heading (SH4)
    hs4 = list({pad8(x)[:4] for x in ncms_raw})
    payload_hs4 = montar_payload_post(hs4, p_from, p_to, details=["heading","ncm"])
    lst, st, _ = post_general(payload_hs4)
    eprint(f"[POST C] heading -> status={st} rows={len(lst)}")
    return lst

def ping_years():
    verify = tls_verify()
    try:
        r = requests.get("https://api-comexstat.mdic.gov.br/general/dates/years", timeout=20, verify=verify)
        if r.ok:
            return r.json()
    except Exception as e:
        eprint(f"[PING] falhou: {type(e).__name__}")
    return None

def main():
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    if len(args) < 3:
        out = {"descricao":"Argumentos insuficientes","total":0,"resultados":[]}
        print(json.dumps(out, ensure_ascii=False)); return

    ncm_raw = args[0]
    p_from_raw, p_to_raw = args[1], args[2]
    details = ["ncm", "country", "state"]  # detalhe mínimo primeiro

    ncms_raw = [s.strip() for s in ncm_raw.split(",") if s.strip()]
    p_from = normalize_period_to_yyyy_mm(p_from_raw)
    p_to   = normalize_period_to_yyyy_mm(p_to_raw)

    if debug_enabled():
        y = ping_years()
        if y: eprint("[PING years]", y)

    # 1) tenta POST
    bruta = tentar_variantes_post(ncms_raw, p_from, p_to, details)

    # 2) se nada, legado GET
    if not bruta:
        eprint("[FALLBACK] tentando API legada via GET ?filter=")
        bruta = get_legacy(ncms_raw, p_from, p_to)

    resultados = [transformar_registro(it) for it in bruta] if bruta else []

    total = len(resultados)
    ncm_legivel = ",".join(ncms_raw)
    descricao = f"Foram encontradas {total} linhas no ComexStat para o(s) NCM(s) {ncm_legivel} no período de {p_from} a {p_to}."

    saida = {"descricao": descricao, "total": total, "resultados": resultados}
    # Apenas imprime o JSON no stdout; não grava em arquivo nem cria diretório
    print(json.dumps(saida, ensure_ascii=False))

if __name__ == "__main__":
    main()
