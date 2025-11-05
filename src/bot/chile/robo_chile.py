import argparse
import os
import re
import shutil
import subprocess
from pathlib import Path
import json
import calendar
from datetime import datetime

import requests
import pandas as pd
import chardet

# Garantir saída em UTF-8 mesmo no Windows/PowerShell
try:
    import sys as _sys
    if hasattr(_sys.stdout, "reconfigure"):
        _sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

CKAN_BASE = "https://datos.gob.cl/api/3/action/package_show"

MONTH_NAMES = {
    1:  ["enero", "jan", "january"],
    2:  ["febrero", "feb", "february"],
    3:  ["marzo", "mar", "march"],
    4:  ["abril", "apr", "april"],
    5:  ["mayo", "may"],
    6:  ["junio", "jun", "june"],
    7:  ["julio", "jul", "july"],
    8:  ["agosto", "aug", "august", "ago"],
    9:  ["septiembre", "setiembre", "sep", "september"],
    10: ["octubre", "oct", "october"],
    11: ["noviembre", "nov", "november"],
    12: ["diciembre", "dec", "dez", "december"],
}

COLUMN_NAMES = [
"NUMENCRIPTADO","TIPO_DOCTO","ADU","FORM","FECVENCI","CODCOMUN","NUM_UNICO_IMPORTADOR","CODPAISCON","DESDIRALM",
"CODCOMRS","ADUCTROL","NUMPLAZO","INDPARCIAL","NUMHOJINS","TOTINSUM","CODALMA","NUM_RS","FEC_RS","ADUA_RS",
"NUMHOJANE","NUM_SEC","PA_ORIG","PA_ADQ","VIA_TRAN","TRANSB","PTO_EMB","PTO_DESEM","TPO_CARGA","ALMACEN",
"FEC_ALMAC","FECRETIRO","NU_REGR","ANO_REG","CODVISBUEN","NUMREGLA","NUMANORES","CODULTVB","PAGO_GRAV",
"FECTRA","FECACEP","GNOM_CIA_T","CODPAISCIA","NUMRUTCIA","DIGVERCIA","NUM_MANIF","NUM_MANIF1","NUM_MANIF2",
"FEC_MANIF","NUM_CONOC","FEC_CONOC","NOMEMISOR","NUMRUTEMI","DIGVEREMI","GREG_IMP","REG_IMP","BCO_COM",
"CODORDIV","FORM_PAGO","NUMDIAS","VALEXFAB","MONEDA","MONGASFOB","CL_COMPRA","TOT_ITEMS","FOB","TOT_HOJAS",
"COD_FLE","FLETE","TOT_BULTOS","COD_SEG","SEGURO","TOT_PESO","CIF","NUM_AUT","FEC_AUT","GBCOCEN","ID_BULTOS",
"TPO_BUL1","CANT_BUL1","TPO_BUL2","CANT_BUL2","TPO_BUL3","CANT_BUL3","TPO_BUL4","CANT_BUL4","TPO_BUL5","CANT_BUL5",
"TPO_BUL6","CANT_BUL6","TPO_BUL7","CANT_BUL7","TPO_BUL8","CANT_BUL8","CTA_OTRO","MON_OTRO","CTA_OTR1","MON_OTR1",
"CTA_OTR2","MON_OTR2","CTA_OTR3","MON_OTR3","CTA_OTR4","MON_OTR4","CTA_OTR5","MON_OTR5","CTA_OTR6","MON_OTR6",
"CTA_OTR7","MON_OTR7","MON_178","MON_191","FEC_501","VAL_601","FEC_502","VAL_602","FEC_503","VAL_603","FEC_504",
"VAL_604","FEC_505","VAL_605","FEC_506","VAL_606","FEC_507","VAL_607","TASA","NCUOTAS","ADU_DI","NUM_DI","FEC_DI",
"MON_699","MON_199","NUMITEM","DNOMBRE","DMARCA","DVARIEDAD","DOTRO1","DOTRO2","ATR-5","ATR-6","SAJU-ITEM",
"AJU-ITEM","CANT-MERC","MERMAS","MEDIDA","PRE-UNIT","ARANC-ALA","NUMCOR","NUMACU","CODOBS1","DESOBS1","CODOBS2",
"DESOBS2","CODOBS3","DESOBS3","CODOBS4","DESOBS4","ARANC-NAC","CIF-ITEM","ADVAL-ALA","ADVAL","VALAD","OTRO1",
"CTA1","SIGVAL1","VAL1","OTRO2","CTA2","SIGVAL2","VAL2","OTRO3","CTA3","SIGVAL3","VAL3","OTRO4","CTA4","SIGVAL4","VAL4"
]

# ---- RAR opcional ----
try:
    import rarfile
    rarfile.UNRAR_TOOL = r"C:\Program Files\WinRAR\UnRAR.exe"
    RAR_OK = True
except Exception:
    RAR_OK = False

def debug_enabled(argv) -> bool:
    return any(a in ("--debug","-d") for a in argv)

def eprint(msg: str, argv):
    if debug_enabled(argv):
        import sys
        print(msg, file=sys.stderr)

def fetch_package(year: int) -> dict:
    slug = f"registro-de-importacion-{year}"
    r = requests.get(CKAN_BASE, params={"id": slug}, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        raise RuntimeError(f"CKAN retornou success=false para {slug}")
    return data["result"]

def month_match(text: str, year: int, month: int) -> bool:
    s = text.lower()
    if str(year) not in s:
        return False
    return any(m in s for m in MONTH_NAMES[month])

def select_month_resources(resources: list, year: int, month: int):
    hits = [r for r in resources if month_match((r.get("name","") + " " + r.get("url","")), year, month)]
    if not hits:
        raise FileNotFoundError(f"Nenhum arquivo encontrado para {month:02d}/{year}.")
    def part_idx(res):
        m = re.search(r'\.part(\d+)\.rar$', res.get("url","") or "", flags=re.I)
        return int(m.group(1)) if m else 0
    hits.sort(key=part_idx)
    return hits

def download(url: str, dst: Path, argv):
    dst.parent.mkdir(parents=True, exist_ok=True)
    eprint(f"Baixando: {url}", argv)
    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with open(dst, "wb") as f:
            shutil.copyfileobj(resp.raw, f)

def extract_rar(first_part: Path, out_dir: Path, argv):
    import shutil as _shutil
    out_dir.mkdir(parents=True, exist_ok=True)
    rar_path = str(first_part)
    eprint(f"Extraindo RAR: {first_part.name}", argv)
    unar = _shutil.which("unar")
    if unar:
        if debug_enabled(argv):
            proc = subprocess.run([unar, "-force-overwrite", "-o", str(out_dir), rar_path],
                                  check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            eprint(proc.stdout, argv)
        else:
            subprocess.run([unar, "-force-overwrite", "-o", str(out_dir), rar_path],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return
    unrar = _shutil.which("unrar") or r"C:\Program Files\WinRAR\UnRAR.exe"
    if Path(unrar).exists():
        if debug_enabled(argv):
            proc = subprocess.run([unrar, "x", "-o+", rar_path, str(out_dir)],
                                  check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            eprint(proc.stdout, argv)
        else:
            subprocess.run([unrar, "x", "-o+", rar_path, str(out_dir)],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return
    if RAR_OK:
        rf = rarfile.RarFile(rar_path); rf.extractall(path=str(out_dir)); rf.close(); return
    seven = _shutil.which("7z") or _shutil.which("7za")
    if seven:
        if debug_enabled(argv):
            proc = subprocess.run([seven, "x", "-y", f"-o{str(out_dir)}", rar_path],
                                  check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            eprint(proc.stdout, argv)
        else:
            subprocess.run([seven, "x", "-y", f"-o{str(out_dir)}", rar_path],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return
    raise RuntimeError("Instale UnRAR/7-Zip para extrair .rar.")

def sniff_text(path: Path):
    raw = path.read_bytes()
    enc = chardet.detect(raw).get("encoding") or "latin-1"
    sample = raw[:20000]
    try:
        s = sample.decode(enc, errors="replace")
    except Exception:
        enc = "latin-1"; s = sample.decode(enc, errors="replace")
    import csv as _csv
    try:
        dialect = _csv.Sniffer().sniff(s, delimiters=";,\t|")
        sep = dialect.delimiter
    except Exception:
        sep = ';' if s.count(';') >= max(s.count('\t'), s.count(',')) else ('\t' if s.count('\t') >= s.count(',') else ',')
    return enc, sep

def find_data_files(folder: Path):
    exts = (".txt", ".csv", ".xlsx", ".xls")
    out = []
    for root, _, files in os.walk(folder):
        for fn in files:
            if fn.lower().endswith(exts):
                out.append(Path(root) / fn)
    return out

# --------- escrita streaming da array de resultados ---------
def write_array_stream(
    data_paths,
    argv,
    tmp_array_path: Path,
    year: int,
    month: int,
    limit: int | None,
    enable_limit: bool
) -> int:
    """
    Escreve a array JSON em tmp_array_path (streaming) e retorna a contagem.
    Se enable_limit=True e limit>0, corta após N registros.
    Injeta country_code='CL', ano_ref=<year>, mes_ref=<month> em cada item.
    """
    use_limit = bool(enable_limit and limit is not None and limit > 0)

    total = 0
    first_item_written = False
    with open(tmp_array_path, "w", encoding="utf-8") as arr:
        arr.write("[\n")
        for p in data_paths:
            eprint(f"Lendo: {p.name}", argv)
            ext = p.suffix.lower()
            try:
                if ext in (".txt", ".csv"):
                    enc, sep = sniff_text(p)
                    for chunk in pd.read_csv(
                        p,
                        encoding=enc,
                        sep=sep,
                        dtype=str,
                        header=None,
                        on_bad_lines="skip",
                        low_memory=False,
                        chunksize=150_000,
                        keep_default_na=False,
                    ):
                        df = chunk
                        if df.shape[1] < len(COLUMN_NAMES):
                            for i in range(len(COLUMN_NAMES) - df.shape[1]):
                                df[f"_pad_{i+1}"] = None
                            df = df.iloc[:, :len(COLUMN_NAMES)]
                        elif df.shape[1] > len(COLUMN_NAMES):
                            df = df.iloc[:, :len(COLUMN_NAMES)]
                        df.columns = COLUMN_NAMES
                        df = df.where(pd.notnull(df), None)

                        for rec in df.to_dict(orient="records"):
                            # limpeza fina
                            for k, v in list(rec.items()):
                                if isinstance(v, float) and pd.isna(v):
                                    rec[k] = None
                                elif isinstance(v, str):
                                    rec[k] = v.strip()

                            # >>> novos campos por item
                            rec["country_code"] = "CL"
                            rec["ano_ref"] = year
                            rec["mes_ref"] = month

                            if first_item_written:
                                arr.write(",\n")
                            arr.write(json.dumps(rec, ensure_ascii=False, allow_nan=False))
                            first_item_written = True
                            total += 1

                            if use_limit and total >= limit:
                                arr.write("\n]")
                                return total

                elif ext in (".xlsx", ".xls"):
                    xdf = pd.read_excel(
                        p,
                        engine="openpyxl",
                        header=None,
                        dtype=str,
                        na_filter=False,
                    )
                    df = xdf
                    if df.shape[1] < len(COLUMN_NAMES):
                        for i in range(len(COLUMN_NAMES) - df.shape[1]):
                            df[f"_pad_{i+1}"] = None
                        df = df.iloc[:, :len(COLUMN_NAMES)]
                    elif df.shape[1] > len(COLUMN_NAMES):
                        df = df.iloc[:, :len(COLUMN_NAMES)]
                    df.columns = COLUMN_NAMES
                    df = df.where(pd.notnull(df), None)

                    for rec in df.to_dict(orient="records"):
                        for k, v in list(rec.items()):
                            if isinstance(v, float) and pd.isna(v):
                                rec[k] = None
                            elif isinstance(v, str):
                                rec[k] = v.strip()

                        # >>> novos campos por item
                        rec["country_code"] = "CL"
                        rec["ano_ref"] = year
                        rec["mes_ref"] = month

                        if first_item_written:
                            arr.write(",\n")
                        arr.write(json.dumps(rec, ensure_ascii=False, allow_nan=False))
                        first_item_written = True
                        total += 1

                        if use_limit and total >= limit:
                            arr.write("\n]")
                            return total
                else:
                    eprint(f"[ignorado] extensão não suportada: {ext}", argv)
            except Exception as e:
                eprint(f"[aviso] falha ao ler {p.name}: {e}", argv)
        arr.write("\n]")
    return total

# --------------- pipeline principal (JSON final) ---------------
def run(year: int, month: int, workdir: Path, argv, limit: int | None, enable_limit: bool):
    pkg = fetch_package(year)
    res = select_month_resources(pkg.get("resources", []), year, month)

    month_dir = workdir / f"{year}-{month:02d}"
    tmp = month_dir / "_tmp"
    tmp.mkdir(parents=True, exist_ok=True)

    data_paths = []
    first_rar = None

    for r in res:
        url = r.get("url") or r.get("download_url") or r.get("path") or ""
        if not url:
            continue
        dst = tmp / Path(url).name
        download(url, dst, argv)
        if dst.suffix.lower() == ".rar":
            if first_rar is None or re.search(r'\.part0*1\.rar$', dst.name, re.I):
                first_rar = dst
        elif dst.suffix.lower() in (".txt", ".csv", ".xlsx", ".xls"):
            data_paths.append(dst)

    if first_rar is not None:
        extracted = tmp / "extracted"
        extract_rar(first_rar, extracted, argv)
        data_paths.extend(find_data_files(extracted))

    if not data_paths:
        try:
            shutil.rmtree(month_dir, ignore_errors=True)
            try: os.rmdir(workdir)
            except OSError: pass
        finally:
            pass
        raise FileNotFoundError("Nenhum arquivo .txt/.csv/.xlsx encontrado após o download.")

    data_paths.sort(key=lambda p: p.stat().st_size, reverse=True)

    # 1) escreve apenas a array (streaming) em arquivo temporário
    tmp_array_path = tmp / "resultados_array.json"
    total = write_array_stream(
        data_paths, argv, tmp_array_path,
        year=year, month=month,
        limit=limit, enable_limit=enable_limit
    )

    first_day = f"01/{month:02d}/{year}"
    last_day_num = calendar.monthrange(year, month)[1]
    last_day = f"{last_day_num:02d}/{month:02d}/{year}"

    lim_tag = f" (limitado a {limit})" if (enable_limit and limit is not None and limit > 0) else ""
    descricao = f"Foram encontradas {total} importações no período de {first_day} a {last_day}{lim_tag}"
    
    # Emite o JSON final diretamente no stdout (sem gravar arquivo)
    with open(tmp_array_path, "r", encoding="utf-8") as arr:
        print('{')
        print('  "descricao": ' + json.dumps(descricao, ensure_ascii=False) + ',')
        print('  "total": ' + str(total) + ',')
        print('  "resultados": ', end='')
        shutil.copyfileobj(arr, _sys.stdout)
        print("\n}")
        
    # ===== LIMPEZA TOTAL DO WORKDIR =====
    try:
        shutil.rmtree(month_dir, ignore_errors=True)
    except Exception:
        pass
    try:
        os.rmdir(workdir)  # remove se vazio
    except OSError:
        pass

    # Não imprime resumo extra no stdout para não poluir o JSON

# ---------------------- CLI -----------------------
if __name__ == "__main__":
    import sys
    ap = argparse.ArgumentParser(
        description="Chile (CKAN) -> JSON bruto streaming, country_code=CL (limpa workdir ao final)."
    )
    ap.add_argument("year", type=int, help="Ano (ex.: 2025)")
    ap.add_argument("month", type=int, help="Mês 1..12 (ex.: 1)")
    ap.add_argument("--workdir", type=str, default="./data_work",
                    help="Diretório de trabalho/temporários (será removido ao final)")
    ap.add_argument("--debug", "-d", action="store_true", help="Mostra logs de progresso (stderr)")

    # 🔒 Limite OPCIONAL com proteção
    ap.add_argument("--limit", type=int, default=None,
                    help="(opcional) Corta após N registros — ignorado sem --enable-limit")
    ap.add_argument("--enable-limit", action="store_true",
                    help="(segurança) Só aplica --limit se esta flag também for passada")

    args = ap.parse_args()

    if not (1 <= args.month <= 12):
        ap.error("month deve ser 1..12")

    run(
        year=args.year,
        month=args.month,
        workdir=Path(args.workdir),
        argv=sys.argv,
        limit=args.limit,
        enable_limit=args.enable_limit
    )
