import re
import time
import math
import uuid
import threading
import unicodedata
from io import BytesIO
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import requests
from openpyxl import load_workbook
from fastapi import FastAPI, UploadFile, File, HTTPException, Response, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse

# =========================================================
# CONFIG
# =========================================================

BASE_DIR = Path(__file__).resolve().parent
APP_NAME = "GeoPadroniza"
APP_UA = "GeoPadroniza/SAFE-5.0 (+local-app)"
NOMINATIM_DELAY_SECONDS = 1.1
REQUEST_TIMEOUT = 25

MODES = {"ultra", "rapido", "completo"}

app = FastAPI(title=APP_NAME, version="5.0.0-inteligente")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

JOBS: Dict[str, Dict[str, Any]] = {}

# =========================================================
# REGRAS
# =========================================================

PREPOSICOES = {"de", "da", "do", "das", "dos", "e"}

TIPOS_LOGRADOURO = [
    (r"^(avenida|av\.?)\b", "Av"),
    (r"^(rua|r\.?)\b", "R"),
    (r"^(estrada|estr\.?)\b", "Estr"),
    (r"^(travessa|tv\.?)\b", "Tv"),
    (r"^(pra[çc]a|p[çc]a\.?)\b", "Pça"),
    (r"^(largo|lgo\.?)\b", "Lgo"),
    (r"^(rodovia|rod\.?)\b", "Rod"),
    (r"^(alameda|al\.?)\b", "Al"),
]

CORRECOES_RUA = {
    r"\bd[\. ]?em[ií]lio winther\b": "Dr. Emílio Winther",
    r"\bd[\. ]?cesar costa\b": "Dr. César Costa",
    r"\bd[\. ]?cesacosta\b": "Dr. César Costa",
    r"\bd[\. ]?urbano figueira\b": "Dr. Urbano Figueira",
    r"\bjo[aã]o paulo de oliveira gama\b": "João Paulo de Oliveira Gama",
    r"\bvisconde do rio branco\b": "Visconde do Rio Branco",
    r"\banizio ortiz monteiro\b": "Anízio Ortiz Monteiro",
    r"\bitalia\b": "Itália",
    r"\bnove de julho\b": "Nove de Julho",
    r"\bantonio dias oliveira\b": "Antônio Dias Oliveira",
    r"\bpadre antonio diogo feij[oó]\b": "Padre Antônio Diogo Feijó",
}

TOKENS_RUINS = {
    "", "-", "--", "---", "n/a", "na", "null", "none",
    "sem", "nao informado", "não informado", "nao", "não",
    "teste", "x", "xx", "xxx", "0"
}

SHEET_HINTS = {"cliente", "clientes", "cadastro", "endere", "contato", "base", "dados"}

OUTPUT_HEADERS = [
    "geo_modo",
    "geo_colunas_fonte",
    "geo_tipo_sugerido",
    "geo_rua_sugerida",
    "geo_numero_sugerido",
    "geo_bairro_sugerido",
    "geo_cidade_sugerida",
    "geo_uf_sugerida",
    "geo_cep_sugerido",
    "geo_status",
    "geo_confianca",
    "geo_fonte",
    "geo_revisar_motivos",
    "geo_endereco_consultado",
    "geo_display_name",
    "geo_lat",
    "geo_lng",
    "geo_obs",
]

UF_BR = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
    "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
    "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
}

HEADER_ALIASES = {
    "tipo": [
        "tipo logradouro", "tipo de logradouro", "tp logradouro", "tipo"
    ],
    "rua": [
        "logradouro", "rua", "endereco", "endereço"
    ],
    "numero": [
        "numero", "número", "num", "n°", "nº"
    ],
    "bairro": [
        "bairro"
    ],
    "cidade": [
        "cidade", "municipio", "município"
    ],
    "uf": [
        "uf", "estado"
    ],
    "cep": [
        "cep", "codigo postal", "código postal"
    ],
    "complemento": [
        "complemento", "compl"
    ],
}

HEADER_SUFFIX_TOKENS = {
    "cliente", "principal", "entrega", "cobranca", "cobrança",
    "residencial", "comercial", "cadastro", "correspondencia",
    "correspondência", "1", "2", "3"
}

TIPO_LOGRADOURO_TOKENS = {
    "avenida", "av", "rua", "r", "estrada", "estr", "travessa", "tv",
    "praca", "praça", "pca", "pça", "largo", "lgo", "rodovia", "rod",
    "alameda", "al"
}

LOGRADOURO_HINTS = {
    "avenida", "av", "rua", "r", "estrada", "estr", "travessa", "tv",
    "praca", "praça", "pca", "pça", "largo", "lgo", "rodovia", "rod",
    "alameda", "al", "doutor", "dr", "professor", "prof", "padre",
    "visconde", "barao", "barão", "coronel", "cel"
}

BAIRRO_HINTS = {
    "centro", "jardim", "jd", "parque", "vila", "residencial",
    "chacara", "chácara", "bairro", "condominio", "condomínio",
    "loteamento", "distrito"
}

COMPLEMENTO_HINTS = {
    "ap", "apto", "apartamento", "bl", "bloco", "fundos", "frente",
    "sala", "sl", "casa", "sobrado", "quadra", "qd", "lote", "lt",
    "andar", "loja", "box", "km"
}

# =========================================================
# UTIL
# =========================================================

def limpar_texto(valor):
    if valor is None:
        return ""
    if isinstance(valor, float) and math.isnan(valor):
        return ""
    texto = str(valor).strip()
    texto = re.sub(r"\s+", " ", texto)
    return texto

def remover_acentos(texto):
    texto = limpar_texto(texto)
    return "".join(
        c for c in unicodedata.normalize("NFD", texto)
        if unicodedata.category(c) != "Mn"
    )

def normalizar_chave(texto):
    texto = remover_acentos(texto).lower()
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()

def tokenizar_normalizado(texto: str) -> List[str]:
    texto = normalizar_chave(texto)
    return [t for t in re.split(r"[^a-z0-9]+", texto) if t]

def smart_title(texto):
    texto = limpar_texto(texto)
    if not texto:
        return ""
    partes = texto.lower().split(" ")
    out = []
    for i, p in enumerate(partes):
        if i > 0 and p in PREPOSICOES:
            out.append(p)
        else:
            out.append(p[:1].upper() + p[1:])
    return " ".join(out)

def so_digitos(texto):
    return re.sub(r"\D", "", limpar_texto(texto))

def formatar_cep(cep):
    c = so_digitos(cep)
    if len(c) == 8:
        return f"{c[:5]}-{c[5:]}"
    return ""

def uf_valida(uf):
    u = limpar_texto(uf).upper()
    return u in UF_BR

def numero_valido(numero):
    n = normalizar_chave(numero)
    if not n:
        return False
    if n in {"s/n", "sn"}:
        return True
    return bool(re.search(r"\d", n))

def rua_suspeita(rua):
    r = normalizar_chave(rua)
    if not r or r in TOKENS_RUINS:
        return True
    if len(r) < 4:
        return True
    if sum(ch.isdigit() for ch in r) > len(r) * 0.4:
        return True
    return False

def bairro_suspeito(bairro):
    b = normalizar_chave(bairro)
    return not b or b in TOKENS_RUINS or len(b) < 3

def cidade_suspeita(cidade):
    c = normalizar_chave(cidade)
    return not c or c in TOKENS_RUINS or len(c) < 3

def cep_valido(cep):
    return len(so_digitos(cep)) == 8

def render_tipo_logradouro(texto):
    t = limpar_texto(texto)
    if not t:
        return ""
    tn = normalizar_chave(t)
    for padrao, novo in TIPOS_LOGRADOURO:
        if re.match(padrao, tn, flags=re.IGNORECASE):
            return novo
    return smart_title(t)

def corrigir_rua(rua):
    r = limpar_texto(rua)
    if not r:
        return ""
    for padrao, correcao in CORRECOES_RUA.items():
        r = re.sub(padrao, correcao, r, flags=re.IGNORECASE)
    r = smart_title(r)
    r = re.sub(r"\bDr\b\.?", "Dr.", r)
    r = re.sub(r"\bPca\b", "Pça", r)
    return r

def separar_tipo_nome_logradouro(logradouro):
    txt = limpar_texto(logradouro)
    if not txt:
        return "", ""
    txt_norm = normalizar_chave(txt)
    for padrao, tipo in TIPOS_LOGRADOURO:
        if re.match(padrao, txt_norm, flags=re.IGNORECASE):
            partes = txt.split(" ", 1)
            nome = partes[1] if len(partes) > 1 else ""
            return tipo, smart_title(nome)
    return "", smart_title(txt)

def montar_logradouro(tipo, rua):
    tipo = limpar_texto(tipo)
    rua = limpar_texto(rua)
    if tipo and rua:
        rua_norm = normalizar_chave(rua)
        tipo_norm = normalizar_chave(tipo)
        if rua_norm.startswith(tipo_norm + " "):
            return rua
        return f"{tipo} {rua}".strip()
    return rua or tipo

def montar_endereco_consulta(campos):
    via = montar_logradouro(campos.get("tipo", ""), campos.get("rua", ""))
    partes = [
        via,
        campos.get("numero", ""),
        campos.get("complemento", ""),
        campos.get("bairro", ""),
        campos.get("cidade", ""),
        campos.get("uf", ""),
        campos.get("cep", ""),
        "Brasil",
    ]
    partes = [limpar_texto(x) for x in partes if limpar_texto(x)]
    return ", ".join(partes)

def similaridade(a, b):
    a = normalizar_chave(a)
    b = normalizar_chave(b)
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    common = len(set(a.split()) & set(b.split()))
    return common / max(len(set(a.split()) | set(b.split())), 1)

def sheet_parece_endereco_por_nome(sheet_title: str) -> bool:
    st = normalizar_chave(sheet_title)
    return any(h in st for h in SHEET_HINTS)

# =========================================================
# HEURÍSTICAS DE DETECÇÃO INTELIGENTE
# =========================================================

def nome_coluna_combina_alias(nome_coluna: str, alias: str) -> bool:
    nome = normalizar_chave(nome_coluna)
    alias = normalizar_chave(alias)

    if not nome or not alias:
        return False

    if nome == alias:
        return True

    if nome.startswith(alias + " "):
        resto = nome[len(alias):].strip()
        resto_tokens = set(tokenizar_normalizado(resto))
        if resto_tokens and resto_tokens.issubset(HEADER_SUFFIX_TOKENS):
            return True

    if nome.endswith(" " + alias):
        prefixo = nome[: -len(alias)].strip()
        prefixo_tokens = set(tokenizar_normalizado(prefixo))
        if prefixo_tokens and prefixo_tokens.issubset({"campo", "dados", "info", "informacao", "informação"}):
            return True

    return False

def header_map(ws):
    cols = {}
    for c in range(1, ws.max_column + 1):
        cols[c] = normalizar_chave(ws.cell(row=1, column=c).value)
    return cols

def achar_coluna_por_header(headers: Dict[int, str], aliases: List[str]):
    aliases_n = [normalizar_chave(a) for a in aliases]

    for alias in aliases_n:
        for idx, nome in headers.items():
            if nome == alias:
                return idx

    for alias in aliases_n:
        for idx, nome in headers.items():
            if nome_coluna_combina_alias(nome, alias):
                return idx

    return None

def detectar_header_na_primeira_linha(ws):
    headers = header_map(ws)

    cols = {
        "tipo": achar_coluna_por_header(headers, HEADER_ALIASES["tipo"]),
        "rua": achar_coluna_por_header(headers, HEADER_ALIASES["rua"]),
        "numero": achar_coluna_por_header(headers, HEADER_ALIASES["numero"]),
        "bairro": achar_coluna_por_header(headers, HEADER_ALIASES["bairro"]),
        "cidade": achar_coluna_por_header(headers, HEADER_ALIASES["cidade"]),
        "uf": achar_coluna_por_header(headers, HEADER_ALIASES["uf"]),
        "cep": achar_coluna_por_header(headers, HEADER_ALIASES["cep"]),
        "complemento": achar_coluna_por_header(headers, HEADER_ALIASES["complemento"]),
    }

    hits = sum(1 for v in cols.values() if v)
    tem_campo_chave = bool(cols["rua"] or cols["cep"] or cols["cidade"] or cols["bairro"])

    if hits >= 2 and tem_campo_chave:
        return {
            "source": "header_linha_1",
            "start_row": 2,
            "cols": cols,
        }

    return None

def amostras_coluna(ws, col_idx: int, start_row: int = 1, max_rows: int = 30) -> List[str]:
    valores = []
    fim = min(ws.max_row, start_row + max_rows - 1)
    for r in range(start_row, fim + 1):
        v = limpar_texto(ws.cell(row=r, column=col_idx).value)
        if v:
            valores.append(v)
    return valores

def parece_cep_valor(valor: str) -> bool:
    return len(so_digitos(valor)) == 8

def parece_uf_valor(valor: str) -> bool:
    return limpar_texto(valor).upper() in UF_BR

def parece_numero_valor(valor: str) -> bool:
    v = normalizar_chave(valor)
    if not v:
        return False
    if v in {"s/n", "sn"}:
        return True
    if parece_cep_valor(v):
        return False
    return bool(re.fullmatch(r"\d{1,6}[a-z]?", v))

def parece_tipo_logradouro_valor(valor: str) -> bool:
    v = normalizar_chave(valor).replace(".", "")
    return v in TIPO_LOGRADOURO_TOKENS

def parece_complemento_valor(valor: str) -> bool:
    toks = set(tokenizar_normalizado(valor))
    if not toks:
        return False
    if toks & COMPLEMENTO_HINTS:
        return True
    v = normalizar_chave(valor)
    return bool(re.search(r"\b(apto|apartamento|bloco|sala|fundos|quadra|lote|andar|loja|km)\b", v))

def parece_texto_localidade(valor: str) -> bool:
    v = limpar_texto(valor)
    if not v:
        return False
    if parece_cep_valor(v) or parece_uf_valor(v) or parece_numero_valor(v):
        return False
    vn = normalizar_chave(v)
    if vn in TOKENS_RUINS:
        return False
    if len(vn) < 3:
        return False
    if sum(ch.isdigit() for ch in vn) > 1:
        return False
    return True

def parece_bairro_valor(valor: str) -> bool:
    if not parece_texto_localidade(valor):
        return False
    toks = set(tokenizar_normalizado(valor))
    if toks & BAIRRO_HINTS:
        return True
    return 1 <= len(toks) <= 5

def parece_cidade_valor(valor: str) -> bool:
    if not parece_texto_localidade(valor):
        return False
    toks = set(tokenizar_normalizado(valor))
    if toks & BAIRRO_HINTS:
        return False
    return 1 <= len(toks) <= 4

def parece_nome_rua_sem_tipo(valor: str) -> bool:
    if not parece_texto_localidade(valor):
        return False
    toks = tokenizar_normalizado(valor)
    if len(toks) < 2:
        return False
    if set(toks) & BAIRRO_HINTS:
        return False
    if set(toks) & COMPLEMENTO_HINTS:
        return False
    return True

def parece_logradouro_valor(valor: str) -> bool:
    if not valor:
        return False
    v = normalizar_chave(valor)
    if v in TOKENS_RUINS:
        return False
    if parece_cep_valor(v) or parece_uf_valor(v):
        return False
    if re.match(
        r"^(avenida|av\.?|rua|r\.?|estrada|estr\.?|travessa|tv\.?|pra[çc]a|p[çc]a\.?|largo|lgo\.?|rodovia|rod\.?|alameda|al\.?|doutor|dr\.?|professor|prof\.?|padre|visconde|coronel|cel\.?)\b",
        v,
        flags=re.IGNORECASE,
    ):
        return True

    toks = set(tokenizar_normalizado(v))
    if toks & LOGRADOURO_HINTS:
        return True

    return False

def pontuar_coluna_por_amostras(ws, col_idx: int) -> Dict[str, float]:
    valores = amostras_coluna(ws, col_idx, start_row=1, max_rows=30)
    scores = {
        "tipo": 0.0,
        "rua": 0.0,
        "numero": 0.0,
        "bairro": 0.0,
        "cidade": 0.0,
        "uf": 0.0,
        "cep": 0.0,
        "complemento": 0.0,
        "_filled": float(len(valores)),
    }

    if not valores:
        return scores

    for v in valores:
        if parece_cep_valor(v):
            scores["cep"] += 4.5
            continue

        if parece_uf_valor(v):
            scores["uf"] += 4.5
            continue

        if parece_tipo_logradouro_valor(v):
            scores["tipo"] += 4.0

        if parece_numero_valor(v):
            scores["numero"] += 3.2

        if parece_complemento_valor(v):
            scores["complemento"] += 3.0

        if parece_logradouro_valor(v):
            scores["rua"] += 4.2

        if parece_texto_localidade(v):
            scores["bairro"] += 1.0
            scores["cidade"] += 1.0

            if parece_bairro_valor(v):
                scores["bairro"] += 1.7

            if parece_cidade_valor(v):
                scores["cidade"] += 1.7

            if parece_nome_rua_sem_tipo(v):
                scores["rua"] += 0.8

    # Bônus posicional: ajuda especialmente em J/K/N/O/R,
    # sem obrigar depender disso.
    if col_idx == 10:   # J
        scores["tipo"] += 2.0
    if col_idx == 11:   # K
        scores["rua"] += 3.0
    if col_idx == 12:   # L
        scores["numero"] += 1.0
    if col_idx == 14:   # N
        scores["bairro"] += 2.5
    if col_idx == 15:   # O
        scores["cidade"] += 2.5
    if col_idx == 16:   # P
        scores["uf"] += 1.5
    if col_idx == 18:   # R
        scores["cep"] += 3.0

    # Pequeno bônus se o nome da aba já sugerir endereço
    if sheet_parece_endereco_por_nome(ws.title):
        scores["rua"] += 0.2
        scores["bairro"] += 0.2
        scores["cidade"] += 0.2
        scores["cep"] += 0.2

    return scores

def escolher_melhor_coluna(score_map: Dict[int, Dict[str, float]], field: str, used: set, min_score: float):
    melhor_col = None
    melhor_score = min_score

    for col_idx, scores in score_map.items():
        if col_idx in used:
            continue
        score = scores.get(field, 0.0)
        if score > melhor_score:
            melhor_score = score
            melhor_col = col_idx

    return melhor_col, melhor_score

def mapeamento_tem_sinais(cols: Dict[str, Optional[int]]) -> bool:
    principais = sum(
        1 for k in ["rua", "bairro", "cidade", "cep", "uf", "numero", "tipo"]
        if cols.get(k)
    )

    tem_estrutura = bool(
        cols.get("rua") and (cols.get("bairro") or cols.get("cidade") or cols.get("cep") or cols.get("uf"))
    )

    tem_cep_estrutura = bool(
        cols.get("cep") and (cols.get("rua") or cols.get("cidade") or cols.get("bairro"))
    )

    return principais >= 3 and (tem_estrutura or tem_cep_estrutura)

def detectar_mapeamento_por_inferencia(ws):
    score_map = {}
    for c in range(1, ws.max_column + 1):
        score_map[c] = pontuar_coluna_por_amostras(ws, c)

    used = set()
    cols = {
        "tipo": None,
        "rua": None,
        "numero": None,
        "bairro": None,
        "cidade": None,
        "uf": None,
        "cep": None,
        "complemento": None,
    }

    # prioriza campos mais identificáveis
    for field, min_score in [
        ("cep", 4.0),
        ("uf", 4.0),
        ("rua", 3.5),
        ("tipo", 3.2),
        ("numero", 2.8),
        ("complemento", 2.8),
        ("bairro", 2.2),
        ("cidade", 2.2),
    ]:
        col, _ = escolher_melhor_coluna(score_map, field, used, min_score)
        if col:
            cols[field] = col
            used.add(col)

    # Se não detectou rua, tenta um fallback mais tolerante
    if not cols["rua"]:
        col, _ = escolher_melhor_coluna(score_map, "rua", set(), 2.0)
        if col:
            cols["rua"] = col

    # Se não detectou bairro/cidade separadamente, tenta usar ordem posicional
    if cols["rua"] and not cols["bairro"] and not cols["cidade"]:
        candidatos_texto = []
        for col_idx, scores in score_map.items():
            if col_idx == cols["rua"] or col_idx == cols["cep"] or col_idx == cols["uf"]:
                continue
            score_texto = max(scores.get("bairro", 0.0), scores.get("cidade", 0.0))
            if score_texto >= 2.0:
                candidatos_texto.append((col_idx, score_texto))

        candidatos_texto.sort(key=lambda x: x[0])

        if len(candidatos_texto) >= 1:
            cols["bairro"] = cols["bairro"] or candidatos_texto[0][0]
        if len(candidatos_texto) >= 2:
            if candidatos_texto[1][0] != cols["bairro"]:
                cols["cidade"] = cols["cidade"] or candidatos_texto[1][0]

    if not mapeamento_tem_sinais(cols):
        return None

    return {
        "source": "inferencia_inteligente_linha_1",
        "start_row": 1,
        "cols": cols,
    }

def detectar_mapeamento_ws(ws):
    # 1) tenta cabeçalho real na linha 1
    header_det = detectar_header_na_primeira_linha(ws)
    if header_det:
        return header_det

    # 2) tenta inferência inteligente sem depender de cabeçalho
    infer_det = detectar_mapeamento_por_inferencia(ws)
    if infer_det:
        return infer_det

    return {
        "source": None,
        "start_row": 1,
        "cols": {
            "tipo": None,
            "rua": None,
            "numero": None,
            "bairro": None,
            "cidade": None,
            "uf": None,
            "cep": None,
            "complemento": None,
        }
    }

# =========================================================
# JOBS
# =========================================================

def init_job(filename: str, mode: str) -> str:
    job_id = uuid.uuid4().hex
    JOBS[job_id] = {
        "id": job_id,
        "filename": filename,
        "mode": mode,
        "status": "aguardando",
        "progress": 0,
        "current": 0,
        "total": 0,
        "message": "Aguardando processamento...",
        "done": False,
        "error": None,
        "output_bytes": None,
        "output_name": None,
    }
    return job_id

def update_job(job_id: str, **kwargs):
    if job_id in JOBS:
        JOBS[job_id].update(kwargs)

# =========================================================
# CLIENTE HTTP
# =========================================================

class ConsultaContexto:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": APP_UA,
            "Accept": "application/json",
        })
        self.cache_viacep_cep = {}
        self.cache_viacep_busca = {}
        self.cache_nominatim = {}
        self._last_nominatim = 0.0

    def aguardar_nominatim(self):
        agora = time.time()
        delta = agora - self._last_nominatim
        if delta < NOMINATIM_DELAY_SECONDS:
            time.sleep(NOMINATIM_DELAY_SECONDS - delta)
        self._last_nominatim = time.time()

# =========================================================
# VIACEP
# =========================================================

def viacep_por_cep(cep, ctx: ConsultaContexto):
    cep_limpo = so_digitos(cep)
    if len(cep_limpo) != 8:
        return None

    if cep_limpo in ctx.cache_viacep_cep:
        return ctx.cache_viacep_cep[cep_limpo]

    url = f"https://viacep.com.br/ws/{cep_limpo}/json/"
    try:
        resp = ctx.session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get("erro"):
            ctx.cache_viacep_cep[cep_limpo] = None
            return None
        ctx.cache_viacep_cep[cep_limpo] = data
        return data
    except Exception:
        ctx.cache_viacep_cep[cep_limpo] = None
        return None

def escolher_melhor_resultado_viacep(resultados, logradouro_alvo="", bairro_alvo=""):
    if not isinstance(resultados, list) or not resultados:
        return None

    melhor = None
    melhor_score = -1.0

    for item in resultados:
        s1 = similaridade(logradouro_alvo, item.get("logradouro", ""))
        s2 = similaridade(bairro_alvo, item.get("bairro", "")) if bairro_alvo else 0
        score = s1 + (s2 * 0.35)
        if melhor is None or score > melhor_score:
            melhor = item
            melhor_score = score

    return melhor

def viacep_por_endereco(uf, cidade, logradouro, bairro, ctx: ConsultaContexto):
    uf = limpar_texto(uf).upper()
    cidade = limpar_texto(cidade)
    logradouro = limpar_texto(logradouro)

    if len(uf) != 2 or len(cidade) < 2 or len(logradouro) < 3:
        return None

    chave = (uf, normalizar_chave(cidade), normalizar_chave(logradouro))
    if chave in ctx.cache_viacep_busca:
        return ctx.cache_viacep_busca[chave]

    cidade_url = requests.utils.quote(cidade)
    logradouro_url = requests.utils.quote(logradouro)
    url = f"https://viacep.com.br/ws/{uf}/{cidade_url}/{logradouro_url}/json/"

    try:
        resp = ctx.session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        melhor = escolher_melhor_resultado_viacep(data, logradouro, bairro)
        ctx.cache_viacep_busca[chave] = melhor
        return melhor
    except Exception:
        ctx.cache_viacep_busca[chave] = None
        return None

# =========================================================
# OSM
# =========================================================

def nominatim_buscar(endereco, ctx: ConsultaContexto):
    q = limpar_texto(endereco)
    if not q:
        return None

    if q in ctx.cache_nominatim:
        return ctx.cache_nominatim[q]

    try:
        ctx.aguardar_nominatim()
        resp = ctx.session.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": q,
                "format": "jsonv2",
                "addressdetails": 1,
                "limit": 1,
                "countrycodes": "br",
                "accept-language": "pt-BR",
            },
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        item = data[0] if data else None
        ctx.cache_nominatim[q] = item
        return item
    except Exception:
        ctx.cache_nominatim[q] = None
        return None

def pegar_osm_address(item, *keys):
    if not item:
        return ""
    addr = item.get("address", {}) or {}
    for k in keys:
        if addr.get(k):
            return addr.get(k)
    return ""

# =========================================================
# WORKBOOK / PLANILHA
# =========================================================

def ler_campos_da_linha(ws, row_idx: int, cols_map: Dict[str, Any]):
    def v(col_idx):
        if not col_idx:
            return ""
        return limpar_texto(ws.cell(row=row_idx, column=col_idx).value)

    return {
        "tipo": render_tipo_logradouro(v(cols_map["tipo"])),
        "rua": corrigir_rua(v(cols_map["rua"])),
        "numero": limpar_texto(v(cols_map["numero"])),
        "bairro": smart_title(v(cols_map["bairro"])),
        "cidade": smart_title(v(cols_map["cidade"])),
        "uf": limpar_texto(v(cols_map["uf"])).upper(),
        "cep": formatar_cep(v(cols_map["cep"])) or limpar_texto(v(cols_map["cep"])),
        "complemento": limpar_texto(v(cols_map["complemento"])),
    }

def linha_tem_algum_dado(campos):
    return any(limpar_texto(v) for v in campos.values())

def avaliar_linha(campos) -> Tuple[bool, List[str]]:
    via = montar_logradouro(campos.get("tipo", ""), campos.get("rua", ""))
    motivos = []

    if rua_suspeita(via):
        motivos.append("logradouro_suspeito")

    if bairro_suspeito(campos.get("bairro", "")):
        motivos.append("bairro_suspeito")

    if cidade_suspeita(campos.get("cidade", "")):
        motivos.append("cidade_suspeita")

    uf = campos.get("uf", "")
    if uf and not uf_valida(uf):
        motivos.append("uf_invalida")
    if not uf:
        motivos.append("uf_vazia")

    cep = campos.get("cep", "")
    if cep and not cep_valido(cep):
        motivos.append("cep_invalido")
    if not cep:
        motivos.append("cep_vazio")

    numero = campos.get("numero", "")
    if numero and not numero_valido(numero):
        motivos.append("numero_suspeito")

    suspeita = len(motivos) > 0
    return suspeita, motivos

def consegue_busca_endereco(campos):
    via = montar_logradouro(campos.get("tipo", ""), campos.get("rua", ""))
    cidade = campos.get("cidade", "")
    uf = campos.get("uf", "")
    return (
        len(normalizar_chave(via)) >= 4
        and len(normalizar_chave(cidade)) >= 3
        and uf_valida(uf)
    )

def aplicar_retorno_viacep(campos, data):
    if not data:
        return campos

    tipo, nome_logradouro = separar_tipo_nome_logradouro(data.get("logradouro", ""))

    if tipo:
        campos["tipo"] = tipo
    if nome_logradouro:
        campos["rua"] = nome_logradouro
    if data.get("bairro"):
        campos["bairro"] = smart_title(data.get("bairro", ""))
    if data.get("localidade"):
        campos["cidade"] = smart_title(data.get("localidade", ""))
    if data.get("uf"):
        campos["uf"] = limpar_texto(data.get("uf", "")).upper()
    if data.get("cep"):
        campos["cep"] = formatar_cep(data.get("cep", ""))

    return campos

def aplicar_retorno_osm(campos, item):
    if not item:
        return campos

    road = pegar_osm_address(item, "road", "pedestrian", "residential", "footway", "path")
    house_number = pegar_osm_address(item, "house_number")
    suburb = pegar_osm_address(item, "suburb", "neighbourhood")
    city = pegar_osm_address(item, "city", "town", "municipality", "village")
    state = pegar_osm_address(item, "state")
    postcode = pegar_osm_address(item, "postcode")

    tipo, nome = separar_tipo_nome_logradouro(road)

    if tipo:
        campos["tipo"] = tipo
    if nome:
        campos["rua"] = nome
    if house_number:
        campos["numero"] = house_number
    if suburb:
        campos["bairro"] = smart_title(suburb)
    if city:
        campos["cidade"] = smart_title(city)
    if state and not campos.get("uf"):
        state_tokens = tokenizar_normalizado(state)
        if len(state_tokens) == 1 and len(state_tokens[0]) == 2:
            campos["uf"] = state_tokens[0].upper()
    if postcode:
        campos["cep"] = formatar_cep(postcode) or campos.get("cep", "")

    return campos

def definir_status(campos, usou_viacep_cep, usou_viacep_endereco, usou_osm):
    suspeita_final, motivos_finais = avaliar_linha(campos)

    if usou_osm and usou_viacep_cep:
        return "confirmado", "95", motivos_finais

    if usou_osm:
        return "confirmado", "90", motivos_finais

    if usou_viacep_cep and not suspeita_final:
        return "confirmado", "88", motivos_finais

    if usou_viacep_cep:
        return "aproximado", "82", motivos_finais

    if usou_viacep_endereco and not suspeita_final:
        return "aproximado", "78", motivos_finais

    if usou_viacep_endereco:
        return "revisar", "65", motivos_finais

    if not suspeita_final:
        return "presumido", "55", motivos_finais

    return "revisar", "30", motivos_finais

def ensure_output_columns(ws):
    existing = {}
    for c in range(1, ws.max_column + 1):
        val = limpar_texto(ws.cell(row=1, column=c).value)
        if val:
            existing[val] = c

    out = {}
    next_col = ws.max_column + 1

    for h in OUTPUT_HEADERS:
        if h in existing:
            out[h] = existing[h]
        else:
            ws.cell(row=1, column=next_col, value=h)
            out[h] = next_col
            next_col += 1

    return out

def contar_total_linhas_processaveis(wb):
    total = 0

    for ws in wb.worksheets:
        if ws.title == "LOG_PROCESSAMENTO":
            continue
        if ws.max_row < 1:
            continue

        det = detectar_mapeamento_ws(ws)
        if not det["source"]:
            continue

        start_row = det["start_row"]
        cols_map = det["cols"]

        if start_row > ws.max_row:
            continue

        for row_idx in range(start_row, ws.max_row + 1):
            campos = ler_campos_da_linha(ws, row_idx, cols_map)
            if linha_tem_algum_dado(campos):
                total += 1

    return total

# =========================================================
# PROCESSAMENTO DA LINHA
# =========================================================

def processar_linha(campos, ctx, mode):
    suspeita_antes, _ = avaliar_linha(campos)

    usou_viacep_cep = False
    usou_viacep_endereco = False
    usou_osm = False
    osm_item = None
    endereco_consultado = ""

    # -------------------------
    # MODO ULTRA-RÁPIDO
    # -------------------------
    if mode == "ultra":
        endereco_consultado = montar_endereco_consulta(campos)

    # -------------------------
    # MODO RÁPIDO
    # -------------------------
    elif mode == "rapido":
        if suspeita_antes:
            if cep_valido(campos.get("cep", "")):
                data_cep = viacep_por_cep(campos.get("cep", ""), ctx)
                if data_cep:
                    campos = aplicar_retorno_viacep(campos, data_cep)
                    usou_viacep_cep = True
            elif consegue_busca_endereco(campos):
                via = montar_logradouro(campos.get("tipo", ""), campos.get("rua", ""))
                data_end = viacep_por_endereco(
                    campos.get("uf", ""),
                    campos.get("cidade", ""),
                    via,
                    campos.get("bairro", ""),
                    ctx,
                )
                if data_end:
                    campos = aplicar_retorno_viacep(campos, data_end)
                    usou_viacep_endereco = True

        endereco_consultado = montar_endereco_consulta(campos)

    # -------------------------
    # MODO COMPLETO
    # -------------------------
    else:
        if cep_valido(campos.get("cep", "")):
            data_cep = viacep_por_cep(campos.get("cep", ""), ctx)
            if data_cep:
                campos = aplicar_retorno_viacep(campos, data_cep)
                usou_viacep_cep = True

        if not usou_viacep_cep and consegue_busca_endereco(campos):
            via = montar_logradouro(campos.get("tipo", ""), campos.get("rua", ""))
            data_end = viacep_por_endereco(
                campos.get("uf", ""),
                campos.get("cidade", ""),
                via,
                campos.get("bairro", ""),
                ctx,
            )
            if data_end:
                campos = aplicar_retorno_viacep(campos, data_end)
                usou_viacep_endereco = True

        endereco_consultado = montar_endereco_consulta(campos)
        if len(normalizar_chave(endereco_consultado)) >= 8:
            osm_item = nominatim_buscar(endereco_consultado, ctx)
            if osm_item:
                campos = aplicar_retorno_osm(campos, osm_item)
                usou_osm = True

    status, confianca, motivos_finais = definir_status(
        campos=campos,
        usou_viacep_cep=usou_viacep_cep,
        usou_viacep_endereco=usou_viacep_endereco,
        usou_osm=usou_osm,
    )

    fontes = []
    if usou_viacep_cep:
        fontes.append("ViaCEP(CEP)")
    if usou_viacep_endereco:
        fontes.append("ViaCEP(endereço)")
    if usou_osm:
        fontes.append("OSM")
    if not fontes:
        fontes.append("Local")

    if mode == "ultra":
        obs = "Modo ultra-rápido: padronização local com detecção inteligente, sem consultas externas."
    elif mode == "rapido":
        if suspeita_antes and not (usou_viacep_cep or usou_viacep_endereco):
            obs = "Linha suspeita, sem retorno externo; manteve limpeza local."
        elif usou_viacep_cep or usou_viacep_endereco:
            obs = "Linha enriquecida no modo rápido."
        else:
            obs = "Linha considerada boa; apenas limpeza local."
    else:
        if usou_osm:
            obs = "Linha enriquecida no modo completo com OSM."
        elif usou_viacep_cep or usou_viacep_endereco:
            obs = "Linha enriquecida no modo completo com ViaCEP."
        else:
            obs = "Sem retorno externo; manteve limpeza local."

    return {
        "campos": campos,
        "osm_item": osm_item,
        "endereco_consultado": endereco_consultado,
        "status": status,
        "confianca": confianca,
        "fontes": " + ".join(fontes),
        "motivos_finais": motivos_finais,
        "obs": obs,
    }

def escrever_saida_linha(ws, row_idx, out_cols, mode, source_label, resultado):
    campos = resultado["campos"]
    osm_item = resultado["osm_item"]

    mode_label = {
        "ultra": "ULTRA",
        "rapido": "RÁPIDO",
        "completo": "COMPLETO",
    }[mode]

    ws.cell(row=row_idx, column=out_cols["geo_modo"], value=mode_label)
    ws.cell(row=row_idx, column=out_cols["geo_colunas_fonte"], value=source_label)

    ws.cell(row=row_idx, column=out_cols["geo_tipo_sugerido"], value=campos.get("tipo", ""))
    ws.cell(row=row_idx, column=out_cols["geo_rua_sugerida"], value=campos.get("rua", ""))
    ws.cell(row=row_idx, column=out_cols["geo_numero_sugerido"], value=campos.get("numero", ""))
    ws.cell(row=row_idx, column=out_cols["geo_bairro_sugerido"], value=campos.get("bairro", ""))
    ws.cell(row=row_idx, column=out_cols["geo_cidade_sugerida"], value=campos.get("cidade", ""))
    ws.cell(row=row_idx, column=out_cols["geo_uf_sugerida"], value=campos.get("uf", ""))
    ws.cell(row=row_idx, column=out_cols["geo_cep_sugerido"], value=campos.get("cep", ""))

    ws.cell(row=row_idx, column=out_cols["geo_status"], value=resultado["status"])
    ws.cell(row=row_idx, column=out_cols["geo_confianca"], value=resultado["confianca"])
    ws.cell(row=row_idx, column=out_cols["geo_fonte"], value=resultado["fontes"])
    ws.cell(row=row_idx, column=out_cols["geo_revisar_motivos"], value="; ".join(resultado["motivos_finais"]))
    ws.cell(row=row_idx, column=out_cols["geo_endereco_consultado"], value=resultado["endereco_consultado"])
    ws.cell(row=row_idx, column=out_cols["geo_obs"], value=resultado["obs"])

    if osm_item:
        ws.cell(row=row_idx, column=out_cols["geo_display_name"], value=limpar_texto(osm_item.get("display_name", "")))
        ws.cell(row=row_idx, column=out_cols["geo_lat"], value=limpar_texto(osm_item.get("lat", "")))
        ws.cell(row=row_idx, column=out_cols["geo_lng"], value=limpar_texto(osm_item.get("lon", "")))

# =========================================================
# WORKBOOK
# =========================================================

def processar_workbook_bytes(input_bytes: bytes, mode: str, ext: str, job_id=None) -> bytes:
    ctx = ConsultaContexto()

    keep_vba = ext == ".xlsm"
    wb = load_workbook(BytesIO(input_bytes), keep_vba=keep_vba)

    total = contar_total_linhas_processaveis(wb)

    if job_id:
        update_job(
            job_id,
            total=total,
            current=0,
            progress=0,
            status="processando",
            message="Lendo planilha..."
        )

    logs = []
    current = 0

    for ws in wb.worksheets:
        if ws.title == "LOG_PROCESSAMENTO":
            continue

        if ws.max_row < 1:
            logs.append([ws.title, "não", "", 0, "Aba vazia"])
            continue

        det = detectar_mapeamento_ws(ws)
        cols_map = det["cols"]
        source = det["source"]
        start_row = det["start_row"]

        if not source:
            logs.append([ws.title, "não", "", 0, "Aba preservada sem processamento; estrutura de endereço não identificada com segurança."])
            continue

        if start_row > ws.max_row:
            logs.append([ws.title, "não", source, 0, "Nenhuma linha de dados para processar."])
            continue

        out_cols = ensure_output_columns(ws)
        processadas = 0

        for row_idx in range(start_row, ws.max_row + 1):
            campos = ler_campos_da_linha(ws, row_idx, cols_map)

            if not linha_tem_algum_dado(campos):
                continue

            resultado = processar_linha(campos, ctx, mode)
            escrever_saida_linha(ws, row_idx, out_cols, mode, source, resultado)
            processadas += 1
            current += 1

            if job_id and total > 0:
                pct = int((current / total) * 100)
                update_job(
                    job_id,
                    current=current,
                    total=total,
                    progress=min(pct, 100),
                    status="processando",
                    message=f"Modo {mode}: {current}/{total} • Aba {ws.title}"
                )

        logs.append([
            ws.title,
            "sim",
            source,
            processadas,
            f"Aba preservada; apenas colunas geo_* foram adicionadas. Início de leitura na linha {start_row}."
        ])

    if "LOG_PROCESSAMENTO" in wb.sheetnames:
        del wb["LOG_PROCESSAMENTO"]

    log_ws = wb.create_sheet("LOG_PROCESSAMENTO")
    log_ws.append(["aba", "processada", "fonte_colunas", "linhas_processadas", "observacao"])
    for linha in logs:
        log_ws.append(linha)

    out = BytesIO()
    wb.save(out)
    out.seek(0)
    return out.getvalue()

def executar_job(job_id: str, input_bytes: bytes, ext: str, nome_arquivo: str, mode: str):
    try:
        update_job(
            job_id,
            status="processando",
            message=f"Iniciando processamento no modo {mode}..."
        )

        output_bytes = processar_workbook_bytes(
            input_bytes=input_bytes,
            mode=mode,
            ext=ext,
            job_id=job_id
        )

        output_ext = ".xlsm" if ext == ".xlsm" else ".xlsx"

        update_job(
            job_id,
            status="concluido",
            progress=100,
            done=True,
            output_bytes=output_bytes,
            output_name=f"geopadroniza_{mode}_{Path(nome_arquivo).stem}{output_ext}",
            message="Processamento concluído."
        )
    except Exception as e:
        update_job(
            job_id,
            status="erro",
            done=True,
            error=str(e),
            message=f"Erro: {str(e)}"
        )

# =========================================================
# ROTAS
# =========================================================

@app.get("/", response_class=HTMLResponse)
def home():
    html_file = BASE_DIR / "index.html"
    if html_file.exists():
        return HTMLResponse(html_file.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>GeoPadroniza</h1><p>Coloque o index.html na mesma pasta do app.py.</p>")

@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)

@app.get("/health")
def health():
    return {
        "ok": True,
        "app": APP_NAME,
        "modo_seguro": True,
        "preserva_colunas_originais": True,
        "deteccao_inteligente": True,
        "linha_1_pode_ser_dado": True,
        "modos": ["ultra", "rapido", "completo"],
        "extensoes_seguras": [".xlsx", ".xlsm"],
    }

@app.post("/process")
async def process(
    file: UploadFile = File(...),
    mode: str = Form("ultra")
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Arquivo inválido.")

    ext = Path(file.filename).suffix.lower()
    if ext not in [".xlsx", ".xlsm"]:
        raise HTTPException(
            status_code=400,
            detail="Para preservar CPF/CNPJ e abas sem risco, use arquivo .xlsx ou .xlsm."
        )

    mode = (mode or "ultra").strip().lower()
    if mode not in MODES:
        raise HTTPException(status_code=400, detail="Modo inválido. Use 'ultra', 'rapido' ou 'completo'.")

    input_bytes = await file.read()
    if not input_bytes:
        raise HTTPException(status_code=400, detail="Arquivo vazio.")

    job_id = init_job(file.filename, mode)

    thread = threading.Thread(
        target=executar_job,
        args=(job_id, input_bytes, ext, file.filename, mode),
        daemon=True
    )
    thread.start()

    return {
        "ok": True,
        "job_id": job_id,
        "mode": mode
    }

@app.get("/status/{job_id}")
def status_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado.")

    return {
        "id": job["id"],
        "mode": job["mode"],
        "status": job["status"],
        "progress": job["progress"],
        "current": job["current"],
        "total": job["total"],
        "message": job["message"],
        "done": job["done"],
        "error": job["error"],
    }

@app.get("/download/{job_id}")
def download_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado.")

    if not job["done"]:
        raise HTTPException(status_code=400, detail="O processamento ainda não terminou.")

    if job["error"]:
        raise HTTPException(status_code=500, detail=job["error"])

    if not job["output_bytes"]:
        raise HTTPException(status_code=500, detail="Arquivo final não encontrado.")

    headers = {
        "Content-Disposition": f'attachment; filename="{job["output_name"]}"'
    }

    return StreamingResponse(
        BytesIO(job["output_bytes"]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )