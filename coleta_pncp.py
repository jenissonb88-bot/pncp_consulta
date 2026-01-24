import requests
import json
import os
import zipfile
import concurrent.futures
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

# --- CONFIGURAÇÕES ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CNPJ_ALVO = "08778201000126"
# Forçando uma data específica para teste (depois voltamos ao checkpoint)
DATA_TESTE_FIXA = datetime(2024, 7, 2) 
MAX_WORKERS = 10 
ARQ_ZIP = 'dados_pncp.zip'
ARQ_JSON_INTERNO = 'dados_pncp.json'
ARQ_CHECKPOINT = 'checkpoint.txt'

ESTADOS_EXCLUIDOS = ["PR", "SC", "RS", "DF", "RO", "RR", "AP", "AC"]

PALAVRAS_INTERESSE = [
    "MEDICAMENTO", "REMEDIO", "HOSPITAL", "SAUDE", "INSUMO", 
    "FRALDA", "SORO", "ABSORVENTE", "HOSPITALAR", "FARMAC", 
    "MEDICO", "ODONTO", "QUIMICO", "LABORAT", "CLINIC", 
    "CIRURGIC", "SANEANTE", "PENSO", "DIALISE", "GAZE", "AGULHA",
    "MATERIAIS", "AQUISICAO" # Adicionei termos genéricos para teste
]

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def objeto_e_relevante(texto):
    if not texto: return False
    texto_upper = texto.upper()
    return any(termo in texto_upper for termo in PALAVRAS_INTERESSE)

def criar_sessao():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def processar_item_ranking(session, it, url_base_itens, cnpj_alvo):
    # REMOVIDO O FILTRO 'temResultado' PARA VER SE ELE ENCONTRA O ITEM
    # if not it.get('temResultado'): return None 
    
    num_item = it.get('numeroItem')
    url_res = f"{url_base_itens}/{num_item}/resultados"
    try:
        r = session.get(url_res, timeout=10)
        if r.status_code == 200:
            vends = r.json()
            if not vends: return None # Tem item, mas não tem ganhador ainda
            
            if isinstance(vends, dict): vends = [vends]
            resultados = []
            for v in vends:
                cnpj_venc = (v.get('niFornecedor') or "").replace(".", "").replace("/", "").replace("-", "")
                dt_h = v.get('dataHomologacao') or v.get('dataResultado') or "---"
                resultados.append({
                    "item": num_item,
                    "total": float(v.get('valorTotalHomologado') or 0),
                    "fornecedor": v.get('nomeRazaoSocialFornecedor'),
                    "cnpj": cnpj_venc
                })
            return resultados
    except: pass
    return None

def run_diagnostico():
    session = criar_sessao()
    data_atual = DATA_TESTE_FIXA
    DATA_STR = data_atual.strftime('%Y%m%d')
    
    print(f"\n🕵️ MODO ESPIÃO ATIVADO: {data_atual.strftime('%d/%m/%Y')}")
    print("Vamos analisar as primeiras 50 licitações e ver por que estão sendo ignoradas...\n")

    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = { 
        "dataInicial": DATA_STR, "dataFinal": DATA_STR, 
        "codigoModalidadeContratacao": "6", "pagina": 1, "tamanhoPagina": 50 
    }
    
    resp = session.get(url, params=params)
    print(f"📡 Status da API: {resp.status_code}")
    
    if resp.status_code != 200:
        print("❌ Erro na API do PNCP. O site pode estar fora do ar ou bloqueando.")
        return

    data_json = resp.json()
    lics = data_json.get('data', [])
    print(f"📦 Licitações encontradas na página 1: {len(lics)}")

    if not lics:
        print("⚠️ A API retornou LISTA VAZIA para esta data. Tente outra data.")
        return

    contadores = {"estado_excluido": 0, "objeto_irrelevante": 0, "sem_itens": 0, "sucesso": 0}

    for i, lic in enumerate(lics):
        uf = lic.get('unidadeOrgao', {}).get('ufSigla')
        obj = (lic.get('objeto', '') or "SEM OBJETO").upper()
        orgao = lic.get('orgaoEntidade', {}).get('razaoSocial')
        
        print(f"--- Licitação #{i+1} ({uf}) ---")
        
        if uf in ESTADOS_EXCLUIDOS:
            print(f"❌ Ignorada: Estado {uf} está na lista negra.")
            contadores["estado_excluido"] += 1
            continue

        if not objeto_e_relevante(obj):
            print(f"❌ Ignorada: Objeto não tem palavras-chave.")
            print(f"   Texto: {obj[:100]}...")
            contadores["objeto_irrelevante"] += 1
            continue

        print(f"✅ PASSOU NO FILTRO! Verificando itens...")
        print(f"   Órgão: {orgao}")
        print(f"   Objeto: {obj[:60]}...")

        # Tentar baixar itens
        cnpj_org = str(lic.get('orgaoEntidade', {}).get('cnpj', '')).replace(".", "").replace("/", "").replace("-", "")
        ano = lic.get('anoCompra')
        seq = lic.get('sequencialCompra')
        url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens"
        
        r_it = session.get(url_itens, params={'pagina': 1}, timeout=10)
        itens = r_it.json()
        
        if not itens:
            print("   ⚠️ Sem itens cadastrados na API.")
            contadores["sem_itens"] += 1
            continue
            
        print(f"   📄 Itens encontrados: {len(itens)}. Verificando resultados...")
        
        # Pega o primeiro item só para testar
        res = processar_item_ranking(session, itens[0], url_itens, CNPJ_ALVO)
        if res:
            print(f"   🎉 TEM RESULTADO! Fornecedor: {res[0]['fornecedor']}")
            contadores["sucesso"] += 1
        else:
            print("   ❄️ Sem resultado (Licitação publicada mas não finalizada ou API vazia).")

    print("\n--- RESUMO DO DIAGNÓSTICO ---")
    print(f"Total analisado: {len(lics)}")
    print(f"Ignorado por Estado: {contadores['estado_excluido']}")
    print(f"Ignorado por Palavra-Chave: {contadores['objeto_irrelevante']}")
    print(f"Com itens mas sem resultado: {len(lics) - sum(contadores.values()) + contadores['sucesso']}")
    print(f"Sucessos confirmados: {contadores['sucesso']}")

if __name__ == "__main__":
    run_diagnostico()
