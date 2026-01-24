import requests
import json
import os
import time
import urllib3
import concurrent.futures
import zipfile
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CNPJ_ALVO = "08778201000126"   # DROGAFONTE
DATA_LIMITE_FINAL = datetime.now()
DIAS_POR_CICLO = 1             
MAX_WORKERS = 20               
ARQ_ZIP = 'dados_pncp.zip'     
ARQ_JSON_INTERNO = 'dados_pncp.json' 
ARQ_CHECKPOINT = 'checkpoint.txt'

# 1. ESTADOS EXCLUÍDOS
ESTADOS_EXCLUIDOS = ["PR", "SC", "RS", "DF", "RO", "RR", "AP", "AC"]

# 2. PALAVRAS-CHAVE EXPANDIDAS (O objeto deve conter pelo menos uma)
PALAVRAS_INTERESSE = [
    "MEDICAMENTO", "REMEDIO", "HOSPITAL", "SAUDE", "INSUMO", 
    "FRALDA", "SORO", "ABSORVENTE", "HOSPITALAR", "FARMAC", 
    "MEDICO", "ODONTO", "QUIMICO", "LABORAT", "CLINIC", 
    "CIRURGIC", "SANEANTE", "PENSO", "DIALISE", "GAZE", "AGULHA"
]

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def objeto_e_relevante(texto):
    """Verifica se o objeto da licitação é do nicho de saúde."""
    if not texto: return False
    texto_upper = texto.upper()
    return any(termo in texto_upper for termo in PALAVRAS_INTERESSE)

def carregar_banco():
    """Carrega o banco e aplica filtros, protegendo dados antigos sem campo 'objeto'."""
    if os.path.exists(ARQ_ZIP):
        try:
            with zipfile.ZipFile(ARQ_ZIP, 'r') as z:
                arquivos = z.namelist()
                json_file = next((f for f in arquivos if f.endswith('.json')), None)
                
                if json_file:
                    with z.open(json_file) as f:
                        dados = json.load(f)
                        banco_filtrado = {}
                        for lic in dados:
                            uf = lic.get('uf')
                            obj = lic.get('objeto')
                            
                            # Filtro 1: Se o estado está na lista de exclusão, descarta sempre
                            if uf in ESTADOS_EXCLUIDOS:
                                continue
                            
                            # Filtro 2: Se não tem 'objeto' (dado antigo), mantém para segurança.
                            # Se tem, aplica a filtragem de palavras-chave.
                            if obj is None or objeto_e_relevante(obj):
                                banco_filtrado[lic.get('id_licitacao')] = lic
                        
                        removidos = len(dados) - len(banco_filtrado)
                        if removidos > 0:
                            print(f"🧹 Faxina: {removidos} registros removidos. Restam {len(banco_filtrado)}.")
                        return banco_filtrado
        except Exception as e:
            print(f"⚠️ Erro ao carregar banco: {e}")
    return {}

def salvar_estado(banco, data_proxima):
    lista_final = list(banco.values())
    with open(ARQ_JSON_INTERNO, 'w', encoding='utf-8') as f:
        json.dump(lista_final, f, ensure_ascii=False)
    with zipfile.ZipFile(ARQ_ZIP, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        z.write(ARQ_JSON_INTERNO, arcname=ARQ_JSON_INTERNO)
    if os.path.exists(ARQ_JSON_INTERNO): os.remove(ARQ_JSON_INTERNO)
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(data_proxima.strftime('%Y%m%d'))
    print(f"\n💾 [SUCESSO] Banco salvo com {len(lista_final)} licitações relevantes.")

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                data_str = f.read().strip()
                if data_str:
                    return datetime.strptime(data_str, '%Y%m%d')
        except: pass
    return datetime(2025, 1, 1)

def criar_sessao():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def processar_item_ranking(session, it, url_base_itens, cnpj_alvo):
    if not it.get('temResultado'): return None
    num_item = it.get('numeroItem')
    url_res = f"{url_base_itens}/{num_item}/resultados"
    try:
        r = session.get(url_res, timeout=15)
        if r.status_code == 200:
            vends = r.json()
            if isinstance(vends, dict): vends = [vends]
            resultados = []
            for v in vends:
                cnpj_venc = (v.get('niFornecedor') or "").replace(".", "").replace("/", "").replace("-", "")
                dt_h = v.get('dataHomologacao') or v.get('dataResultado') or ""
                if dt_h:
                    try: dt_h = "/".join(dt_h.split('T')[0].split('-')[::-1])
                    except: dt_h = "---"
                else: dt_h = "---"
                resultados.append({
                    "item": num_item,
                    "desc": it.get('descricao', ''),
                    "qtd": float(v.get('quantidadeHomologada') or 0),
                    "unitario": float(v.get('valorUnitarioHomologado') or 0),
                    "total": float(v.get('valorTotalHomologado') or 0),
                    "fornecedor": v.get('nomeRazaoSocialFornecedor'),
                    "cnpj": cnpj_venc,
                    "data_homo": dt_h,
                    "e_alvo": (cnpj_alvo in cnpj_venc)
                })
            return resultados
    except: pass
    return None

def run():
    session = criar_sessao()
    data_atual = ler_checkpoint()
    
    if data_atual.date() > DATA_LIMITE_FINAL.date():
        print("✅ Ranking atualizado!")
        return

    banco_total = carregar_banco()
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"--- 🏥 BUSCA SAÚDE: {data_atual.strftime('%d/%m/%Y')} ---")

    pagina = 1
    while True:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = { 
            "dataInicial": DATA_STR, "dataFinal": DATA_STR, 
            "codigoModalidadeContratacao": "6", "pagina": pagina, "tamanhoPagina": 50 
        }
        
        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code != 200: break
            data_json = resp.json()
            lics = data_json.get('data', [])
            if not lics: break
        except: break

        for lic in lics:
            uf_licitacao = lic.get('unidadeOrgao', {}).get('ufSigla')
            objeto_desc = (lic.get('objeto', '') or "").strip()

            # Filtros Iniciais (UF e Palavras-chave)
            if uf_licitacao in ESTADOS_EXCLUIDOS: continue
            if not objeto_e_relevante(objeto_desc): continue

            # Se chegou aqui, é saúde e é de um estado permitido. Vamos processar:
            cnpj_org_bruto = lic.get('orgaoEntidade', {}).get('cnpj', '')
            cnpj_org_limpo = str(cnpj_org_bruto).replace(".", "").replace("/", "").replace("-", "").strip()
            ano = lic.get('anoCompra')
            seq = lic.get('sequencialCompra')
            uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
            id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"
            
            url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org_limpo}/compras/{ano}/{seq}/itens"
            todos_itens = []
            p_it = 1
            while True:
                try:
                    r_it = session.get(url_itens, params={'pagina': p_it, 'tamanhoPagina': 1000}, timeout=20)
                    if r_it.status_code != 200: break
                    lote = r_it.json()
                    if not lote: break
                    todos_itens.extend(lote)
                    if len(lote) < 1000: break
                    p_it += 1
                except: break

            if not todos_itens: continue

            itens_ranking = []
            resumo = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(processar_item_ranking, session, it, url_itens, CNPJ_ALVO) for it in todos_itens]
                for f in concurrent.futures.as_completed(futures):
                    res = f.result()
                    if res:
                        for r in res:
                            itens_ranking.append(r)
                            nome = r['fornecedor'] or "Desconhecido"
                            resumo[nome] = resumo.get(nome, 0) + r['total']

            if itens_ranking:
                banco_total[id_lic] = {
                    "id_licitacao": id_lic,
                    "orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                    "objeto": objeto_desc, # SALVAMENTO DO OBJETO PARA FILTRO FUTURO
                    "edital": f"{lic.get('numeroCompra')}/{ano}",
                    "uf": uf_licitacao,
                    "cidade": lic.get('unidadeOrgao', {}).get('municipioNome'),
                    "uasg": uasg,
                    "link_edital": f"https://pncp.gov.br/app/editais/{cnpj_org_limpo}/{ano}/{seq}",
                    "itens": itens_ranking,
                    "resumo": resumo,
                    "total_licitacao": sum(resumo.values())
                }
                print("💊", end="", flush=True)

        if pagina >= data_json.get('totalPaginas', 1): break
        pagina += 1

    salvar_estado(banco_total, data_atual + timedelta(days=1))

if __name__ == "__main__":
    run()
