import requests
import json
from datetime import datetime, timedelta
import os
import time
import urllib3
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÃO ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

ARQ_DADOS = 'dados_pncp.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
CNPJ_ALVO = "08778201000126"  # DROGAFONTE
DATA_LIMITE_FINAL = datetime.now()
MAX_WORKERS = 20  # Velocidade turbo: 20 processos simultâneos

# -------------------------------------------------
# MOTOR DE CONEXÃO
# -------------------------------------------------
def criar_sessao():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount('https://', adapter)
    return session

# -------------------------------------------------
# UTILITÁRIOS DE DADOS
# -------------------------------------------------
def carregar_banco():
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                dados = json.load(f)
                return {lic.get('id_licitacao'): lic for lic in dados}
        except: pass
    return {}

def salvar_estado(banco, data_proxima):
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, indent=2, ensure_ascii=False)
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(data_proxima.strftime('%Y%m%d'))
    print(f"\n💾 [ESTADO SALVO] {len(banco)} licitações mapeadas.")

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                return datetime.strptime(f.read().strip(), '%Y%m%d')
        except: pass
    return datetime(2025, 1, 1)

# -------------------------------------------------
# WORKER: PROCESSAMENTO PARALELO DE ITENS
# -------------------------------------------------
def processar_item_full(session, it, url_base_itens, cnpj_alvo):
    """Analisa um item e retorna todos os seus vencedores."""
    if not it.get('temResultado'):
        return None

    num_item = it.get('numeroItem')
    url_res = f"{url_base_itens}/{num_item}/resultados"
    
    try:
        r_res = session.get(url_res, timeout=15)
        if r_res.status_code == 200:
            resultados = r_res.json()
            if isinstance(resultados, dict): resultados = [resultados]
            
            itens_vencedores = []
            for res in resultados:
                cnpj_venc = (res.get('niFornecedor') or "").replace(".", "").replace("/", "").replace("-", "")
                val_total = float(res.get('valorTotalHomologado') or 0.0)
                
                itens_vencedores.append({
                    "numero_item": num_item,
                    "descricao": it.get('descricao', ''),
                    "quantidade": float(res.get('quantidadeHomologada') or 0.0),
                    "valor_unitario": float(res.get('valorUnitarioHomologado') or 0.0),
                    "valor_total_item": val_total,
                    "cnpj_fornecedor": cnpj_venc,
                    "nome_fornecedor": res.get('nomeRazaoSocialFornecedor'),
                    "data_homologacao": res.get('dataHomologacao') or res.get('dataResultado'),
                    "vencedor_e_alvo": (cnpj_alvo in cnpj_venc)
                })
            return itens_vencedores
    except:
        pass
    return None

# -------------------------------------------------
# EXECUÇÃO PRINCIPAL
# -------------------------------------------------
def run():
    session = criar_sessao()
    data_atual = ler_checkpoint()
    
    if data_atual.date() > DATA_LIMITE_FINAL.date():
        print("✅ Tudo atualizado!")
        return

    print(f"🚀 TURBO PNCP: Processando {data_atual.strftime('%d/%m/%Y')}")
    banco_total = carregar_banco()
    DATA_STR = data_atual.strftime('%Y%m%d')
    
    pagina = 1
    while True:
        url_busca = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR, "dataFinal": DATA_STR,
            "codigoModalidadeContratacao": "6", "pagina": pagina,
            "tamanhoPagina": 50, "niFornecedor": CNPJ_ALVO
        }

        resp = session.get(url_busca, params=params, timeout=30)
        if resp.status_code != 200: break
        
        json_resp = resp.json()
        lics = json_resp.get('data', [])
        if not lics: break

        print(f"📦 Página {pagina}: {len(lics)} editais.")

        for lic in lics:
            # 1. Identificação
            cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
            ano, seq = lic.get('anoCompra'), lic.get('sequencialCompra')
            uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
            id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"
            
            print(f"  🔍 Analisando {id_lic}...", end=" ", flush=True)

            # 2. Paginação de Itens (Garantir que pega todos)
            todos_itens_api = []
            pag_item = 1
            url_base_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens"
            
            while True:
                r_it = session.get(url_base_itens, params={'pagina': pag_item, 'tamanhoPagina': 1000})
                if r_it.status_code != 200: break
                lote = r_it.json()
                if not lote: break
                todos_itens_api.extend(lote)
                if len(lote) < 1000: break
                pag_item += 1

            # 3. Processamento Turbo (Multithread)
            itens_consolidados = []
            resumo_ranking = {}
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(processar_item_full, session, it, url_base_itens, CNPJ_ALVO) for it in todos_itens_api]
                for future in concurrent.futures.as_completed(futures):
                    resultado = future.result()
                    if resultado:
                        for r in resultado:
                            itens_consolidados.append(r)
                            # Atualiza ranking de fornecedores
                            nome = r['nome_fornecedor'] or "Desconhecido"
                            resumo_ranking[nome] = resumo_ranking.get(nome, 0.0) + r['valor_total_item']

            # 4. Salvar no Objeto
            if itens_consolidados:
                banco_total[id_lic] = {
                    "id_licitacao": id_lic,
                    "orgao_nome": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                    "numero_pregao": f"{lic.get('numeroCompra')}/{ano}",
                    "cidade": lic.get('unidadeOrgao', {}).get('municipioNome'),
                    "uf": lic.get('unidadeOrgao', {}).get('ufSigla'),
                    "link_edital": f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}",
                    "itens_todos_fornecedores": sorted(itens_consolidados, key=lambda x: x['numero_item']),
                    "resumo_fornecedores": resumo_ranking,
                    "ValorTotal": sum(resumo_ranking.values())
                }
                print(f"OK! ({len(itens_consolidados)} itens mapeados)")
            else:
                print("Sem resultados homologados.")

        if pagina >= json_resp.get('totalPaginas', 1): break
        pagina += 1

    salvar_estado(banco_total, data_atual + timedelta(days=1))

if __name__ == "__main__":
    run()
