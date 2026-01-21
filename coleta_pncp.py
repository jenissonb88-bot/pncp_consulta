import requests
import json
import os
import time
import urllib3
import concurrent.futures
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES GERAIS ---
# A ordem aqui é vital para evitar o erro "NameError"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CNPJ_ALVO = "08778201000126"   # DROGAFONTE
DATA_LIMITE_FINAL = datetime.now()
DIAS_POR_CICLO = 1             # Mude para 30 para buscar o passado de uma vez
MAX_WORKERS = 20               # Processos simultâneos
ARQ_DADOS = 'dados_pncp.json'
ARQ_CHECKPOINT = 'checkpoint.txt'

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

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
    session.mount('http://', adapter)
    return session

# -------------------------------------------------
# UTILITÁRIOS DE DADOS
# -------------------------------------------------
def carregar_banco():
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                conteudo = f.read().strip()
                if not conteudo: return {}
                dados = json.loads(conteudo)
                return {lic.get('id_licitacao'): lic for lic in dados}
        except: pass
    return {}

def salvar_estado(banco, data_proxima):
    # Salva a lista de licitações
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, indent=2, ensure_ascii=False)
    # Atualiza o checkpoint no arquivo texto
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(data_proxima.strftime('%Y%m%d'))
    print(f"\n💾 [SALVO] Banco: {len(banco)} processos | Checkpoint: {data_proxima.strftime('%d/%m/%Y')}")

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                return datetime.strptime(f.read().strip(), '%Y%m%d')
        except: pass
    return datetime(2025, 1, 1)

# -------------------------------------------------
# WORKER: PROCESSAMENTO DE ITENS
# -------------------------------------------------
def processar_item_full(session, it, url_base_itens, cnpj_alvo):
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
                itens_vencedores.append({
                    "numero_item": num_item,
                    "descricao": it.get('descricao', ''),
                    "quantidade": float(res.get('quantidadeHomologada') or 0.0),
                    "valor_unitario": float(res.get('valorUnitarioHomologado') or 0.0),
                    "valor_total_item": float(res.get('valorTotalHomologado') or 0.0),
                    "cnpj_fornecedor": cnpj_venc,
                    "nome_fornecedor": res.get('nomeRazaoSocialFornecedor'),
                    "vencedor_e_alvo": (cnpj_alvo in cnpj_venc)
                })
            return itens_vencedores
    except: pass
    return None

# -------------------------------------------------
# EXECUÇÃO PRINCIPAL (LOOP DE CICLOS)
# -------------------------------------------------
def run():
    session = criar_sessao()
    data_atual = ler_checkpoint()
    
    if data_atual.date() > DATA_LIMIT_FINAL.date():
        print("✅ Tudo atualizado!")
        return

    data_fim = data_atual + timedelta(days=DIAS_POR_CICLO - 1)
    if data_fim > DATA_LIMIT_FINAL: data_fim = DATA_LIMIT_FINAL

    print(f"--- 🚀 INICIANDO RANKING: {data_atual.strftime('%d/%m')} até {data_fim.strftime('%d/%m')} ---")
    
    banco_total = carregar_banco()

    while data_atual <= data_fim:
        DATA_STR = data_atual.strftime('%Y%m%d')
        print(f"\n📅 Dia {data_atual.strftime('%d/%m/%Y')}:", end=" ", flush=True)
        
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

            for lic in lics:
                cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                ano, seq = lic.get('anoCompra'), lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
                id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"
                
                # Pegando o Edital oficial
                edital_oficial = f"{lic.get('numeroCompra')}/{ano}"

                # Paginação de Itens
                todos_itens_api = []
                pag_item = 1
                url_base_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens"
                while True:
                    r_it = session.get(url_base_itens, params={'pagina': pag_item, 'tamanhoPagina': 1000}, timeout=20)
                    if r_it.status_code != 200: break
                    lote = r_it.json()
                    if not lote: break
                    todos_itens_api.extend(lote)
                    if len(lote) < 1000: break
                    pag_item += 1

                # Processamento Multithread
                itens_consolidados = []
                resumo_ranking = {}
                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [executor.submit(processar_item_full, session, it, url_base_itens, CNPJ_ALVO) for it in todos_itens_api]
                    for future in concurrent.futures.as_completed(futures):
                        resultado = future.result()
                        if resultado:
                            for r in resultado:
                                itens_consolidados.append(r)
                                nome = r['nome_fornecedor'] or "Desconhecido"
                                resumo_ranking[nome] = resumo_ranking.get(nome, 0.0) + r['valor_total_item']

                if itens_consolidados:
                    banco_total[id_lic] = {
                        "id_licitacao": id_lic,
                        "orgao_nome": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                        "numero_pregao": edital_oficial,
                        "cidade": lic.get('unidadeOrgao', {}).get('municipioNome'),
                        "uf": lic.get('unidadeOrgao', {}).get('ufSigla'),
                        "uasg": uasg,
                        "link_edital": f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}",
                        "itens_todos_fornecedores": sorted(itens_consolidados, key=lambda x: x['numero_item']),
                        "resumo_fornecedores": resumo_ranking,
                        "ValorTotal": sum(resumo_ranking.values())
                    }
                    print("✅", end="", flush=True)

            if pagina >= json_resp.get('totalPaginas', 1): break
            pagina += 1

        # Avança o dia e salva o estado
        data_atual += timedelta(days=1)
        salvar_estado(banco_total, data_atual)

if __name__ == "__main__":
    run()
