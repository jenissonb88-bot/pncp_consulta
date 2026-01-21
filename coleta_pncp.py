import requests
import json
import os
import time
import urllib3
import concurrent.futures
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES ---
CNPJ_ALVO = "08778201000126"
DIAS_POR_CICLO = 1
MAX_WORKERS = 20
ARQ_DADOS = 'dados_pncp.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
DATA_LIMITE_FINAL = datetime.now()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def criar_sessao():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

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
    print(f" 💾 [Banco: {len(banco)} licitações]", end="", flush=True)

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                return datetime.strptime(f.read().strip(), '%Y%m%d')
        except: pass
    return datetime(2025, 1, 1)

def processar_item_ranking(session, it, url_base_itens, cnpj_alvo):
    """Coleta todos os vencedores do item para o Ranking."""
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
                resultados.append({
                    "item": num_item,
                    "desc": it.get('descricao', ''),
                    "qtd": float(v.get('quantidadeHomologada') or 0),
                    "valor": float(v.get('valorUnitarioHomologado') or 0),
                    "total": float(v.get('valorTotalHomologado') or 0),
                    "fornecedor": v.get('nomeRazaoSocialFornecedor'),
                    "cnpj": cnpj_venc,
                    "e_alvo": (cnpj_alvo in cnpj_venc)
                })
            return resultados
    except: pass
    return None

def main():
    data_inicio = ler_checkpoint()
    if data_inicio.date() > DATA_LIMITE_FINAL.date():
        print("🎯 Ranking atualizado.")
        return

    session = criar_sessao()
    banco_total = carregar_banco()
    
    # Processa exatamente 1 dia conforme solicitado
    data_atual = data_inicio
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"--- 📊 RANKING: Dia {data_atual.strftime('%d/%m/%Y')} ---")

    pagina = 1
    while True:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {"dataInicial": DATA_STR, "dataFinal": DATA_STR, "codigoModalidadeContratacao": "6", "pagina": pagina, "tamanhoPagina": 50}
        
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code != 200: break
        
        data = resp.json().get('data', [])
        if not data: break

        for lic in data:
            cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
            ano, seq = lic.get('anoCompra'), lic.get('sequencialCompra')
            uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
            id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"
            
            # Coleta itens
            todos_itens = []
            url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens"
            p_it = 1
            while True:
                r_it = session.get(url_itens, params={'pagina': p_it, 'tamanhoPagina': 1000}, timeout=20)
                if r_it.status_code != 200: break
                lote = r_it.json()
                if not lote: break
                todos_itens.extend(lote)
                if len(lote) < 1000: break
                p_it += 1

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
                            resumo[r['fornecedor']] = resumo.get(r['fornecedor'], 0) + r['total']

            if itens_ranking:
                banco_total[id_lic] = {
                    "id_licitacao": id_lic,
                    "orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                    "edital": f"{lic.get('numeroCompra')}/{ano}",
                    "uf": lic.get('unidadeOrgao', {}).get('ufSigla'),
                    "itens": itens_ranking,
                    "resumo": resumo,
                    "total_licitacao": sum(resumo.values())
                }
                print("✅", end="", flush=True)

        if pagina >= resp.json().get('totalPaginas', 1): break
        pagina += 1

    # Ao fim do dia, avança 1 dia e salva
    salvar_estado(banco_total, data_atual + timedelta(days=1))

if __name__ == "__main__":
    main()
