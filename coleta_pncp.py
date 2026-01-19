import requests
import json
from datetime import datetime, timedelta
import os
import time
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO ---
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}
ARQ_DADOS = 'dados.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
DATA_LIMITE_FINAL = datetime.now() 

def carregar_banco():
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                dados = json.load(f)
                return {i['Licitacao']: i for i in dados}
        except: pass
    return {}

def salvar_estado(banco, data_proxima):
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, indent=4, ensure_ascii=False)
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(data_proxima.strftime('%Y%m%d'))
    print(f"\n💾 Checkpoint: {data_proxima.strftime('%d/%m/%Y')} | Banco: {len(banco)} registros")

def buscar_itens_vencidos(cnpj, ano, seq):
    """ Busca os itens e quem venceu cada um dentro da licitação """
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{str(seq).zfill(6)}/itens?pagina=1&tamanhoPagina=100"
    vencedores = []
    try:
        r = requests.get(url, headers=HEADERS, verify=False, timeout=15)
        if r.status_code == 200:
            itens = r.json()
            for it in itens:
                if it.get('temResultado'):
                    # Busca o resultado específico do item
                    num_item = it.get('numeroItem')
                    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{str(seq).zfill(6)}/itens/{num_item}/resultados"
                    r_res = requests.get(url_res, headers=HEADERS, verify=False, timeout=10)
                    if r_res.status_code == 200:
                        res_data = r_res.json()
                        # Garante que tratamos como lista (API pode retornar objeto único)
                        if isinstance(res_data, dict): res_data = [res_data]
                        for res in res_data:
                            vencedores.append({
                                "Item": num_item,
                                "Desc": it.get('descricao'),
                                "Vencedor": res.get('nomeRazaoSocialFornecedor'),
                                "CNPJ_Vencedor": res.get('niFornecedor'),
                                "Total": float(res.get('valorTotalHomologado') or 0),
                                "DataHomologacao": res.get('dataHomologacao')
                            })
        return vencedores
    except: return []

# --- PROCESSO ---
banco_total = carregar_banco()
data_atual = datetime(2025, 1, 1)

if os.path.exists(ARQ_CHECKPOINT):
    with open(ARQ_CHECKPOINT, 'r') as f:
        data_atual = datetime.strptime(f.read().strip(), '%Y%m%d')

print(f"🚀 Sniper Global 2026: Buscando contratações...")



while data_atual <= DATA_LIMITE_FINAL:
    # Formato AAAA-MM-DD é mais aceito no endpoint de busca atual
    data_formatada = data_atual.strftime('%Y-%m-%d')
    print(f"\n📅 {data_atual.strftime('%d/%m/%Y')}:", end=" ")
    
    pagina = 1
    while True:
        # Usando o endpoint de consulta por data de publicação/atualização
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": data_formatada,
            "dataFinal": data_formatada,
            "pagina": pagina,
            "tamanhoPagina": 50
        }
        
        try:
            r = requests.get(url, params=params, headers=HEADERS, verify=False, timeout=20)
            if r.status_code != 200: break
            
            data_json = r.json()
            contratacoes = data_json.get('data', [])
            if not contratacoes: break

            for c in contratacoes:
                cnpj = c.get('orgaoEntidade', {}).get('cnpj')
                ano = c.get('anoCompra')
                seq = c.get('sequencialCompra')
                id_lic = f"{cnpj}-{ano}-{seq}"

                if id_lic not in banco_total:
                    # Busca itens e vencedores
                    itens = buscar_itens_vencidos(cnpj, ano, seq)
                    if itens:
                        banco_total[id_lic] = {
                            "IdPNCP": f"{cnpj}-1-{str(seq).zfill(6)}/{ano}",
                            "Orgao": c.get('orgaoEntidade', {}).get('razaoSocial'),
                            "Municipio": c.get('unidadeOrgao', {}).get('municipioNome'),
                            "UF": c.get('unidadeOrgao', {}).get('ufSigla'),
                            "Link": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
                            "Licitacao": id_lic,
                            "Itens": itens,
                            "DtInicioPropostas": c.get('dataAberturaProposta')
                        }
                        print("🎯", end="", flush=True)

            if pagina >= data_json.get('totalPaginas', 1): break
            pagina += 1
            time.sleep(0.2)
        except: break

    salvar_estado(banco_total, data_atual + timedelta(days=1))
    data_atual += timedelta(days=1)
    time.sleep(1)
