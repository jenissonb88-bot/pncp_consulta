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
ARQ_DADOS = 'dados_pncp.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
CNPJ_ALVO = "08778201000126"
DATA_LIMITE_MAXIMA = datetime.now() 

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
    print(f"\n💾 Checkpoint salvo: {data_proxima.strftime('%d/%m/%Y')} | Banco: {len(banco)} registros")

def buscar_detalhes_e_itens(cnpj, ano, seq):
    """ Busca Objeto, Datas de Proposta e captura TODOS os itens da licitação """
    info = {"objeto": "", "inicio": None, "fim": None, "itens": []}
    
    # 1. Cabeçalho (Objeto e Prazos)
    url_cab = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{str(seq).zfill(6)}"
    try:
        r = requests.get(url_cab, headers=HEADERS, verify=False, timeout=15)
        if r.status_code == 200:
            d = r.json()
            info["objeto"] = d.get('objeto')
            info["inicio"] = d.get('dataInicioRecebimentoPropostas')
            info["fim"] = d.get('dataFimRecebimentoPropostas')
    except: pass

    # 2. Varredura de Itens (Paginação larga para pegar tudo)
    url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{str(seq).zfill(6)}/itens?pagina=1&tamanhoPagina=5000"
    try:
        ri = requests.get(url_itens, headers=HEADERS, verify=False, timeout=20)
        if ri.status_code == 200:
            for it in ri.json():
                num_item = it.get('numeroItem')
                item_data = {
                    "Item": num_item,
                    "Desc": it.get('descricao'),
                    "Status": "Divulgado",
                    "Vencedor": None,
                    "CNPJ_Vencedor": None,
                    "DataHomologacao": None,
                    "Valor": float(it.get('valorUnitarioEstimado') or 0)
                }
                
                if it.get('temResultado'):
                    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{str(seq).zfill(6)}/itens/{num_item}/resultados"
                    rr = requests.get(url_res, headers=HEADERS, verify=False, timeout=10)
                    if rr.status_code == 200:
                        vends = rr.json()
                        if isinstance(vends, dict): vends = [vends]
                        for v in vends:
                            item_data["Status"] = v.get('statusNome', 'Homologado')
                            item_data["Vencedor"] = v.get('nomeRazaoSocialFornecedor')
                            item_data["CNPJ_Vencedor"] = v.get('niFornecedor')
                            item_data["DataHomologacao"] = v.get('dataHomologacao')
                            item_data["Valor"] = float(v.get('valorTotalHomologado') or item_data["Valor"])
                
                info["itens"].append(item_data)
    except: pass
    return info

# --- PROCESSO ---
banco_total = carregar_banco()
data_atual = datetime(2025, 1, 1)

if os.path.exists(ARQ_CHECKPOINT):
    with open(ARQ_CHECKPOINT, 'r') as f:
        data_atual = datetime.strptime(f.read().strip(), '%Y%m%d')

if data_atual > DATA_LIMITE_MAXIMA:
    print("✅ Sniper atualizado!")
    exit(0)

print(f"🚀 Sniper Alvo (Resultados): {CNPJ_ALVO} | Data: {data_atual.strftime('%d/%m/%Y')}")



# BUSCA POR RESULTADOS (Onde a Drogafonte aparece com mais frequência)
data_str = data_atual.strftime('%Y%m%d')
pagina = 1
while True:
    # Usamos o endpoint de resultados por CNPJ do fornecedor (niFornecedor)
    url = f"https://pncp.gov.br/api/pncp/v1/resultados?dataSfi={data_str}&dataSff={data_str}&niFornecedor={CNPJ_ALVO}&pagina={pagina}&tamanhoPagina=50"
    
    try:
        resp = requests.get(url, headers=HEADERS, verify=False, timeout=30)
        if resp.status_code != 200: break
        
        json_resp = resp.json()
        itens_vencidos = json_resp.get('data', [])
        if not itens_vencidos: break

        for item in itens_vencidos:
            cnpj_org = item.get('orgaoCnpj')
            ano, seq = item.get('anoCompra'), item.get('sequencialCompra')
            id_lic = f"{cnpj_org}-{ano}-{seq}"
            
            if id_lic not in banco_total:
                # Busca cabeçalho e todos os itens da compra
                detalhes = buscar_detalhes_e_itens(cnpj_org, ano, seq)
                
                banco_total[id_lic] = {
                    "IdPNCP": f"{cnpj_org}-1-{str(seq).zfill(6)}/{ano}",
                    "Orgao": item.get('orgaoRazaoSocial'),
                    "Municipio": item.get('municipioNome'),
                    "UF": item.get('ufSigla'),
                    "Objeto": detalhes["objeto"],
                    "Status": "Homologada",
                    "DtInicioPropostas": detalhes["inicio"],
                    "DtFimPropostas": detalhes["fim"],
                    "Link": f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}",
                    "Licitacao": id_lic,
                    "Itens": detalhes["itens"]
                }
                print("🎯", end="", flush=True)

        if pagina >= json_resp.get('totalPaginas', 1): break
        pagina += 1
    except: break

salvar_estado(banco_total, data_atual + timedelta(days=1))
