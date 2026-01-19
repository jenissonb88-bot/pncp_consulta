import requests
import json
from datetime import datetime, timedelta
import os
import time
import urllib3

# Desativa avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO ---
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}
ARQ_DADOS = 'dados.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
DATA_LIMITE_FINAL = datetime.now() 

# Termos que garantem o retorno de dados da API global
TERMOS_BUSCA = ["medicamento", "hospitalar", "fralda", "alcool", "clorexidina", "seringa", "luva"]

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
    print(f"\n💾 Checkpoint: {data_proxima.strftime('%d/%m/%Y')} | Banco: {len(banco)} licitações")

def buscar_detalhes_e_itens(cnpj_org, ano, seq):
    """ 
    Busca informações detalhadas da compra (Datas e Objeto) 
    e percorre os itens para identificar resultados.
    """
    detalhes = {"inicio": None, "fim": None, "objeto": "", "itens": []}
    
    # 1. Consulta o cabeçalho para pegar datas de proposta e objeto
    url_cabecalho = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{str(seq).zfill(6)}"
    try:
        r_cab = requests.get(url_cabecalho, headers=HEADERS, verify=False, timeout=15)
        if r_cab.status_code == 200:
            dados_cab = r_cab.json()
            detalhes["inicio"] = dados_cab.get('dataInicioRecebimentoPropostas')
            detalhes["fim"] = dados_cab.get('dataFimRecebimentoPropostas')
            detalhes["objeto"] = dados_cab.get('objeto')
    except: pass

    # 2. Consulta a lista de itens
    url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{str(seq).zfill(6)}/itens?pagina=1&tamanhoPagina=500"
    try:
        r_it = requests.get(url_itens, headers=HEADERS, verify=False, timeout=15)
        if r_it.status_code == 200:
            itens_api = r_it.json()
            for it in itens_api:
                num_item = it.get('numeroItem')
                item_data = {
                    "Item": num_item,
                    "Desc": it.get('descricao'),
                    "Status": "Divulgado", # Default
                    "Vencedor": None,
                    "DataHomologacao": None,
                    "Total": float(it.get('valorTotalEstimado') or 0)
                }

                # 3. Se o item tem resultado, busca os detalhes (Homologação, Deserto, Fracassado)
                if it.get('temResultado'):
                    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{str(seq).zfill(6)}/itens/{num_item}/resultados"
                    r_v = requests.get(url_res, headers=HEADERS, verify=False, timeout=10)
                    if r_v.status_code == 200:
                        vends = r_v.json()
                        if isinstance(vends, dict): vends = [vends]
                        for v in vends:
                            # Captura a data de homologação exata do item
                            item_data["Status"] = v.get('statusNome', 'Homologado')
                            item_data["Vencedor"] = v.get('nomeRazaoSocialFornecedor')
                            item_data["DataHomologacao"] = v.get('dataHomologacao')
                            item_data["Total"] = float(v.get('valorTotalHomologado') or item_data["Total"])
                
                detalhes["itens"].append(item_data)
    except: pass
    return detalhes

# --- INÍCIO ---
banco_total = carregar_banco()
data_atual = datetime(2025, 1, 1)

if os.path.exists(ARQ_CHECKPOINT):
    try:
        with open(ARQ_CHECKPOINT, 'r') as f:
            data_atual = datetime.strptime(f.read().strip(), '%Y%m%d')
    except: pass

print(f"🚀 Iniciando Radar Saúde PNCP (Busca Dinâmica até Hoje)")

while data_atual <= DATA_LIMITE_FINAL:
    DATA_STR = data_atual.strftime('%Y%m%d')
    # O endpoint de consulta por publicação exige formato YYYY-MM-DD
    DATA_FORMAT = data_atual.strftime('%Y-%m-%d')
    print(f"\n📅 {data_atual.strftime('%d/%m/%Y')}:", end=" ", flush=True)
    
    for termo in TERMOS_BUSCA:
        pagina = 1
        while True:
            url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
            params = {
                "dataInicial": DATA_FORMAT, "dataFinal": DATA_FORMAT, 
                "termo": termo, "pagina": pagina, "tamanhoPagina": 50
            }

            try:
                resp = requests.get(url, params=params, headers=HEADERS, verify=False, timeout=30)
                if resp.status_code != 200: break
                
                json_resp = resp.json()
                lics = json_resp.get('data', [])
                if not lics: break

                for lic in lics:
                    cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                    ano, seq = lic.get('anoCompra'), lic.get('sequencialCompra')
                    id_lic = f"{cnpj_org}-{ano}-{seq}"

                    if id_lic not in banco_total:
                        # Busca informações detalhadas (Datas, Objeto e Itens)
                        info = buscar_detalhes_e_itens(cnpj_org, ano, seq)
                        
                        banco_total[id_lic] = {
                            "IdPNCP": f"{cnpj_org}-1-{str(seq).zfill(6)}/{ano}",
                            "Orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                            "Municipio": lic.get('unidadeOrgao', {}).get('municipioNome'),
                            "UF": lic.get('unidadeOrgao', {}).get('ufSigla'),
                            "Objeto": info["objeto"],
                            "DtInicioPropostas": info["inicio"],
                            "DtFimPropostas": info["fim"],
                            "Link": f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}",
                            "Licitacao": id_lic,
                            "Itens": info["itens"]
                        }
                        print("🎯", end="", flush=True)
                
                if pagina >= json_resp.get('totalPaginas', 1): break
                pagina += 1
            except: break
    
    salvar_estado(banco_total, data_atual + timedelta(days=1))
    data_atual += timedelta(days=1)
    time.sleep(1)

print(f"\n\n✅ Processamento concluído.")
