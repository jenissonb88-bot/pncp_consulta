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

# Termos que garantem o retorno de dados e filtram o seu nicho
TERMOS_BUSCA = ["medicamento", "hospitalar", "fralda", "alcool", "clorexidina", "seringa", "luva", "gaze"]

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

def buscar_detalhes_e_itens(cnpj, ano, seq):
    """ Busca Objeto, Datas de Proposta e captura até 2000 itens por licitação """
    info = {"objeto": "", "inicio": None, "fim": None, "itens": []}
    
    # 1. Consulta o Cabeçalho para as Datas de Proposta e Objeto
    url_cab = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{str(seq).zfill(6)}"
    try:
        r_cab = requests.get(url_cab, headers=HEADERS, verify=False, timeout=15)
        if r_cab.status_code == 200:
            d = r_cab.json()
            info["objeto"] = d.get('objeto')
            info["inicio"] = d.get('dataInicioRecebimentoPropostas')
            info["fim"] = d.get('dataFimRecebimentoPropostas')
    except: pass

    # 2. Consulta a lista de Itens com tamanhoPagina=2000
    url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{str(seq).zfill(6)}/itens?pagina=1&tamanhoPagina=2000"
    try:
        r_it = requests.get(url_itens, headers=HEADERS, verify=False, timeout=20)
        if r_it.status_code == 200:
            itens_json = r_it.json()
            for it in itens_json:
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

                # 3. Busca resultado do item (Homologação, Deserto ou Fracassado)
                if it.get('temResultado'):
                    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{str(seq).zfill(6)}/itens/{num_item}/resultados"
                    r_v = requests.get(url_res, headers=HEADERS, verify=False, timeout=10)
                    if r_v.status_code == 200:
                        vends = r_v.json()
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

# --- INÍCIO ---
banco_total = carregar_banco()
data_atual = datetime(2025, 1, 1)

if os.path.exists(ARQ_CHECKPOINT):
    with open(ARQ_CHECKPOINT, 'r') as f:
        data_atual = datetime.strptime(f.read().strip(), '%Y%m%d')

print(f"🚀 Sniper 2000 Itens: Buscando histórico e atualizações...")



while data_atual <= DATA_LIMITE_FINAL:
    # A API de publicação exige o formato YYYY-MM-DD
    data_format = data_atual.strftime('%Y-%m-%d')
    print(f"\n📅 {data_atual.strftime('%d/%m/%Y')}:", end=" ", flush=True)
    
    for termo in TERMOS_BUSCA:
        pagina = 1
        while True:
            url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
            params = {"dataInicial": data_format, "dataFinal": data_format, "termo": termo, "pagina": pagina, "tamanhoPagina": 50}
            
            try:
                resp = requests.get(url, params=params, headers=HEADERS, verify=False, timeout=30)
                if resp.status_code != 200: break
                
                compras = resp.json().get('data', [])
                if not compras: break

                for c in compras:
                    cnpj = c.get('orgaoEntidade', {}).get('cnpj')
                    ano, seq = c.get('anoCompra'), c.get('sequencialCompra')
                    id_lic = f"{cnpj}-{ano}-{seq}"

                    if id_lic not in banco_total:
                        # Processamento detalhado da licitação
                        detalhes = buscar_detalhes_e_itens(cnpj, ano, seq)
                        
                        banco_total[id_lic] = {
                            "IdPNCP": f"{cnpj}-1-{str(seq).zfill(6)}/{ano}",
                            "Orgao": c.get('orgaoEntidade', {}).get('razaoSocial'),
                            "Municipio": c.get('unidadeOrgao', {}).get('municipioNome'),
                            "UF": c.get('unidadeOrgao', {}).get('ufSigla'),
                            "Objeto": detalhes["objeto"],
                            "DtInicioPropostas": detalhes["inicio"],
                            "DtFimPropostas": detalhes["fim"],
                            "Link": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
                            "Licitacao": id_lic,
                            "Itens": detalhes["itens"]
                        }
                        print("🎯", end="", flush=True)

                if pagina >= resp.json().get('totalPaginas', 1): break
                pagina += 1
            except: break

    salvar_estado(banco_total, data_atual + timedelta(days=1))
    data_atual += timedelta(days=1)
    time.sleep(1)

print(f"\n\n✅ Coleta concluída até hoje.")
