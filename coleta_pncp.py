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

def buscar_vencedores_itens(cnpj, ano, seq):
    """ Busca os itens e seus respectivos vencedores dentro da compra """
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{str(seq).zfill(6)}/itens?pagina=1&tamanhoPagina=100"
    lista_itens = []
    try:
        r = requests.get(url, headers=HEADERS, verify=False, timeout=15)
        if r.status_code == 200:
            itens_json = r.json()
            for it in itens_json:
                if it.get('temResultado'):
                    num_item = it.get('numeroItem')
                    # Busca o resultado do item específico
                    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{str(seq).zfill(6)}/itens/{num_item}/resultados"
                    r_res = requests.get(url_res, headers=HEADERS, verify=False, timeout=10)
                    if r_res.status_code == 200:
                        res_data = r_res.json()
                        if isinstance(res_data, dict): res_data = [res_data]
                        for res in res_data:
                            lista_itens.append({
                                "Item": num_item,
                                "Desc": it.get('descricao'),
                                "Vencedor": res.get('nomeRazaoSocialFornecedor'),
                                "CNPJ_Vencedor": res.get('niFornecedor'),
                                "Qtd": res.get('quantidadeHomologada'),
                                "Total": float(res.get('valorTotalHomologado') or 0),
                                "DataHomologacao": res.get('dataHomologacao')
                            })
        return lista_itens
    except: return []

# --- PROCESSO PRINCIPAL ---
banco_total = carregar_banco()
data_atual = datetime(2025, 1, 1)

if os.path.exists(ARQ_CHECKPOINT):
    with open(ARQ_CHECKPOINT, 'r') as f:
        data_atual = datetime.strptime(f.read().strip(), '%Y%m%d')

print(f"🚀 Sniper Global: Iniciando busca por Contratações (Limite: {DATA_LIMITE_FINAL.strftime('%d/%m/%Y')})")

while data_atual <= DATA_LIMITE_FINAL:
    # Usamos o formato YYYY-MM-DD que é o padrão da API de consulta
    data_formatada = data_atual.strftime('%Y-%m-%d')
    print(f"\n📅 {data_atual.strftime('%d/%m/%Y')}:", end=" ")
    
    pagina = 1
    while True:
        # Endpoint de consulta de contratações (mais estável que o de resultados direto)
        url_busca = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": data_formatada,
            "dataFinal": data_formatada,
            "pagina": pagina,
            "tamanhoPagina": 50
        }
        
        try:
            r = requests.get(url_busca, params=params, headers=HEADERS, verify=False, timeout=20)
            if r.status_code != 200: break
            
            data_json = r.json()
            contratacoes = data_json.get('data', [])
            if not contratacoes: break

            for c in contratacoes:
                cnpj_org = c.get('orgaoEntidade', {}).get('cnpj')
                ano = c.get('anoCompra')
                seq = c.get('sequencialCompra')
                id_lic = f"{cnpj_org}-{ano}-{seq}"

                if id_lic not in banco_total:
                    vencidos = buscar_vencedores_itens(cnpj_org, ano, seq)
                    # Só adicionamos ao banco se houver itens com resultado homologado
                    if vencidos:
                        banco_total[id_lic] = {
                            "IdPNCP": f"{cnpj_org}-1-{str(seq).zfill(6)}/{ano}",
                            "DtInicioPropostas": c.get('dataAberturaProposta'),
                            "DtFimPropostas": c.get('dataEncerramentoProposta'),
                            "Orgao": c.get('orgaoEntidade', {}).get('razaoSocial'),
                            "Municipio": c.get('unidadeOrgao', {}).get('municipioNome'),
                            "UF": c.get('unidadeOrgao', {}).get('ufSigla'),
                            "Edital": c.get('numeroCompra'),
                            "Link": f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}",
                            "Licitacao": id_lic,
                            "Itens": vencidos
                        }
                        print("🎯", end="", flush=True)

            if pagina >= data_json.get('totalPaginas', 1): break
            pagina += 1
        except: break

    salvar_estado(banco_total, data_atual + timedelta(days=1))
    data_atual += timedelta(days=1)
    time.sleep(0.5)

print(f"\n\n✅ Coleta concluída!")
