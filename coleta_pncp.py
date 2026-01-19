import requests
import json
from datetime import datetime, timedelta
import os
import time
import urllib3

# Desativa avisos de SSL para garantir rodagem no GitHub Actions
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO ---
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}
ARQ_DADOS = 'dados.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
# Captura até o minuto atual da execução
DATA_LIMITE_FINAL = datetime.now() 

# Filtros para garantir que a API não bloqueie a busca global
TERMOS_BUSCA = [
    "medicamento", "material medico", "insumo hospitalar", 
    "fralda", "absorvente", "alcool", "clorexidina", "seringa"
]

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

def processar_itens(cnpj, ano, seq):
    """ Varre todos os itens para identificar Homologados, Desertos ou Fracassados """
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{str(seq).zfill(6)}/itens?pagina=1&tamanhoPagina=500"
    itens_lista = []
    try:
        r = requests.get(url, headers=HEADERS, verify=False, timeout=15)
        if r.status_code == 200:
            itens_json = r.json()
            for it in itens_json:
                item_data = {
                    "Item": it.get('numeroItem'),
                    "Desc": it.get('descricao'),
                    "Status": "Divulgado",
                    "Vencedor": None,
                    "Valor": float(it.get('valorUnitarioEstimado') or 0)
                }
                
                if it.get('temResultado'):
                    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{str(seq).zfill(6)}/itens/{it.get('numeroItem')}/resultados"
                    r_res = requests.get(url_res, headers=HEADERS, verify=False, timeout=10)
                    if r_res.status_code == 200:
                        resultados = r_res.json()
                        if isinstance(resultados, dict): resultados = [resultados]
                        for res in resultados:
                            item_data["Status"] = res.get('statusNome', 'Homologado')
                            item_data["Vencedor"] = res.get('nomeRazaoSocialFornecedor')
                            item_data["Valor"] = float(res.get('valorUnitarioHomologado') or item_data["Valor"])
                
                itens_lista.append(item_data)
        return itens_lista
    except: return []

# --- INÍCIO DO PROCESSO ---
banco_total = carregar_banco()
data_atual = datetime(2025, 1, 1)

if os.path.exists(ARQ_CHECKPOINT):
    with open(ARQ_CHECKPOINT, 'r') as f:
        data_atual = datetime.strptime(f.read().strip(), '%Y%m%d')

print(f"🚀 Radar Global Ativado | Início: {data_atual.strftime('%d/%m/%Y')} | Limite: Hoje")

while data_atual <= DATA_LIMITE_FINAL:
    data_formatada = data_atual.strftime('%Y-%m-%d')
    print(f"\n📅 {data_atual.strftime('%d/%m/%Y')}:", end=" ", flush=True)
    
    for termo in TERMOS_BUSCA:
        pagina = 1
        while True:
            # Endpoint estável de contratações por termo
            url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
            params = {
                "dataInicial": data_formatada,
                "dataFinal": data_formatada,
                "termo": termo,
                "pagina": pagina,
                "tamanhoPagina": 50
            }
            
            try:
                r = requests.get(url, params=params, headers=HEADERS, verify=False, timeout=20)
                if r.status_code != 200: break
                
                data_json = r.json()
                compras = data_json.get('data', [])
                if not compras: break

                for c in compras:
                    cnpj = c.get('orgaoEntidade', {}).get('cnpj')
                    ano = c.get('anoCompra')
                    seq = c.get('sequencialCompra')
                    id_lic = f"{cnpj}-{ano}-{seq}"

                    if id_lic not in banco_total:
                        itens = processar_itens(cnpj, ano, seq)
                        
                        # Inteligência de Status Geral
                        status_geral = c.get('situacaoNome', 'Divulgada')
                        if any(i['Status'] == 'Homologado' for i in itens): status_geral = "Homologada"
                        elif all(i['Status'] == 'Deserto' for i in itens) and itens: status_geral = "Deserta"
                        elif all(i['Status'] == 'Fracassado' for i in itens) and itens: status_geral = "Fracassada"

                        banco_total[id_lic] = {
                            "IdPNCP": f"{cnpj}-1-{str(seq).zfill(6)}/{ano}",
                            "Status": status_geral,
                            "Orgao": c.get('orgaoEntidade', {}).get('razaoSocial'),
                            "Municipio": c.get('unidadeOrgao', {}).get('municipioNome'),
                            "UF": c.get('unidadeOrgao', {}).get('ufSigla'),
                            "Objeto": c.get('objeto'),
                            "DtInicioPropostas": c.get('dataAberturaProposta'),
                            "DtFimPropostas": c.get('dataEncerramentoProposta'),
                            "Link": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
                            "Licitacao": id_lic,
                            "Itens": itens
                        }
                        print("🎯", end="", flush=True)

                if pagina >= data_json.get('totalPaginas', 1): break
                pagina += 1
            except: break

    salvar_estado(banco_total, data_atual + timedelta(days=1))
    data_atual += timedelta(days=1)
    time.sleep(1)

print(f"\n\n✅ Ciclo concluído com sucesso.")
