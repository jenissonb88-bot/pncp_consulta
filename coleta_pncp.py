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
DATA_LIMITE_FINAL = datetime.now() # Atualiza até o dia de hoje

# Filtros para evitar bloqueio e focar no seu interesse
TERMOS = ["medicamento", "hospitalar", "fralda", "alcool", "clorexidina"]

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

def buscar_extras(cnpj, ano, seq):
    """ Busca Objeto e Datas de Proposta que não vêm no resultado geral """
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{str(seq).zfill(6)}"
    try:
        r = requests.get(url, headers=HEADERS, verify=False, timeout=10)
        if r.status_code == 200:
            d = r.json()
            return {
                "objeto": d.get('objeto'),
                "inicio": d.get('dataInicioRecebimentoPropostas'),
                "fim": d.get('dataFimRecebimentoPropostas'),
                "id_oficial": f"{cnpj}-1-{str(seq).zfill(6)}/{ano}"
            }
    except: pass
    return None

# --- INÍCIO ---
banco_total = carregar_banco()
data_atual = datetime(2025, 1, 1)

if os.path.exists(ARQ_CHECKPOINT):
    with open(ARQ_CHECKPOINT, 'r') as f:
        data_atual = datetime.strptime(f.read().strip(), '%Y%m%d')

print(f"🚀 Iniciando Coleta Global de Saúde (Até: {DATA_LIMITE_FINAL.strftime('%d/%m/%Y')})")



while data_atual <= DATA_LIMITE_FINAL:
    data_str = data_atual.strftime('%Y%m%d')
    print(f"\n📅 {data_atual.strftime('%d/%m/%Y')}:", end=" ", flush=True)
    
    for termo in TERMOS:
        pagina = 1
        while True:
            # Usando o endpoint de RESULTADOS que você confirmou que funciona
            url = f"https://pncp.gov.br/api/pncp/v1/resultados?dataSfi={data_str}&dataSff={data_str}&pagina={pagina}&tamanhoPagina=50&termo={termo}"
            
            try:
                resp = requests.get(url, headers=HEADERS, verify=False, timeout=20)
                if resp.status_code != 200: break
                
                itens = resp.json().get('data', [])
                if not itens: break

                for it in itens:
                    cnpj, ano, seq = it.get('orgaoCnpj'), it.get('anoCompra'), it.get('sequencialCompra')
                    id_lic = f"{cnpj}-{ano}-{seq}"

                    if id_lic not in banco_total:
                        extras = buscar_extras(cnpj, ano, seq)
                        banco_total[id_lic] = {
                            "IdPNCP": extras['id_oficial'] if extras else id_lic,
                            "Status": "Homologada",
                            "Orgao": it.get('orgaoRazaoSocial'),
                            "Municipio": it.get('municipioNome'),
                            "UF": it.get('ufSigla'),
                            "Objeto": extras['objeto'] if extras else "Ver edital",
                            "DtInicioPropostas": extras['inicio'] if extras else None,
                            "DtFimPropostas": extras['fim'] if extras else None,
                            "Link": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
                            "Licitacao": id_lic,
                            "Itens": []
                        }

                    # Adiciona o item vencedor e a data de homologação correta dele
                    if not any(x['Item'] == it.get('numeroItem') for x in banco_total[id_lic]["Itens"]):
                        banco_total[id_lic]["Itens"].append({
                            "Item": it.get('numeroItem'),
                            "Desc": it.get('descricaoItem'),
                            "Status": it.get('statusNome') or "Homologado",
                            "DataHomologacao": it.get('dataHomologacao'),
                            "Vencedor": it.get('nomeRazaoSocialFornecedor'),
                            "Total": float(it.get('valorTotalHomologado') or 0)
                        })
                        print("🎯", end="", flush=True)

                if pagina >= resp.json().get('totalPaginas', 1): break
                pagina += 1
            except: break
            
    salvar_estado(banco_total, data_atual + timedelta(days=1))
    data_atual += timedelta(days=1)
    time.sleep(1)
