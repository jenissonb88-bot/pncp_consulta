import requests
import json
from datetime import datetime, timedelta
import os
import time

# --- CONFIGURAÇÃO ---
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}
ARQ_DADOS = 'dados.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
CNPJ_ALVO = "08778201000126"
DATA_LIMITE_FINAL = datetime(2025, 12, 31)
DIAS_POR_CICLO = 3 

def carregar_banco():
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                dados = json.load(f)
                return {f"{i['Licitacao']}-{i['CNPJ']}": i for i in dados}
        except: pass
    return {}

def salvar_estado(banco, data_proxima):
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, indent=4, ensure_ascii=False)
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(data_proxima.strftime('%Y%m%d'))
    print(f"\n💾 [ESTADO SALVO] Próximo início: {data_proxima.strftime('%d/%m/%Y')}")

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            return datetime.strptime(f.read().strip(), '%Y%m%d')
    return datetime(2025, 1, 1)

# --- INÍCIO ---
data_inicio = ler_checkpoint()
if data_inicio > DATA_LIMITE_FINAL:
    print("🎯 Missão 2025 concluída!")
    exit(0)

data_fim = data_inicio + timedelta(days=DIAS_POR_CICLO - 1)
if data_fim > DATA_LIMITE_FINAL: data_fim = DATA_LIMITE_FINAL

print(f"--- 🚀 SNIPER TURBO (CAPTURA TOTAL DE ITENS) ---")
print(f"--- ALVO: {CNPJ_ALVO} | JANELA: {data_inicio.strftime('%d/%m')} a {data_fim.strftime('%d/%m')} ---")

banco_total = carregar_banco()
data_atual = data_inicio

while data_atual <= data_fim:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 {data_atual.strftime('%d/%m/%Y')}:", end=" ")
    
    pagina = 1
    while True:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR, "dataFinal": DATA_STR, 
            "codigoModalidadeContratacao": "6", "pagina": pagina, 
            "tamanhoPagina": 50, "niFornecedor": CNPJ_ALVO
        }

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code != 200: break
            
            json_resp = resp.json()
            lics = json_resp.get('data', [])
            if not lics: break
            print(f"[{len(lics)} editais]", end="", flush=True)

            for idx, lic in enumerate(lics):
                if idx % 10 == 0 and idx > 0: salvar_estado(banco_total, data_atual)

                cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                ano, seq = lic.get('anoCompra'), lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
                id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"
                num_edital_real = lic.get('numeroCompra')
                link_custom = f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}"

                chave = f"{id_lic}-{CNPJ_ALVO}"

                try:
                    time.sleep(0.1)
                    # MUDANÇA AQUI: Adicionado tamanhoPagina=500 para pegar todos os itens de uma vez
                    url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens?pagina=1&tamanhoPagina=500"
                    r_it = requests.get(url_itens, headers=HEADERS, timeout=15)
                    
                    if r_it.status_code == 200:
                        itens_api = r_it.json()
                        
                        for it in itens_api:
                            if it.get('temResultado'):
                                r_v = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens/{it.get('numeroItem')}/resultados", headers=HEADERS, timeout=10)
                                if r_v.status_code == 200:
                                    vends = r_v.json()
                                    if isinstance(vends, dict): vends = [vends]
                                    
                                    for v in vends:
                                        cv = (v.get('niFornecedor') or "").replace(".", "").replace("/", "").replace("-", "")
                                        if CNPJ_ALVO in cv:
                                            if chave not in banco_total:
                                                banco_total[chave] = {
                                                    "DataResult": lic.get('dataAtualizacao') or DATA_STR,
                                                    "DtInicioPropostas": lic.get('dataInicioRecebimentoPropostas'),
                                                    "DtFimPropostas": lic.get('dataFimRecebimentoPropostas'),
                                                    "IdPNCP": lic.get('idContratacaoPncp'),
                                                    "NumEdital": f"{num_edital_real}/{ano}", 
                                                    "Link": link_custom,
                                                    "UASG": uasg, 
                                                    "Edital": f"{str(seq).zfill(5)}/{ano}",
                                                    "Orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                                                    "UF": lic.get('unidadeOrgao', {}).get('ufSigla'),
                                                    "Municipio": lic.get('unidadeOrgao', {}).get('municipioNome'),
                                                    "Fornecedor": v.get('nomeRazaoSocialFornecedor'), "CNPJ": CNPJ_ALVO, "Licitacao": id_lic, "Itens": []
                                                }
                                            
                                            if not any(x['Item'] == it.get('numeroItem') for x in banco_total[chave]["Itens"]):
                                                banco_total[chave]["Itens"].append({
                                                    "Item": it.get('numeroItem'), "Desc": it.get('descricao'),
                                                    "Qtd": v.get('quantidadeHomologada'), "Unitario": float(v.get('valorUnitarioHomologado') or 0),
                                                    "Total": float(v.get('valorTotalHomologado') or 0), "Status": "Venceu"
                                                })
                                                print("🎯", end="", flush=True)
                except Exception as e:
                    continue
            
            if pagina >= json_resp.get('totalPaginas', 1): break
            pagina += 1
        except Exception as e:
            break
    
    salvar_estado(banco_total, data_atual + timedelta(days=1))
    data_atual += timedelta(days=1)

print(f"\n\n✅ Ciclo concluído com todos os itens capturados.")
