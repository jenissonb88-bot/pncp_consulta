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
# O limite é sempre o momento da execução
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

def buscar_detalhes_compra(cnpj, ano, seq):
    """ Busca as datas de recebimento de proposta (Início e Fim) """
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{str(seq).zfill(6)}"
    try:
        r = requests.get(url, headers=HEADERS, verify=False, timeout=10)
        if r.status_code == 200:
            d = r.json()
            return {
                "inicio": d.get('dataInicioRecebimentoPropostas') or d.get('dataAberturaProposta'),
                "fim": d.get('dataFimRecebimentoPropostas') or d.get('dataEncerramentoProposta'),
                "id_pncp": f"{cnpj}-1-{str(seq).zfill(6)}/{ano}"
            }
    except: return None

# --- PROCESSO ---
banco_total = carregar_banco()
data_atual = datetime(2025, 1, 1)

if os.path.exists(ARQ_CHECKPOINT):
    with open(ARQ_CHECKPOINT, 'r') as f:
        data_atual = datetime.strptime(f.read().strip(), '%Y%m%d')

print(f"🚀 Sniper Global: Capturando resultados homologados até {DATA_LIMITE_FINAL.strftime('%d/%m/%Y')}...")



while data_atual <= DATA_LIMITE_FINAL:
    # IMPORTANTE: A API de resultados usa o parâmetro dataSfi/dataSff para data de inclusão
    data_str = data_atual.strftime('%Y%m%d')
    print(f"\n📅 {data_atual.strftime('%d/%m/%Y')}:", end=" ")
    
    pagina = 1
    while True:
        # Endpoint de RESULTADOS DE ITENS (O mais preciso para capturar vencedores)
        url = f"https://pncp.gov.br/api/pncp/v1/resultados?dataSfi={data_str}&dataSff={data_str}&pagina={pagina}&tamanhoPagina=50"
        
        try:
            r = requests.get(url, headers=HEADERS, verify=False, timeout=25)
            if r.status_code != 200: break
            
            res_json = r.json()
            itens_vencidos = res_json.get('data', [])
            
            if not itens_vencidos: break

            for item in itens_vencidos:
                cnpj_org = item.get('orgaoCnpj')
                ano = item.get('anoCompra')
                seq = item.get('sequencialCompra')
                id_lic = f"{cnpj_org}-{ano}-{seq}"

                # Se a licitação é nova, cria o cabeçalho
                if id_lic not in banco_total:
                    # Double Fetch para pegar datas de proposta e ID formatado
                    detalhes = buscar_detalhes_compra(cnpj_org, ano, seq)
                    
                    banco_total[id_lic] = {
                        "IdPNCP": detalhes['id_pncp'] if detalhes else id_lic,
                        "DataHomologacao": item.get('dataHomologacao'),
                        "DtInicioPropostas": detalhes['inicio'] if detalhes else None,
                        "DtFimPropostas": detalhes['fim'] if detalhes else None,
                        "Orgao": item.get('orgaoRazaoSocial'),
                        "Municipio": item.get('municipioNome'),
                        "UF": item.get('ufSigla'),
                        "Edital": item.get('numeroCompra'),
                        "Link": f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{str(seq).zfill(6)}",
                        "Licitacao": id_lic,
                        "Itens": []
                    }

                # Adiciona o item vencedor à lista (evita duplicar o mesmo item)
                if not any(x['Item'] == item.get('numeroItem') for x in banco_total[id_lic]["Itens"]):
                    banco_total[id_lic]["Itens"].append({
                        "Item": item.get('numeroItem'),
                        "Desc": item.get('descricaoItem'),
                        "Vencedor": item.get('nomeRazaoSocialFornecedor'),
                        "CNPJ_Vencedor": item.get('niFornecedor'),
                        "Qtd": item.get('quantidadeHomologada'),
                        "Total": float(item.get('valorTotalHomologado') or 0)
                    })
                    print("🎯", end="", flush=True)

            if pagina >= res_json.get('totalPaginas', 1): break
            pagina += 1
        except Exception as e:
            print(f" [Aviso: {e}] ", end="")
            break

    salvar_estado(banco_total, data_atual + timedelta(days=1))
    data_atual += timedelta(days=1)
    time.sleep(0.5)

print(f"\n\n✅ Coleta concluída!")
