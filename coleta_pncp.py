import requests
import json
from datetime import datetime, timedelta
import os
import time
import urllib3

# Desativa avisos de SSL para evitar erros de certificado
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO ---
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}
ARQ_DADOS = 'dados.json'
ARQ_CHECKPOINT = 'checkpoint.txt'

# AJUSTE: O limite agora é sempre o dia atual (hoje)
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

def buscar_detalhes_edital(cnpj, ano, sequencial):
    """
    Consulta o endpoint de COMPRAS para obter as datas de proposta.
    O endpoint de RESULTADOS não fornece estes dados.
    """
    seq_6 = str(sequencial).zfill(6)
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq_6}"
    try:
        r = requests.get(url, headers=HEADERS, verify=False, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return {
                "id_oficial": f"{cnpj}-1-{seq_6}/{ano}",
                "inicio": data.get('dataInicioRecebimentoPropostas') or data.get('dataAberturaProposta'),
                "fim": data.get('dataFimRecebimentoPropostas') or data.get('dataEncerramentoProposta')
            }
    except: pass
    return None

# --- INÍCIO DO PROCESSO ---
banco_total = carregar_banco()
data_atual = datetime(2025, 1, 1)

if os.path.exists(ARQ_CHECKPOINT):
    with open(ARQ_CHECKPOINT, 'r') as f:
        data_atual = datetime.strptime(f.read().strip(), '%Y%m%d')

print(f"🚀 Sniper PNCP: Captura Global Iniciada (Limite: {DATA_LIMITE_FINAL.strftime('%d/%m/%Y')})")



while data_atual <= DATA_LIMITE_FINAL:
    data_str = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Data: {data_atual.strftime('%d/%m/%Y')}", end=" ", flush=True)
    
    pagina = 1
    while True:
        # Consulta geral de resultados
        url_res = f"https://pncp.gov.br/api/pncp/v1/resultados?dataSfi={data_str}&dataSff={data_str}&pagina={pagina}&tamanhoPagina=50"
        
        try:
            resp = requests.get(url_res, headers=HEADERS, verify=False, timeout=15)
            if resp.status_code != 200: break
            
            json_resp = resp.json()
            itens = json_resp.get('data', [])
            if not itens: break

            for it in itens:
                try:
                    cnpj_org = it.get('orgaoCnpj')
                    ano = it.get('anoCompra')
                    seq = it.get('sequencialCompra')
                    id_lic = f"{cnpj_org}-{ano}-{seq}"

                    if id_lic not in banco_total:
                        # Double Fetch para pegar datas de proposta e ID PNCP oficial
                        detalhes = buscar_detalhes_edital(cnpj_org, ano, seq)
                        
                        banco_total[id_lic] = {
                            "IdPNCP": detalhes['id_oficial'] if detalhes else id_lic,
                            "DataHomologacao": it.get('dataHomologacao'), # Campo extraído do HTML que você enviou
                            "DtInicioPropostas": detalhes['inicio'] if detalhes else None,
                            "DtFimPropostas": detalhes['fim'] if detalhes else None,
                            "Orgao": it.get('orgaoRazaoSocial'),
                            "Municipio": it.get('municipioNome'),
                            "UF": it.get('ufSigla'),
                            "Edital": it.get('numeroCompra'),
                            "Link": f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{str(seq).zfill(6)}",
                            "Licitacao": id_lic,
                            "Itens": []
                        }

                    # Registra o item e o vencedor (mesmo que a licitação já exista, pode ter novos itens)
                    banco_total[id_lic]["Itens"].append({
                        "Item": it.get('numeroItem'),
                        "Desc": it.get('descricaoItem'),
                        "Vencedor": it.get('nomeRazaoSocialFornecedor'),
                        "CNPJ_Vencedor": it.get('niFornecedor'),
                        "Qtd": it.get('quantidadeHomologada'),
                        "Unitario": float(it.get('valorUnitarioHomologado') or 0),
                        "Total": float(it.get('valorTotalHomologado') or 0)
                    })
                    print("🎯", end="", flush=True)
                except: continue
            
            if pagina >= json_resp.get('totalPaginas', 1): break
            pagina += 1
        except: break
    
    # Salva o estado e avança o dia
    salvar_estado(banco_total, data_atual + timedelta(days=1))
    data_atual += timedelta(days=1)
    
    # Pequena pausa para respeitar o servidor do governo
    time.sleep(0.5)

print(f"\n\n✅ Coleta atualizada até hoje!")
