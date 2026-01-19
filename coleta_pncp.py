import requests
import json
from datetime import datetime, timedelta
import os
import time
import urllib3

# Desativa avisos de SSL para evitar erros de certificado no GitHub Actions/Ambiente local
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO ---
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}
ARQ_DADOS = 'dados.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
DATA_LIMITE_FINAL = datetime(2025, 12, 31)

def carregar_banco():
    """Carrega o JSON existente e cria um índice por ID de Licitação"""
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                dados = json.load(f)
                # Chave única por licitação para evitar duplicados
                return {i['Licitacao']: i for i in dados}
        except: pass
    return {}

def salvar_estado(banco, data_proxima):
    """Salva os dados coletados e atualiza o checkpoint"""
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

print(f"🚀 Sniper PNCP 2025: Captura Global Iniciada...")



while data_atual <= DATA_LIMITE_FINAL:
    data_str = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Data: {data_atual.strftime('%d/%m/%Y')}", end=" ", flush=True)
    
    pagina = 1
    while True:
        # Endpoint de resultados GERAIS (sem filtro de CNPJ de fornecedor)
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
                    # ID de controle interno
                    id_lic = f"{cnpj_org}-{ano}-{seq}"

                    if id_lic not in banco_total:
                        # Segunda consulta para enriquecer os dados (Datas e ID oficial)
                        detalhes = buscar_detalhes_edital(cnpj_org, ano, seq)
                        
                        banco_total[id_lic] = {
                            "IdPNCP": detalhes['id_oficial'] if detalhes else id_lic,
                            "DataHomologacao": it.get('dataHomologacao'), # Extraído do HTML (Data do resultado)
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

                    # Adiciona os itens e os respetivos fornecedores vencedores
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
    
    # Salva o estado ao final de cada dia processado
    salvar_estado(banco_total, data_atual + timedelta(days=1))
    data_atual += timedelta(days=1)
    time.sleep(1) # Pausa amigável para evitar bloqueios

print(f"\n\n✅ Processamento concluído!")
