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

ARQ_DADOS = 'dados_pncp.json'
ARQ_CHECKPOINT = 'checkpoint.txt'

# CNPJ para filtrar a BUSCA de processos (Licitações onde este CNPJ participou ou tem interesse)
CNPJ_ALVO = "08778201000126" 

# ✅ AJUSTE: O limite é a data e hora exata de AGORA.
# O script vai parar assim que processar o dia de hoje.
DATA_LIMITE_FINAL = datetime.now()

# -------------------------------------------------
# UTILITÁRIOS
# -------------------------------------------------
def carregar_banco():
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                dados = json.load(f)
                banco = {}
                for lic in dados:
                    chave = f"{lic.get('id_licitacao', '')}" 
                    banco[chave] = lic
                return banco
        except Exception as e:
            print(f"❌ Erro carregando {ARQ_DADOS}: {e}")
    return {}

def salvar_estado(banco, data_proxima):
    """Salva o banco de dados e atualiza o checkpoint para o próximo dia."""
    try:
        with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, indent=2, ensure_ascii=False)
        with open(ARQ_CHECKPOINT, 'w') as f:
            f.write(data_proxima.strftime('%Y%m%d'))
        print(f"💾 [SALVO] Checkpoint atualizado para: {data_proxima.strftime('%d/%m/%Y')}")
    except Exception as e:
        print(f"❌ Erro salvando estado: {e}")

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                return datetime.strptime(f.read().strip(), '%Y%m%d')
        except:
            pass
    # Se não houver checkpoint, começa em 01/01/2025
    return datetime(2025, 1, 1)

def merge_itens(itens_existentes, novos_itens):
    """Mescla itens evitando duplicatas."""
    mapa = {}
    for item in itens_existentes:
        key = item['numero_item']
        mapa[key] = item
    
    for novo in novos_itens:
        key = novo['numero_item']
        if key in mapa:
            mapa[key].update(novo)
        else:
            mapa[key] = novo
    
    return sorted(list(mapa.values()), key=lambda x: x['numero_item'])

def calcular_totais(licitacao):
    """Calcula totais e formata campos para o HTML."""
    itens = licitacao.get('itens_todos_fornecedores', [])
    
    valor_total = sum(item.get('valor_total_item', 0) for item in itens)
    licitacao['ValorTotal'] = valor_total
    
    # Define a data do resultado baseada na homologação mais recente dos itens
    datas_itens = [i.get('data_homologacao') for i in itens if i.get('data_homologacao')]
    if datas_itens:
        datas_itens.sort(reverse=True)
        licitacao['data_resultado'] = datas_itens[0]
        licitacao['DataResult'] = datas_itens[0]
    else:
        licitacao['DataResult'] = licitacao.get('data_resultado')

    licitacao['Orgao'] = licitacao.get('orgao_nome', 'N/D')
    licitacao['NumEdital'] = licitacao.get('numero_pregao', 'N/D')
    licitacao['Municipio'] = licitacao.get('cidade', 'N/D')
    
    return licitacao

# -------------------------------------------------
# LOOP PRINCIPAL (AUTOMÁTICO ATÉ A DATA ATUAL)
# -------------------------------------------------
print("🚀 Iniciando Varredura Automática Contínua PNCP...")
data_atual = ler_checkpoint()

# Normaliza para comparar apenas as datas (ignorando horas para evitar bugs de loop)
if data_atual.date() > DATA_LIMITE_FINAL.date():
    print(f"✅ O sistema já processou até {DATA_LIMITE_FINAL.strftime('%d/%m/%Y')}. Nada a fazer hoje.")
    exit(0)

banco_total = carregar_banco()
print(f"📊 Banco carregado: {len(banco_total)} registros existentes.")
print(f"📅 Checkpoint: {data_atual.strftime('%d/%m/%Y')} | Alvo: {DATA_LIMITE_FINAL.strftime('%d/%m/%Y')}")

# Loop contínuo até a data de HOJE
while data_atual.date() <= DATA_LIMITE_FINAL.date():
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n---------------------------------------------------")
    print(f"🔎 Processando dia: {data_atual.strftime('%d/%m/%Y')}")

    pagina = 1
    total_pregoes_dia = 0

    while True:
        # Busca Processos do dia específico (Intervalo de 1 dia)
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR, 
            "dataFinal": DATA_STR,
            "codigoModalidadeContratacao": "6", # Pregão
            "pagina": pagina,
            "tamanhoPagina": 50,
            "niFornecedor": CNPJ_ALVO 
        }

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code != 200: 
                print(f"   [Erro HTTP {resp.status_code}]")
                break
            
            json_resp = resp.json()
            lics = json_resp.get('data', [])
            
            if not lics and pagina == 1: 
                print("   [Nenhum pregão encontrado neste dia]")
                break
            elif not lics:
                break # Fim das páginas

            total_pregoes_dia += len(lics)
            print(f"   > Página {pagina}: {len(lics)} editais encontrados.")

            for lic in lics:
                # Extração de dados básicos
                cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                ano = lic.get('anoCompra')
                seq = lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
                id_licitacao = f"{uasg}{str(seq).zfill(5)}{ano}"
                num_edital_real = lic.get('numeroCompra')
                
                print(f"     Processing {num_edital_real}/{ano}... ", end="")

                # --- BUSCA ITENS ---
                try:
                    url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens"
                    # Tamanho 5000 para trazer TUDO de uma vez
                    r_it = requests.get(url_itens, params={'pagina':1, 'tamanhoPagina': 5000}, headers=HEADERS, timeout=20)
                    
                    if r_it.status_code != 200: 
                        print("Erro Itens")
                        continue

                    itens_api = r_it.json()
                    if not itens_api: 
                        print("Sem itens")
                        continue

                    # Inicializa objeto no banco se novo
                    if id_licitacao not in banco_total:
                        banco_total[id_licitacao] = {
                            "id_licitacao": id_licitacao,
                            "orgao_nome": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                            "uasg": uasg,
                            "numero_pregao": f"{num_edital_real}/{ano}",
                            "data_resultado": lic.get('dataAtualizacao'),
                            "cidade": lic.get('unidadeOrgao', {}).get('municipioNome'),
                            "uf": lic.get('unidadeOrgao', {}).get('ufSigla'),
                            "link_edital": f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}",
                            "itens_todos_fornecedores": [],
                            "resumo_fornecedores": {}
                        }

                    itens_coletados = []
                    resumo_fornecedores = {}

                    # --- DETALHE DE CADA ITEM ---
                    # Para economizar tempo, filtramos apenas itens que TEM resultado
                    itens_com_resultado = [i for i in itens_api if i.get('temResultado')]

                    if not itens_com_resultado:
                        print("S/ Resultados")
                        continue

                    for it in itens_com_resultado:
                        num_item = it.get('numeroItem')
                        r_res = requests.get(f"{url_itens}/{num_item}/resultados", headers=HEADERS, timeout=10)
                        
                        if r_res.status_code == 200:
                            resultados = r_res.json()
                            if isinstance(resultados, dict): resultados = [resultados]
                            
                            for res in resultados:
                                cnpj_venc = res.get('niFornecedor')
                                nome_venc = res.get('nomeRazaoSocialFornecedor')
                                data_homolog = res.get('dataHomologacao') or res.get('dataResultado') or res.get('dataInclusao')
                                
                                item_formatado = {
                                    "numero_item": num_item,
                                    "descricao": it.get('descricao', ''),
                                    "quantidade": res.get('quantidadeHomologada', 0),
                                    "valor_unitario": float(res.get('valorUnitarioHomologado') or 0),
                                    "valor_total_item": float(res.get('valorTotalHomologado') or 0),
                                    "cnpj_fornecedor": cnpj_venc,
                                    "nome_fornecedor": nome_venc,
                                    "data_homologacao": data_homolog,
                                    "vencedor_e_alvo": (str(cnpj_venc) in CNPJ_ALVO)
                                }
                                itens_coletados.append(item_formatado)
                                
                                if nome_venc:
                                    if nome_venc not in resumo_fornecedores: resumo_fornecedores[nome_venc] = 0
                                    resumo_fornecedores[nome_venc] += item_formatado['valor_total_item']

                    # Atualiza Banco
                    banco_total[id_licitacao]['itens_todos_fornecedores'] = merge_itens(
                        banco_total[id_licitacao].get('itens_todos_fornecedores', []), itens_coletados
                    )
                    banco_total[id_licitacao]['resumo_fornecedores'] = resumo_fornecedores
                    banco_total[id_licitacao] = calcular_totais(banco_total[id_licitacao])
                    
                    print(f"OK ({len(itens_coletados)} itens)")

                except Exception as e:
                    print(f"Erro processamento: {str(e)[:15]}")

            # Verifica paginação do dia
            if pagina >= json_resp.get('totalPaginas', 1): break
            pagina += 1

        except Exception as e:
            print(f"   [Erro Geral na Requisição: {str(e)[:20]}]")
            time.sleep(5)
            # Tenta continuar para o próximo dia em caso de erro fatal na API
            break

    # === FIM DO DIA: SALVA TUDO E AVANÇA ===
    # Avança para o dia seguinte
    data_proxima = data_atual + timedelta(days=1)
    salvar_estado(banco_total, data_proxima)
    data_atual = data_proxima
    
    # Pequena pausa para não sobrecarregar o servidor
    time.sleep(1) 

print("\n🎉 Varredura completa até HOJE!")
