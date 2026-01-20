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

ARQ_DADOS = 'dados_pncp.json'  # ✅ Compatível com HTML
ARQ_CHECKPOINT = 'checkpoint.txt'
CNPJ_ALVO = "08778201000126"
DATA_LIMITE_FINAL = datetime(2025, 12, 31)
DIAS_POR_CICLO = 3

# -------------------------------------------------
# UTILITÁRIOS DE ESTADO
# -------------------------------------------------
def carregar_banco():
    """Carrega JSON e devolve dict indexado por (id_licitacao-cnpj_fornecedor)."""
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                dados = json.load(f)
                banco = {}
                for lic in dados:
                    chave = f"{lic.get('id_licitacao', '')}-{lic.get('cnpj_fornecedor', '')}"
                    banco[chave] = lic
                return banco
        except Exception as e:
            print(f"❌ Erro carregando {ARQ_DADOS}: {e}")
    return {}

def salvar_estado(banco, data_proxima):
    """Salva JSON consolidado + checkpoint."""
    try:
        with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, indent=2, ensure_ascii=False)
        with open(ARQ_CHECKPOINT, 'w') as f:
            f.write(data_proxima.strftime('%Y%m%d'))
        print(f"\n💾 [ESTADO SALVO] {len(banco)} registros | Próximo: {data_proxima.strftime('%d/%m/%Y')}")
    except Exception as e:
        print(f"❌ Erro salvando estado: {e}")

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                return datetime.strptime(f.read().strip(), '%Y%m%d')
        except:
            pass
    return datetime(2025, 1, 1)

# -------------------------------------------------
# DEDUPLICAÇÃO E ATUALIZAÇÃO
# -------------------------------------------------
def merge_itens(itens_existentes, novos_itens):
    """Mescla itens evitando duplicatas."""
    mapa_existentes = {}
    for item in itens_existentes:
        chave = (item['numero_item'], item.get('cnpj_fornecedor', ''))
        mapa_existentes[chave] = item
    
    for novo_item in novos_itens:
        chave = (novo_item['numero_item'], novo_item.get('cnpj_fornecedor', ''))
        if chave in mapa_existentes:
            mapa_existentes[chave].update(novo_item)
            print("🔄", end="", flush=True)
        else:
            mapa_existentes[chave] = novo_item
            print("✅", end="", flush=True)
    
    return list(mapa_existentes.values())

# -------------------------------------------------
# ✅ CORREÇÃO: Campos compatíveis com HTML
# -------------------------------------------------
def calcular_totais(licitacao):
    """Calcula totais e mapeia campos para Dashboard HTML."""
    itens = licitacao.get('itens', [])
    
    # ✅ VALOR TOTAL (soma itens que vencemos)
    valor_total = sum(item.get('valor_total_item', 0) for item in itens)
    licitacao['ValorTotal'] = valor_total
    
    # ✅ Mapeamento EXATO para HTML
    licitacao['DataResult'] = licitacao.get('data_resultado')
    licitacao['Orgao'] = licitacao.get('orgao_nome', licitacao.get('orgao_codigo', 'N/D'))
    licitacao['NumEdital'] = licitacao.get('numero_pregao', 'N/D')
    licitacao['Municipio'] = licitacao.get('cidade', 'N/D')
    licitacao['Fornecedor'] = f"{licitacao.get('cnpj_fornecedor', '')} - {licitacao.get('orgao_nome', 'PNCP')[:30]}"
    
    # ✅ Array de itens para cards
    licitacao['Itens'] = itens
    
    return licitacao

# -------------------------------------------------
# LOOP PRINCIPAL
# -------------------------------------------------
print("🚀 Iniciando Coleta PNCP Otimizada...")
data_inicio = ler_checkpoint()

if data_inicio > DATA_LIMITE_FINAL:
    print("✅ Missão 2025 concluída!")
    exit(0)

data_fim = data_inicio + timedelta(days=DIAS_POR_CICLO - 1)
if data_fim > DATA_LIMITE_FINAL: 
    data_fim = DATA_LIMITE_FINAL

print(f"🎯 Alvo: {CNPJ_ALVO}")
print(f"📅 Janela: {data_inicio.strftime('%d/%m/%Y')} → {data_fim.strftime('%d/%m/%Y')}")
print("Legenda: ✅ Novo | 🔄 Atualizado | ⚠️ Sem resultado")

banco_total = carregar_banco()
print(f"📊 Carregados: {len(banco_total)} registros iniciais")

data_atual = data_inicio
while data_atual <= data_fim:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 {data_atual.strftime('%d/%m/%Y')}: ", end="")

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
            if resp.status_code != 200:
                print(f"[HTTP {resp.status_code}]")
                break

            json_resp = resp.json()
            lics = json_resp.get('data', [])
            if not lics:
                print("[Sem licitações]")
                break

            print(f"[{len(lics)} editais] ", end="", flush=True)

            for lic in lics:
                cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                ano = lic.get('anoCompra')
                seq = lic.get('sequencialCompra')

                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
                id_licitacao = f"{uasg}{str(seq).zfill(5)}{ano}"
                num_edital_real = lic.get('numeroCompra')
                link_custom = f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}"

                chave = f"{id_licitacao}-{CNPJ_ALVO}"

                try:
                    time.sleep(0.1)
                    r_it = requests.get(
                        f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens",
                        headers=HEADERS, timeout=15
                    )
                    if r_it.status_code != 200:
                        continue

                    itens_api = r_it.json()
                    if not itens_api:
                        continue

                    # Inicializa licitação se não existir
                    if chave not in banco_total:
                        banco_total[chave] = {
                            "id_licitacao": id_licitacao,
                            "cnpj_fornecedor": CNPJ_ALVO,
                            "orgao_codigo": uasg,
                            "orgao_nome": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                            "uasg": uasg,
                            "numero_pregao": f"{num_edital_real}/{ano}" if num_edital_real else f"{str(seq).zfill(5)}/{ano}",
                            "id_pncp": lic.get('idContratacaoPncp'),
                            "data_resultado": lic.get('dataAtualizacao') or DATA_STR,
                            "cidade": lic.get('unidadeOrgao', {}).get('municipioNome'),
                            "uf": lic.get('unidadeOrgao', {}).get('ufSigla'),
                            "link_edital": link_custom,
                            "itens": [],
                            "itens_todos_fornecedores": [],
                            "resumo_fornecedores": {}
                        }

                    # Atualiza data
                    banco_total[chave]["data_resultado"] = lic.get('dataAtualizacao') or DATA_STR

                    # Coleta itens (lógica igual ao original, mas simplificada)
                    itens_licitacao_novos = []
                    itens_todos_novos = []
                    resumo_novo = {}

                    for it in itens_api:
                        if it.get('temResultado'):
                            r_v = requests.get(
                                f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens/{it.get('numeroItem')}/resultados",
                                headers=HEADERS, timeout=10
                            )
                            if r_v.status_code == 200:
                                vends = r_v.json()
                                if isinstance(vends, dict): vends = [vends]
                                
                                for v in vends:
                                    cv = (v.get('niFornecedor') or "").replace(".", "").replace("/", "").replace("-", "")
                                    if CNPJ_ALVO in cv:
                                        item = {
                                            "numero_item": it.get('numeroItem'),
                                            "descricao": it.get('descricao', ''),
                                            "quantidade": v.get('quantidadeHomologada', 0),
                                            "valor_unitario": float(v.get('valorUnitarioHomologado') or 0),
                                            "valor_total_item": float(v.get('valorTotalHomologado') or 0),
                                            "cnpj_fornecedor": CNPJ_ALVO
                                        }
                                        itens_licitacao_novos.append(item)
                                        
                                        nome_forn = v.get('nomeRazaoSocialFornecedor', 'Fornecedor PNCP')
                                        tot_item = item['valor_total_item']
                                        if nome_forn not in resumo_novo:
                                            resumo_novo[nome_forn] = 0
                                        resumo_novo[nome_forn] += tot_item

                    # ✅ APLICA MERGE E CÁLCULO
                    banco_total[chave]["itens"] = merge_itens(
                        banco_total[chave]["itens"], itens_licitacao_novos
                    )
                    banco_total[chave]["resumo_fornecedores"] = resumo_novo
                    banco_total[chave] = calcular_totais(banco_total[chave])  # ✅ CORREÇÃO

                except Exception as e:
                    print(f"[err:{str(e)[:15]}]", end="")

            if pagina >= json_resp.get('totalPaginas', 1):
                break
            pagina += 1

        except Exception as e:
            print(f"[req err:{str(e)[:15]}]")
            break

    salvar_estado(banco_total, data_atual + timedelta(days=1))
    data_atual += timedelta(days=1)

print("\n🎉 Coleta concluída! Dados prontos para Dashboard.")
print(f"📁 Gerado: {ARQ_DADOS} ({len(banco_total)} registros)")
