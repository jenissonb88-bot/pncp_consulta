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
                # dados agora será lista de licitações; indexar
                banco = {}
                for lic in dados:
                    chave = f"{lic['id_licitacao']}-{lic['cnpj_fornecedor']}"
                    banco[chave] = lic
                return banco
        except:
            pass
    return {}

def salvar_estado(banco, data_proxima):
    """Salva JSON consolidado + checkpoint."""
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, indent=2, ensure_ascii=False)
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(data_proxima.strftime('%Y%m%d'))
    print(f"\n💾 [ESTADO SALVO] Próximo início: {data_proxima.strftime('%d/%m/%Y')}")

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            return datetime.strptime(f.read().strip(), '%Y%m%d')
    # início da busca
    return datetime(2025, 1, 1)

# -------------------------------------------------
# LOOP PRINCIPAL
# -------------------------------------------------
data_inicio = ler_checkpoint()
if data_inicio > DATA_LIMITE_FINAL:
    print("✅ Missão 2025 concluída!")
    exit(0)

data_fim = data_inicio + timedelta(days=DIAS_POR_CICLO - 1)
if data_fim > DATA_LIMITE_FINAL:
    data_fim = DATA_LIMITE_FINAL

print(f"--- 🚀 COLETA PNCP - PREGÃO ELETRÔNICO ---")
print(f"Alvo: {CNPJ_ALVO} | Janela: {data_inicio.strftime('%d/%m')} a {data_fim.strftime('%d/%m')}")

banco_total = carregar_banco()
data_atual = data_inicio

while data_atual <= data_fim:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Data {data_atual.strftime('%d/%m/%Y')}: ", end="")

    pagina = 1
    while True:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR,
            "dataFinal": DATA_STR,
            "codigoModalidadeContratacao": "6",  # pregão eletrônico
            "pagina": pagina,
            "tamanhoPagina": 50,
            "niFornecedor": CNPJ_ALVO
        }

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                print(f"[HTTP {resp.status_code}]", end="")
                break

            json_resp = resp.json()
            lics = json_resp.get('data', [])
            if not lics:
                print("[Sem licitações]", end="")
                break

            print(f"[{len(lics)} editais]", end="", flush=True)

            for idx, lic in enumerate(lics):
                if idx % 10 == 0 and idx > 0:
                    salvar_estado(banco_total, data_atual)

                cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                ano = lic.get('anoCompra')
                seq = lic.get('sequencialCompra')

                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
                id_licitacao = f"{uasg}{str(seq).zfill(5)}{ano}"

                num_edital_real = lic.get('numeroCompra')  # número oficial (ex: 90007)
                link_custom = f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}"

                chave = f"{id_licitacao}-{CNPJ_ALVO}"

                # Se já temos itens cadastrados, pule
                if chave in banco_total and banco_total[chave].get("itens"):
                    continue

                try:
                    time.sleep(0.1)
                    r_it = requests.get(
                        f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens",
                        headers=HEADERS,
                        timeout=15
                    )
                    if r_it.status_code != 200:
                        continue

                    itens_api = r_it.json()
                    if not itens_api:
                        continue

                    # Garante estrutura base da licitação no banco
                    if chave not in banco_total:
                        banco_total[chave] = {
                            "id_licitacao": id_licitacao,
                            "cnpj_fornecedor": CNPJ_ALVO,
                            "orgao_codigo": uasg,
                            "orgao_nome": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                            "uasg": uasg,
                            "numero_pregao": f"{num_edital_real}/{ano}" if num_edital_real else f"{str(seq).zfill(5)}/{ano}",
                            "id_pncp": lic.get('idContratacaoPncp'),
                            "data_inicio_propostas": lic.get('dataInicioRecebimentoPropostas'),
                            "data_fim_propostas": lic.get('dataFimRecebimentoPropostas'),
                            "cidade": lic.get('unidadeOrgao', {}).get('municipioNome'),
                            "uf": lic.get('unidadeOrgao', {}).get('ufSigla'),
                            "objeto": lic.get('objetoCompra') or lic.get('descricao', ''),
                            "link_edital": link_custom,
                            "data_resultado": lic.get('dataAtualizacao') or DATA_STR,
                            "itens": [],
                            "itens_todos_fornecedores": []
                        }

                    itens_licitacao = banco_total[chave]["itens"]
                    itens_todos = banco_total[chave]["itens_todos_fornecedores"]

                    # Para calcular total por fornecedor dentro da licitação
                    totais_fornecedor = {}

                    # ========================================================================
                    # NOVO: COLETA TODOS OS ITENS, NÃO APENAS OS DO CNPJ_ALVO
                    # ========================================================================
                    for it in itens_api:
                        numero_item = it.get('numeroItem')
                        descricao_item = it.get('descricao', '')
                        qtd_estimada = it.get('quantidadeTotal')
                        valor_estimado = float(it.get('valorEstimado') or 0)

                        # SE TEM RESULTADO: puxa os fornecedores/vencedores
                        if it.get('temResultado'):
                            try:
                                r_v = requests.get(
                                    f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens/{numero_item}/resultados",
                                    headers=HEADERS,
                                    timeout=10
                                )
                                if r_v.status_code != 200:
                                    continue

                                vends = r_v.json()
                                if isinstance(vends, dict):
                                    vends = [vends]

                                # COLETA TODOS OS FORNECEDORES (não filtra por CNPJ_ALVO aqui)
                                for v in vends:
                                    fornecedor_cnpj = v.get('niFornecedor') or ''
                                    fornecedor_nome = v.get('nomeRazaoSocialFornecedor')
                                    
                                    qtd = v.get('quantidadeHomologada') or 0
                                    unit = float(v.get('valorUnitarioHomologado') or 0)
                                    tot = float(v.get('valorTotalHomologado') or qtd * unit)
                                    data_homolog = v.get('dataHomologacao') or lic.get('dataAtualizacao')

                                    # Marca se é nosso CNPJ ou não
                                    cv_clean = (fornecedor_cnpj or "").replace(".", "").replace("/", "").replace("-", "")
                                    eh_nosso = CNPJ_ALVO in cv_clean

                                    item_reg = {
                                        "numero_item": numero_item,
                                        "descricao": descricao_item,
                                        "data_homologacao": data_homolog,
                                        "quantidade": qtd,
                                        "valor_unitario": unit,
                                        "fornecedor": fornecedor_nome,
                                        "cnpj_fornecedor": fornecedor_cnpj,
                                        "valor_total_item": tot,
                                        "situacao": "Venceu"
                                    }

                                    # Adiciona em TODOS os itens
                                    if not any(x['numero_item'] == numero_item and x['fornecedor'] == fornecedor_nome for x in itens_todos):
                                        itens_todos.append(item_reg)

                                    # Se é nosso CNPJ, adiciona também em itens específicos e soma total
                                    if eh_nosso:
                                        if not any(x['numero_item'] == numero_item for x in itens_licitacao):
                                            itens_licitacao.append(item_reg)
                                        
                                        fornecedor_nome_key = fornecedor_nome or "Sem identificação"
                                        totais_fornecedor.setdefault(fornecedor_nome_key, 0)
                                        totais_fornecedor[fornecedor_nome_key] += tot

                                    print("✅", end="", flush=True)

                            except Exception as e:
                                print(f"[erro ao processar resultados: {e}]", end="")
                                continue

                        else:
                            # ITEM SEM RESULTADO: deserto ou fracassado
                            item_sem_resultado = {
                                "numero_item": numero_item,
                                "descricao": descricao_item,
                                "data_homologacao": None,
                                "quantidade": qtd_estimada,
                                "valor_unitario": valor_estimado,
                                "fornecedor": None,
                                "cnpj_fornecedor": None,
                                "valor_total_item": None,
                                "situacao": "SemResultado"
                            }
                            
                            if not any(x['numero_item'] == numero_item for x in itens_todos):
                                itens_todos.append(item_sem_resultado)

                            print("⚠️", end="", flush=True)

                    # Atualiza totais por fornecedor (apenas para nosso CNPJ)
                    banco_total[chave]["totais_fornecedor"] = [
                        {
                            "fornecedor": forn,
                            "valor_total_fornecedor": valor
                        }
                        for forn, valor in totais_fornecedor.items()
                    ]

                except Exception as e:
                    print(f"[erro geral: {str(e)[:30]}]", end="")
                    continue

            if pagina >= json_resp.get('totalPaginas', 1):
                break
            pagina += 1
        except Exception as e:
            print(f"[erro na requisição: {str(e)[:30]}]", end="")
            break

    salvar_estado(banco_total, data_atual + timedelta(days=1))
    data_atual += timedelta(days=1)

print("\n\n✅ Coleta concluída.")
```
