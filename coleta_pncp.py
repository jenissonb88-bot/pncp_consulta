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

# Limite dinâmico: Data de HOJE
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
    """Salva dados e define o checkpoint para o dia SEGUINTE."""
    try:
        with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
            json.dump(list(banco.values()), f, indent=2, ensure_ascii=False)
        with open(ARQ_CHECKPOINT, 'w') as f:
            f.write(data_proxima.strftime('%Y%m%d'))
        print(f"💾 [SUCESSO] Checkpoint avançado para: {data_proxima.strftime('%d/%m/%Y')}")
    except Exception as e:
        print(f"❌ Erro salvando estado: {e}")

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                return datetime.strptime(f.read().strip(), '%Y%m%d')
        except:
            pass
    return datetime(2025, 1, 1) # Data inicial padrão se não houver arquivo

def merge_itens(itens_existentes, novos_itens):
    """
    Mescla itens evitando duplicatas e erros de ordenação entre int/str.
    """
    mapa = {}
    for item in itens_existentes:
        # Usa string como chave para garantir unicidade independentemente do tipo
        key = str(item['numero_item'])
        mapa[key] = item
    
    for novo in novos_itens:
        key = str(novo['numero_item'])
        if key in mapa:
            mapa[key].update(novo)
        else:
            mapa[key] = novo
    
    # Função segura para ordenação (trata erro se vier texto misturado com número)
    def safe_sort_key(x):
        try:
            return int(x['numero_item'])
        except (ValueError, TypeError):
            # Se não for número, tenta limpar ou retorna 0
            return 0 
            
    return sorted(list(mapa.values()), key=safe_sort_key)

def calcular_totais(licitacao):
    """
    Calcula totais com proteção contra valores nulos (NoneType).
    """
    itens = licitacao.get('itens_todos_fornecedores', [])
    
    # Proteção: converte para float e usa 0.0 se for None
    valor_total = sum(float(item.get('valor_total_item') or 0.0) for item in itens)
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
# EXECUÇÃO DE DIA ÚNICO (SINGLE DAY RUN)
# -------------------------------------------------
print("🚀 Iniciando Coleta PNCP (Modo: 1 Dia por Execução)...")

# 1. Lê onde paramos
data_atual = ler_checkpoint()

# 2. Verifica se já chegamos no futuro (amanhã)
if data_atual.date() > DATA_LIMITE_FINAL.date():
    print(f"✅ Checkpoint ({data_atual.strftime('%d/%m/%Y')}) já está atualizado com a data de hoje.")
    print("💤 Nada a fazer. Encerrando até amanhã.")
    exit(0)

print(f"📅 Processando APENAS o dia: {data_atual.strftime('%d/%m/%Y')}")

banco_total = carregar_banco()
DATA_STR = data_atual.strftime('%Y%m%d')
pagina = 1
total_novos = 0

# 3. Loop de Paginação (Apenas para O MESMO dia)
while True:
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
        
        # Se der erro 404 ou 500, loga e sai seguro
        if resp.status_code != 200: 
            print(f"⚠️ Erro HTTP {resp.status_code} na página {pagina}. Parando por segurança.")
            # Não avança o checkpoint se der erro na listagem principal
            exit(1) 
        
        json_resp = resp.json()
        lics = json_resp.get('data', [])
        
        if not lics:
            print(f"   > Página {pagina}: Fim da lista ou sem resultados.")
            break

        print(f"   > Página {pagina}: {len(lics)} editais encontrados.")

        for lic in lics:
            # Identificadores Básicos
            cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
            ano = lic.get('anoCompra')
            seq = lic.get('sequencialCompra')
            uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
            id_licitacao = f"{uasg}{str(seq).zfill(5)}{ano}"
            num_edital_real = lic.get('numeroCompra')
            orgao_nome = lic.get('orgaoEntidade', {}).get('razaoSocial', 'Orgao Desconhecido')
            
            print(f"     > {num_edital_real}/{ano} ({orgao_nome[:15]}...): ", end="")

            # --- BUSCA ITENS ---
            try:
                url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens"
                # Tamanho 5000 para trazer TUDO de uma vez
                r_it = requests.get(url_itens, params={'pagina':1, 'tamanhoPagina': 5000}, headers=HEADERS, timeout=25)
                
                if r_it.status_code != 200: 
                    print("ErrItens")
                    continue

                itens_api = r_it.json()
                if not itens_api: 
                    print("Vazio")
                    continue

                # Prepara Objeto se for Novo
                if id_licitacao not in banco_total:
                    banco_total[id_licitacao] = {
                        "id_licitacao": id_licitacao,
                        "orgao_nome": orgao_nome,
                        "uasg": uasg,
                        "numero_pregao": f"{num_edital_real}/{ano}",
                        "data_resultado": lic.get('dataAtualizacao'),
                        "cidade": lic.get('unidadeOrgao', {}).get('municipioNome'),
                        "uf": lic.get('unidadeOrgao', {}).get('ufSigla'),
                        "link_edital": f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}",
                        "itens_todos_fornecedores": [],
                        "resumo_fornecedores": {}
                    }
                    total_novos += 1

                itens_coletados = []
                resumo_fornecedores = {}
                
                # Otimização: Filtra apenas itens que TEM resultado
                itens_com_resultado = [i for i in itens_api if i.get('temResultado')]

                if not itens_com_resultado:
                    print("S/Res")
                    continue

                # --- VARREDURA DOS ITENS ---
                for it in itens_com_resultado:
                    num_item = it.get('numeroItem')
                    
                    try:
                        r_res = requests.get(f"{url_itens}/{num_item}/resultados", headers=HEADERS, timeout=10)
                        
                        if r_res.status_code == 200:
                            resultados = r_res.json()
                            if isinstance(resultados, dict): resultados = [resultados]
                            
                            for res in resultados:
                                cnpj_venc = res.get('niFornecedor')
                                nome_venc = res.get('nomeRazaoSocialFornecedor')
                                # Tenta pegar data de homologação, senão resultado, senão inclusão
                                data_homolog = res.get('dataHomologacao') or res.get('dataResultado') or res.get('dataInclusao')
                                
                                # Garante valores numéricos
                                qtd = float(res.get('quantidadeHomologada') or 0.0)
                                val_unit = float(res.get('valorUnitarioHomologado') or 0.0)
                                val_total = float(res.get('valorTotalHomologado') or 0.0)

                                item_formatado = {
                                    "numero_item": num_item,
                                    "descricao": it.get('descricao', ''),
                                    "quantidade": qtd,
                                    "valor_unitario": val_unit,
                                    "valor_total_item": val_total,
                                    "cnpj_fornecedor": cnpj_venc,
                                    "nome_fornecedor": nome_venc,
                                    "data_homologacao": data_homolog,
                                    "vencedor_e_alvo": (str(cnpj_venc) in CNPJ_ALVO)
                                }
                                itens_coletados.append(item_formatado)
                                
                                # Soma para resumo
                                if nome_venc:
                                    if nome_venc not in resumo_fornecedores: resumo_fornecedores[nome_venc] = 0.0
                                    resumo_fornecedores[nome_venc] += val_total

                    except Exception as e_item:
                        # Erro pontual num item não deve parar a licitação toda
                        pass

                # Salva e Mescla no Dicionário Principal
                if itens_coletados:
                    banco_total[id_licitacao]['itens_todos_fornecedores'] = merge_itens(
                        banco_total[id_licitacao].get('itens_todos_fornecedores', []), itens_coletados
                    )
                    
                    # Atualiza resumo (merge simples de dicionários para somar valores seria ideal, 
                    # mas substituição aqui funciona pois pegamos o estado atual completo)
                    banco_total[id_licitacao]['resumo_fornecedores'] = resumo_fornecedores
                    
                    # Recalcula totais gerais
                    banco_total[id_licitacao] = calcular_totais(banco_total[id_licitacao])
                    
                    print(f"OK ({len(itens_coletados)} it)")
                else:
                    print("0 novos")

            except Exception as e:
                # Mostra o erro completo para debug
                print(f"ErroProc: {str(e)}")

        # Avança Paginação
        if pagina >= json_resp.get('totalPaginas', 1):
            break
        pagina += 1

    except Exception as e:
        print(f"🚨 Erro crítico de conexão no loop principal: {e}")
        # Sai com erro para não salvar checkpoint caso a rede falhe totalmente
        exit(1)

# 4. Fim do processamento do dia -> Salva e avança checkpoint para o dia seguinte
data_proxima = data_atual + timedelta(days=1)
salvar_estado(banco_total, data_proxima)

print(f"\n🏁 Dia {data_atual.strftime('%d/%m/%Y')} finalizado com sucesso.")
print(f"🔜 Próxima execução (daqui a 1h) pegará: {data_proxima.strftime('%d/%m/%Y')}")
