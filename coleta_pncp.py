import requests
import json

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0'
}

def capturar_detalhes_totais(cnpj_orgao, ano, sequencial):
    # 1. Busca todos os itens (limitando a 500 para pegar tudo de uma vez)
    url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{sequencial}/itens?pagina=1&tamanhoPagina=500"
    
    try:
        resp = requests.get(url_itens, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return "Erro ao acessar itens"
        
        itens = resp.json()
        relatorio = []

        for it in itens:
            item_id = it.get('numeroItem')
            descricao = it.get('descricao')
            
            # Status padrão caso não tenha resultado ainda
            info_item = {
                "item": item_id,
                "descricao": descricao,
                "status": "Em disputa/Divulgado",
                "fornecedor": "N/A",
                "valor_homologado": 0
            }

            # 2. Se o item já foi finalizado (temResultado = True)
            if it.get('temResultado'):
                url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{sequencial}/itens/{item_id}/resultados"
                res_resp = requests.get(url_res, headers=HEADERS, timeout=10)
                
                if res_resp.status_code == 200:
                    resultados = res_resp.json()
                    # Resultados podem ser uma lista (ex: vários vencedores por lote)
                    if isinstance(resultados, dict): resultados = [resultados]
                    
                    for r in resultados:
                        status = r.get('statusNome') # Aqui vem 'Homologado', 'Deserto' ou 'Fracassado'
                        info_item["status"] = status
                        info_item["fornecedor"] = r.get('nomeRazaoSocialFornecedor') or "SEM VENCEDOR"
                        info_item["valor_homologado"] = r.get('valorTotalHomologado') or 0
            
            relatorio.append(info_item)
            
        return relatorio

    except Exception as e:
        return f"Falha na conexão: {e}"

# Exemplo de uso:
# dados = capturar_detalhes_totais("01234567000189", "2024", "123")
