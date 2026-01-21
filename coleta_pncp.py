# --- Certifique-se que estas variáveis estejam no topo do arquivo ---
DIAS_POR_CICLO = 1  # Quantos dias ele processa por execução do GitHub
DATA_LIMITE_FINAL = datetime.now()

def run():
    session = criar_sessao()
    data_inicio = ler_checkpoint()
    
    # Se o checkpoint já passou de hoje, ele para.
    if data_inicio.date() > DATA_LIMITE_FINAL.date():
        print("✅ Ranking já está atualizado até hoje!")
        return

    # Define até onde ele vai coletar NESTA execução (ex: 1 dia ou 30 dias)
    data_fim = data_inicio + timedelta(days=DIAS_POR_CICLO - 1)
    if data_fim > DATA_LIMITE_FINAL: 
        data_fim = DATA_LIMITE_FINAL

    print(f"--- 🚀 RANKING TURBO: {data_inicio.strftime('%d/%m')} a {data_fim.strftime('%d/%m')} ---")
    
    banco_total = carregar_banco()
    data_atual = data_inicio

    # O SEGREDO ESTÁ NESTE LOOP ABAIXO:
    while data_atual <= data_fim:
        DATA_STR = data_atual.strftime('%Y%m%d')
        print(f"\n📅 Processando Dia: {data_atual.strftime('%d/%m/%Y')}")
        
        # ... (Toda a sua lógica de busca por página e itens aqui dentro) ...
        # (Certifique-se que o código de busca de editais esteja indentado dentro deste while)

        # AO FINAL DO PROCESSAMENTO DO DIA, AVANÇA E SALVA:
        data_atual += timedelta(days=1)
        salvar_estado(banco_total, data_atual)

    print("\n🏁 Fim do ciclo de coleta programado.")
