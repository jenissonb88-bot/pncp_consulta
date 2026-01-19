import json
import os
from datetime import datetime, timedelta
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pncp_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PNCPCollector:
    def __init__(self, arquivo_saida='dados_pncp.json'):
        self.arquivo_saida = arquivo_saida
        self.dados_coletados = []
        self.erros_log = []
        self.estatisticas = {
            'total_processados': 0,
            'total_sucesso': 0,
            'total_erros': 0,
            'erros_por_tipo': {}
        }
        self.carregar_dados_existentes()
    
    def carregar_dados_existentes(self):
        """Carrega dados já salvos para não repetir"""
        if os.path.exists(self.arquivo_saida):
            try:
                with open(self.arquivo_saida, 'r', encoding='utf-8') as f:
                    self.dados_coletados = json.load(f)
                logger.info(f"✓ Carregados {len(self.dados_coletados)} registros existentes")
            except Exception as e:
                logger.warning(f"⚠️ Erro ao carregar dados existentes: {e}")
                self.dados_coletados = []
    
    def obter_ids_existentes(self):
        """Retorna set de IDs já coletados para evitar duplicatas"""
        return set(f"{lic['id_pncp']}_{lic['numero_pregao']}" 
                   for lic in self.dados_coletados)
    
    def processar_edital(self, edital_data):
        """Processa um edital com tratamento de erros individual"""
        try:
            # Validação básica
            if not isinstance(edital_data, dict):
                raise TypeError(f"Esperado dict, recebido {type(edital_data)}")
            
            # Extrair campos com defaults seguros
            id_pncp = edital_data.get('id', '')
            numero_pregao = edital_data.get('numero_pregao', '')
            
            # Verificar duplicata
            chave_unica = f"{id_pncp}_{numero_pregao}"
            ids_existentes = self.obter_ids_existentes()
            if chave_unica in ids_existentes:
                logger.debug(f"⏭️ Edital {chave_unica} já coletado, pulando")
                return None
            
            # Processar cada campo com tratamento
            edital_limpo = {
                'id_pncp': id_pncp,
                'numero_pregao': numero_pregao,
                'orgao_codigo': self._extrair_seguro(edital_data, 'orgao_codigo', ''),
                'orgao_nome': self._extrair_seguro(edital_data, 'orgao_nome', 'Órgão desconhecido'),
                'uasg': self._extrair_seguro(edital_data, 'uasg', ''),
                'objeto': self._extrair_seguro(edital_data, 'objeto', ''),
                'cidade': self._extrair_seguro(edital_data, 'cidade', ''),
                'uf': self._extrair_seguro(edital_data, 'uf', ''),
                'data_inicio_propostas': self._extrair_data(edital_data.get('data_inicio')),
                'data_fim_propostas': self._extrair_data(edital_data.get('data_fim')),
                'link_edital': self._extrair_seguro(edital_data, 'link_edital', '#'),
                'itens': self._processar_itens(edital_data.get('itens', [])),
                'itens_todos_fornecedores': self._processar_itens_fornecedores(
                    edital_data.get('itens_todos_fornecedores', [])
                ),
                'data_atualizacao': datetime.now().isoformat(),
            }
            
            self.estatisticas['total_sucesso'] += 1
            logger.info(f"✅ Edital {numero_pregao} processado com sucesso")
            return edital_limpo
            
        except Exception as e:
            tipo_erro = type(e).__name__
            self.estatisticas['total_erros'] += 1
            self.estatisticas['erros_por_tipo'][tipo_erro] = \
                self.estatisticas['erros_por_tipo'].get(tipo_erro, 0) + 1
            
            erro_msg = f"❌ Erro ao processar edital: {str(e)}"
            logger.error(erro_msg)
            self.erros_log.append({
                'timestamp': datetime.now().isoformat(),
                'tipo': tipo_erro,
                'mensagem': str(e),
                'dados': str(edital_data)[:100]
            })
            return None
    
    def _extrair_seguro(self, dados, chave, default=''):
        """Extrai valor com safety default"""
        try:
            valor = dados.get(chave, default)
            if valor is None:
                return default
            return str(valor).strip()
        except Exception as e:
            logger.debug(f"⚠️ Erro ao extrair {chave}: {e}")
            return default
    
    def _extrair_data(self, data_str):
        """Extrai e valida data"""
        try:
            if not data_str:
                return None
            if isinstance(data_str, str):
                # Tentar parse de formatos comuns
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y']:
                    try:
                        return datetime.strptime(data_str, fmt).isoformat()
                    except:
                        continue
            return None
        except Exception as e:
            logger.debug(f"⚠️ Erro ao processar data: {e}")
            return None
    
    def _processar_itens(self, itens_data):
        """Processa lista de itens com validação"""
        if not isinstance(itens_data, list):
            logger.debug(f"⚠️ Itens não é lista: {type(itens_data)}")
            return []
        
        itens_processados = []
        for idx, item in enumerate(itens_data):
            try:
                if not isinstance(item, dict):
                    logger.debug(f"⚠️ Item {idx} não é dict")
                    continue
                
                item_limpo = {
                    'numero_item': self._extrair_seguro(item, 'numero_item', str(idx + 1)),
                    'descricao': self._extrair_seguro(item, 'descricao', ''),
                    'quantidade': self._extrair_numero(item.get('quantidade', 0)),
                    'valor_unitario': self._extrair_numero(item.get('valor_unitario', 0)),
                    'valor_total_item': self._extrair_numero(item.get('valor_total', 0)),
                    'fornecedor': self._extrair_seguro(item, 'fornecedor', ''),
                    'cnpj_fornecedor': self._extrair_seguro(item, 'cnpj_fornecedor', ''),
                    'data_homologacao': self._extrair_data(item.get('data_homologacao')),
                }
                itens_processados.append(item_limpo)
            except Exception as e:
                logger.debug(f"⚠️ Erro ao processar item {idx}: {e}")
                continue
        
        return itens_processados
    
    def _processar_itens_fornecedores(self, itens_data):
        """Processa itens de todos fornecedores"""
        return self._processar_itens(itens_data)
    
    def _extrair_numero(self, valor):
        """Extrai número com segurança"""
        try:
            if valor is None or valor == '':
                return 0
            if isinstance(valor, (int, float)):
                return float(valor)
            if isinstance(valor, str):
                # Remove caracteres não-numéricos exceto ponto e vírgula
                valor_limpo = valor.replace(',', '.').replace('R$', '').strip()
                return float(valor_limpo) if valor_limpo else 0
            return 0
        except Exception as e:
            logger.debug(f"⚠️ Erro ao extrair número '{valor}': {e}")
            return 0
    
    def coletar_lote(self, lote_dados, data_inicio=None):
        """Coleta um lote de dados com rastreamento de progresso"""
        logger.info(f"📅 Iniciando coleta de {len(lote_dados)} editais")
        if data_inicio:
            logger.info(f"   Data: {data_inicio}")
        
        self.estatisticas['total_processados'] = len(lote_dados)
        
        for idx, edital in enumerate(lote_dados, 1):
            edital_processado = self.processar_edital(edital)
            if edital_processado:
                self.dados_coletados.append(edital_processado)
            
            # Mostrar progresso a cada 10 itens
            if idx % 10 == 0:
                logger.info(f"   Progresso: {idx}/{len(lote_dados)}")
        
        # Salvar após processar lote
        self.salvar_dados()
        return self.gerar_relatorio()
    
    def salvar_dados(self):
        """Salva dados com backup automático"""
        try:
            # Backup do arquivo anterior
            if os.path.exists(self.arquivo_saida):
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_file = f"{self.arquivo_saida}.backup_{timestamp}"
                os.rename(self.arquivo_saida, backup_file)
                logger.info(f"💾 Backup criado: {backup_file}")
            
            # Salvar novo arquivo
            with open(self.arquivo_saida, 'w', encoding='utf-8') as f:
                json.dump(self.dados_coletados, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✓ Dados salvos: {len(self.dados_coletados)} registros")
            
        except Exception as e:
            logger.error(f"❌ Erro ao salvar dados: {e}")
            raise
    
    def gerar_relatorio(self):
        """Gera relatório de coleta"""
        relatorio = {
            'timestamp': datetime.now().isoformat(),
            'estatisticas': self.estatisticas,
            'erros_sample': self.erros_log[:5],  # Primeiros 5 erros
            'total_erros': len(self.erros_log),
            'proxima_coleta': (datetime.now() + timedelta(days=1)).isoformat(),
        }
        
        logger.info("\n" + "="*60)
        logger.info("📊 RELATÓRIO DE COLETA")
        logger.info("="*60)
        logger.info(f"✅ Sucesso: {self.estatisticas['total_sucesso']}")
        logger.info(f"❌ Erros: {self.estatisticas['total_erros']}")
        logger.info(f"📈 Taxa de sucesso: {self._calcular_taxa()}%")
        logger.info(f"💾 Total no banco: {len(self.dados_coletados)}")
        logger.info(f"📅 Próxima coleta: {relatorio['proxima_coleta']}")
        
        if self.estatisticas['erros_por_tipo']:
            logger.info("\nErros por tipo:")
            for tipo, qtd in self.estatisticas['erros_por_tipo'].items():
                logger.info(f"  - {tipo}: {qtd}")
        
        logger.info("="*60 + "\n")
        
        return relatorio
    
    def _calcular_taxa(self):
        """Calcula taxa de sucesso"""
        total = self.estatisticas['total_processados']
        if total == 0:
            return 0
        taxa = (self.estatisticas['total_sucesso'] / total) * 100
        return round(taxa, 2)
    
    def salvar_relatorio(self, nome_arquivo='relatorio_coleta.json'):
        """Salva relatório em arquivo"""
        try:
            relatorio = self.gerar_relatorio()
            with open(nome_arquivo, 'w', encoding='utf-8') as f:
                json.dump(relatorio, f, ensure_ascii=False, indent=2)
            logger.info(f"📄 Relatório salvo: {nome_arquivo}")
        except Exception as e:
            logger.error(f"Erro ao salvar relatório: {e}")


# ===== EXEMPLO DE USO =====

if __name__ == "__main__":
    # Simular dados de entrada com alguns erros intencionais
    dados_teste = [
        {
            'id': 'PNCP001',
            'numero_pregao': '2025001',
            'orgao_codigo': 'SAUDE',
            'orgao_nome': 'Secretaria de Saúde',
            'uasg': '001',
            'objeto': 'Fornecimento de medicamentos',
            'cidade': 'Recife',
            'uf': 'PE',
            'data_inicio': '2025-01-01',
            'data_fim': '2025-01-15',
            'link_edital': 'https://example.com/edital1',
            'itens': [
                {
                    'numero_item': '001',
                    'descricao': 'Paracetamol 500mg',
                    'quantidade': 1000,
                    'valor_unitario': 0.50,
                    'valor_total': 500.00,
                    'fornecedor': 'Empresa A',
                    'cnpj_fornecedor': '08778201000126',
                    'data_homologacao': '2025-01-20'
                }
            ]
        },
        {
            # Este edital causará erro (faltam dados obrigatórios)
            'id': 'PNCP002',
            'numero_pregao': None,  # Isso vai gerar erro
            'orgao_codigo': 'EDUCACAO'
            # Faltam vários campos
        },
        {
            'id': 'PNCP003',
            'numero_pregao': '2025003',
            'orgao_codigo': 'ADMIN',
            'orgao_nome': 'Administração',
            'objeto': 'Fornecimento de papel A4',
            # Alguns campos faltando - será preenchido com defaults
            'itens': 'invalido'  # Será convertido para lista vazia
        }
    ]
    
    # Executar coleta
    collector = PNCPCollector()
    relatorio = collector.coletar_lote(dados_teste, data_inicio='2025-01-01')
    collector.salvar_relatorio()
