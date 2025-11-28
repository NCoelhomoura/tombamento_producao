"""
Script de migração de dados: SQL Server PRD -> PostgreSQL HML (gmcore)
Migra dados das tabelas: customers, customer_segments, addresses, contacts
"""

import sys
import os
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple

# Adicionar diretório utils ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database_connection import DatabaseConnection

# Configurar logging
# Criar logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Remover handlers existentes para evitar duplicacao
if logger.handlers:
    logger.handlers.clear()

# Handler para arquivo (modo 'w' para truncar arquivo a cada execucao)
# Arquivo na raiz do projeto
try:
    log_file_path = os.path.join(os.path.dirname(__file__), '..', 'log_execution.txt')
    log_file_path = os.path.abspath(log_file_path)  # Converter para caminho absoluto
    file_handler = logging.FileHandler(log_file_path, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
except Exception as e:
    # Se houver erro, apenas criar handler de console
    pass

# Handler para console (terminal) - mostrar em tempo real
try:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
except Exception:
    pass

# Tamanho do chunk para processamento
CHUNK_SIZE = 1000


class CustomersMigration:
    """Classe para executar a migração de dados"""
    
    def __init__(self, limit_rows=0):
        self.stats = {
            'customers': 0,
            'customer_segments': 0,
            'addresses': 0,
            'contacts': 0,
            'errors': []
        }
        self.customer_id_map = {}  # Map: legado_id -> uuid
        self.segment_id_map = {}   # Map: legado_id -> uuid
        self.limit_rows = limit_rows  # 0 = todos, > 0 = limitar quantidade
    
    def clean_string(self, value: Optional[str], max_length: Optional[int] = None) -> Optional[str]:
        """Limpa e trunca string"""
        if value is None:
            return None
        cleaned = str(value).strip()
        if not cleaned or cleaned.lower() in ['null', 'none', '']:
            return None
        if max_length and len(cleaned) > max_length:
            cleaned = cleaned[:max_length]
        return cleaned
    
    def clean_cpf_cnpj(self, value: Optional[str]) -> Optional[str]:
        """Remove formatação de CPF/CNPJ"""
        if not value:
            return None
        import re
        cleaned = re.sub(r'[^\d]', '', str(value))
        if not cleaned or cleaned == '0' * len(cleaned):
            return None
        return cleaned
    
    def convert_status(self, ativo: Optional[bool]) -> str:
        """Converte status booleano para string"""
        if ativo is True:
            return 'active'
        return 'inactive'
    
    def truncate_table(self, table_name: str, schema: str = 'gmcore'):
        """Faz TRUNCATE em uma tabela"""
        conn = None
        try:
            conn = DatabaseConnection.get_postgresql_hml_connection()
            cursor = conn.cursor()
            
            query = f"TRUNCATE TABLE {schema}.{table_name} CASCADE"
            cursor.execute(query)
            conn.commit()
            
            logger.info(f"Tabela {schema}.{table_name} truncada com sucesso")
            print(f"OK - Tabela {schema}.{table_name} truncada")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Erro ao truncar tabela {schema}.{table_name}: {e}")
            if conn:
                conn.rollback()
                conn.close()
            raise
    
    def validate_step1_customer_segments(self):
        """Validação e relatório de qualidade - ETAPA 1"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 1: CUSTOMER_SEGMENTS")
        print("-"*80)
        
        try:
            # Contar origem (aplicando limite se especificado)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            if self.limit_rows > 0:
                cursor_sql.execute(f"SELECT COUNT(*) FROM (SELECT TOP {self.limit_rows} Id FROM SegmentoProduto ORDER BY Id) AS limited")
            else:
                cursor_sql.execute("SELECT COUNT(*) FROM SegmentoProduto")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            conn_pg = DatabaseConnection.get_postgresql_hml_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute("SELECT COUNT(*) FROM gmcore.customer_segments")
            destino_count = cursor_pg.fetchone()[0]
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - SegmentoProduto):")
            print(f"  Total de registros: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL HML - gmcore.customer_segments):")
            print(f"  Total de registros: {destino_count}")
            
            diferenca = origem_count - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os registros foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 1: OK - Origem: {origem_count}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} registros")
                print(f"  {abs(diferenca)} registros {'faltando' if diferenca > 0 else 'extras'} no destino")
                logger.warning(f"VALIDACAO ETAPA 1: Diferenca - Origem: {origem_count}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 1: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step1_migrate_customer_segments(self):
        """ETAPA 1: Migrar customer_segments"""
        print("\n" + "="*80)
        print("ETAPA 1: MIGRANDO CUSTOMER_SEGMENTS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 1: Migrando customer_segments")
        logger.info("="*80)
        
        # Truncate
        print("\n[ETAPA 1] Limpando tabela customer_segments...")
        self.truncate_table('customer_segments')
        
        # Buscar dados do SQL Server
        print("[ETAPA 1] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            Id,
            Nome,
            Ativo,
            DataInclusao,
            DataAlteracao
        FROM SegmentoProduto
        ORDER BY Id
        """
        
        # Adicionar LIMIT se especificado
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY Id", f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute(sql_query)
        
        # Processar em chunks
        conn_pg = DatabaseConnection.get_postgresql_hml_connection()
        cursor_pg = conn_pg.cursor()
        
        chunk = []
        chunk_num = 0
        total_processed = 0
        
        try:
            while True:
                rows = cursor_sql.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                
                chunk_num += 1
                print(f"[ETAPA 1] Processando chunk {chunk_num} ({len(rows)} registros)...")
                
                chunk_errors = 0
                for row in rows:
                    try:
                        segment_id = uuid.uuid4()
                        legado_id = row[0]
                        self.segment_id_map[legado_id] = segment_id
                        
                        insert_query = """
                        INSERT INTO gmcore.customer_segments (
                            id, name, is_active, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s)
                        """
                        
                        cursor_pg.execute(insert_query, (
                            str(segment_id),
                            self.clean_string(row[1], 255),  # Nome
                            row[2] if row[2] is not None else False,  # Ativo
                            row[3],  # DataInclusao -> created_at
                            row[4] if row[4] else row[3]  # DataAlteracao -> updated_at
                        ))
                        
                        self.stats['customer_segments'] += 1
                        total_processed += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir segmento Id={row[0]}: {e}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        # Fazer rollback imediato para não abortar toda a transação
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
                
                # Commit do chunk apenas se não houve erro crítico
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 1] Chunk {chunk_num} processado: {total_processed} registros inseridos, {chunk_errors} erros")
                    logger.info(f"[ETAPA 1] Chunk {chunk_num} processado: {total_processed} registros inseridos, {chunk_errors} erros")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            print(f"\n[ETAPA 1] CONCLUIDA! Total de customer_segments migrados: {self.stats['customer_segments']}")
            logger.info(f"ETAPA 1 concluida: {self.stats['customer_segments']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step1_customer_segments()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 1: {e}")
            raise
        finally:
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
    
    def validate_step2_customers(self):
        """Validação e relatório de qualidade - ETAPA 2"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 2: CUSTOMERS")
        print("-"*80)
        
        try:
            # Contar origem (aplicando limite se especificado)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            if self.limit_rows > 0:
                cursor_sql.execute(f"SELECT COUNT(*) FROM (SELECT TOP {self.limit_rows} Id FROM Cliente ORDER BY Id) AS limited")
            else:
                cursor_sql.execute("SELECT COUNT(*) FROM Cliente")
            origem_count = cursor_sql.fetchone()[0]
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            conn_pg = DatabaseConnection.get_postgresql_hml_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute("SELECT COUNT(*) FROM gmcore.customers")
            destino_count = cursor_pg.fetchone()[0]
            
            # Verificar legacy_id
            cursor_pg.execute("SELECT COUNT(*) FROM gmcore.customers WHERE legacy_id IS NOT NULL")
            com_legacy_id = cursor_pg.fetchone()[0]
            
            # Verificar cnpj preenchido
            cursor_pg.execute("SELECT COUNT(*) FROM gmcore.customers WHERE cnpj IS NOT NULL")
            com_cnpj = cursor_pg.fetchone()[0]
            
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Cliente):")
            print(f"  Total de registros: {origem_count}")
            
            print(f"\nDESTINO (PostgreSQL HML - gmcore.customers):")
            print(f"  Total de registros: {destino_count}")
            print(f"  Com legacy_id: {com_legacy_id}")
            print(f"  Com cnpj: {com_cnpj}")
            
            diferenca = origem_count - destino_count
            
            if diferenca == 0:
                print(f"\nOK - Todos os registros foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 2: OK - Origem: {origem_count}, Destino: {destino_count}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} registros")
                print(f"  {abs(diferenca)} registros {'faltando' if diferenca > 0 else 'extras'} no destino")
                logger.warning(f"VALIDACAO ETAPA 2: Diferenca - Origem: {origem_count}, Destino: {destino_count}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 2: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step2_migrate_customers(self):
        """ETAPA 2: Migrar customers"""
        print("\n" + "="*80)
        print("ETAPA 2: MIGRANDO CUSTOMERS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 2: Migrando customers")
        logger.info("="*80)
        
        # Truncate
        print("\n[ETAPA 2] Limpando tabela customers...")
        self.truncate_table('customers')
        
        # Buscar dados do SQL Server
        print("[ETAPA 2] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            Id,
            Codigo,
            TipoPessoa,
            CpfCnpj,
            InscricaoEstadual,
            InscricaoMunicipal,
            RazaoSocial,
            NomeFantasia,
            Ativo,
            DataInclusao,
            DataAlteracao,
            DataAtivacao
        FROM Cliente
        ORDER BY Id
        """
        
        # Adicionar LIMIT se especificado
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY Id", f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute(sql_query)
        
        # Processar em chunks
        conn_pg = DatabaseConnection.get_postgresql_hml_connection()
        cursor_pg = conn_pg.cursor()
        
        chunk_num = 0
        total_processed = 0
        
        try:
            while True:
                rows = cursor_sql.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                
                chunk_num += 1
                print(f"[ETAPA 2] Processando chunk {chunk_num} ({len(rows)} registros)...")
                logger.info(f"[ETAPA 2] Processando chunk {chunk_num} ({len(rows)} registros)...")
                
                chunk_errors = 0
                for row in rows:
                    try:
                        customer_id = uuid.uuid4()
                        legado_id = row[0]
                        self.customer_id_map[legado_id] = customer_id
                        
                        # Limpar CPF/CNPJ
                        cpf_cnpj = self.clean_cpf_cnpj(row[3])
                        
                        # Se não houver CPF/CNPJ, usar valor padrão (campo é NOT NULL)
                        if not cpf_cnpj:
                            cpf_cnpj = '00000000000000'  # Valor padrão para CNPJ vazio
                        
                        # Determinar status (mapear Ativo para status)
                        # Se Ativo = True -> 'active', se False -> 'inactive'
                        status = 'active' if row[8] is True else 'inactive'
                        
                        insert_query = """
                        INSERT INTO gmcore.customers (
                            id, legacy_id, cnpj, cnpj_status, state_registration,
                            municipal_registration, legal_name, trade_name, status,
                            created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        cursor_pg.execute(insert_query, (
                            str(customer_id),
                            legado_id,  # legacy_id
                            cpf_cnpj,  # cnpj (sempre preenchido, mesmo que com valor padrão)
                            'valid' if cpf_cnpj and cpf_cnpj != '00000000000000' else 'invalid',  # cnpj_status
                            self.clean_string(row[4], 20) if row[4] else None,  # InscricaoEstadual -> state_registration
                            self.clean_string(row[5], 20) if row[5] else None,  # InscricaoMunicipal -> municipal_registration
                            self.clean_string(row[6], 255),  # RazaoSocial -> legal_name
                            self.clean_string(row[7], 255),  # NomeFantasia -> trade_name
                            status,  # status
                            row[9],  # DataInclusao -> created_at
                            row[10] if row[10] else row[9]  # DataAlteracao -> updated_at
                        ))
                        
                        self.stats['customers'] += 1
                        total_processed += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir customer Id={row[0]}: {e}"
                        logger.error(error_msg)
                        print(f"ERRO - {error_msg}")
                        self.stats['errors'].append(error_msg)
                        chunk_errors += 1
                        # Fazer rollback imediato para não abortar toda a transação
                        try:
                            conn_pg.rollback()
                            cursor_pg = conn_pg.cursor()
                        except Exception as rollback_error:
                            logger.error(f"Erro ao fazer rollback: {rollback_error}")
                        continue
                
                # Commit do chunk apenas se não houve erro crítico
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 2] Chunk {chunk_num} processado: {total_processed} registros inseridos, {chunk_errors} erros")
                    logger.info(f"[ETAPA 2] Chunk {chunk_num} processado: {total_processed} registros inseridos, {chunk_errors} erros")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            print(f"\n[ETAPA 2] CONCLUIDA! Total de customers migrados: {self.stats['customers']}")
            logger.info(f"ETAPA 2 concluida: {self.stats['customers']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step2_customers()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 2: {e}")
            raise
        finally:
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
    
    def validate_step3_addresses(self):
        """Validação e relatório de qualidade - ETAPA 3"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 3: ADDRESSES")
        print("-"*80)
        
        try:
            # Aplicar limite se especificado
            limit_clause = ""
            if self.limit_rows > 0:
                limit_clause = f" OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY"
            
            # Contar origem - endereços principais (aplicando mesmo filtro e limite da migração)
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            query_main = f"""
                SELECT COUNT(*) 
                FROM (
                    SELECT Id, Endereco, EnderecoCobranca
                    FROM Cliente
                    WHERE (Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != '')
                       OR (EnderecoCobranca IS NOT NULL AND LTRIM(RTRIM(EnderecoCobranca)) != '')
                    ORDER BY Id
                    {limit_clause}
                ) AS limited
                WHERE Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != ''
            """
            cursor_sql.execute(query_main)
            origem_main = cursor_sql.fetchone()[0]
            
            # Contar origem - endereços de cobrança
            query_billing = f"""
                SELECT COUNT(*) 
                FROM (
                    SELECT Id, Endereco, EnderecoCobranca
                    FROM Cliente
                    WHERE (Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != '')
                       OR (EnderecoCobranca IS NOT NULL AND LTRIM(RTRIM(EnderecoCobranca)) != '')
                    ORDER BY Id
                    {limit_clause}
                ) AS limited
                WHERE EnderecoCobranca IS NOT NULL AND LTRIM(RTRIM(EnderecoCobranca)) != ''
            """
            cursor_sql.execute(query_billing)
            origem_billing = cursor_sql.fetchone()[0]
            
            origem_total = origem_main + origem_billing
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            conn_pg = DatabaseConnection.get_postgresql_hml_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute("SELECT COUNT(*) FROM gmcore.addresses")
            destino_total = cursor_pg.fetchone()[0]
            
            # Contar por tipo
            cursor_pg.execute("SELECT COUNT(*) FROM gmcore.addresses WHERE type = 'main'")
            destino_main = cursor_pg.fetchone()[0]
            
            cursor_pg.execute("SELECT COUNT(*) FROM gmcore.addresses WHERE type = 'billing'")
            destino_billing = cursor_pg.fetchone()[0]
            
            # Verificar addressable_id preenchido
            cursor_pg.execute("SELECT COUNT(*) FROM gmcore.addresses WHERE addressable_id IS NOT NULL")
            com_addressable_id = cursor_pg.fetchone()[0]
            
            # Verificar addressable_type
            cursor_pg.execute("SELECT COUNT(*) FROM gmcore.addresses WHERE addressable_type = 'customers'")
            com_addressable_type = cursor_pg.fetchone()[0]
            
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Cliente):")
            print(f"  Enderecos principais: {origem_main}")
            print(f"  Enderecos de cobranca: {origem_billing}")
            print(f"  Total esperado: {origem_total}")
            
            print(f"\nDESTINO (PostgreSQL HML - gmcore.addresses):")
            print(f"  Enderecos principais (type='main'): {destino_main}")
            print(f"  Enderecos de cobranca (type='billing'): {destino_billing}")
            print(f"  Total inserido: {destino_total}")
            print(f"  Com addressable_id: {com_addressable_id}")
            print(f"  Com addressable_type='customers': {com_addressable_type}")
            
            diferenca = origem_total - destino_total
            
            if diferenca == 0:
                print(f"\nOK - Todos os enderecos foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 3: OK - Origem: {origem_total}, Destino: {destino_total}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} enderecos")
                print(f"  {abs(diferenca)} enderecos {'faltando' if diferenca > 0 else 'extras'} no destino")
                logger.warning(f"VALIDACAO ETAPA 3: Diferenca - Origem: {origem_total}, Destino: {destino_total}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 3: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step3_migrate_addresses(self):
        """ETAPA 3: Migrar addresses"""
        print("\n" + "="*80)
        print("ETAPA 3: MIGRANDO ADDRESSES")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 3: Migrando addresses")
        logger.info("="*80)
        
        # Truncate
        print("\n[ETAPA 3] Limpando tabela addresses...")
        self.truncate_table('addresses')
        
        # Buscar dados do SQL Server
        print("[ETAPA 3] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            Id,
            Endereco,
            Numero,
            Complemento,
            Bairro,
            CEP,
            Cidade,
            CodigoMunicipio,
            UF,
            EnderecoCobranca,
            NumeroCobranca,
            ComplementoCobranca,
            BairroCobranca,
            CepCobranca,
            CidadeCobranca,
            CodigoMunicipioCobranca,
            UFCobranca,
            Latitude,
            Longitude,
            DataInclusao,
            DataAlteracao
        FROM Cliente
        WHERE (Endereco IS NOT NULL AND LTRIM(RTRIM(Endereco)) != '')
           OR (EnderecoCobranca IS NOT NULL AND LTRIM(RTRIM(EnderecoCobranca)) != '')
        ORDER BY Id
        """
        
        # Adicionar LIMIT se especificado
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY Id", f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute(sql_query)
        
        # Processar em chunks
        conn_pg = DatabaseConnection.get_postgresql_hml_connection()
        cursor_pg = conn_pg.cursor()
        
        chunk_num = 0
        total_processed = 0
        
        try:
            while True:
                rows = cursor_sql.fetchmany(CHUNK_SIZE)
                if not rows:
                    break
                
                chunk_num += 1
                print(f"[ETAPA 3] Processando chunk {chunk_num} ({len(rows)} registros)...")
                logger.info(f"[ETAPA 3] Processando chunk {chunk_num} ({len(rows)} registros)...")
                
                chunk_errors = 0
                for row in rows:
                    legado_id = row[0]
                    customer_id = self.customer_id_map.get(legado_id)
                    
                    if not customer_id:
                        continue
                    
                    # Endereço Principal
                    if row[1] and str(row[1]).strip():  # Endereco
                        try:
                            import re
                            cep = re.sub(r'[^\d]', '', str(row[5])) if row[5] else None
                            if not cep or cep == '0' * len(cep):
                                cep = '00000000'  # Valor padrão (campo é NOT NULL)
                            
                            # Converter CodigoMunicipio para integer se possível
                            municipal_code = 0  # Valor padrão (campo é NOT NULL)
                            if row[7]:
                                try:
                                    codigo_str = re.sub(r'[^\d]', '', str(row[7]))
                                    if codigo_str:
                                        municipal_code = int(codigo_str[:10])  # Limitar a 10 dígitos
                                except:
                                    pass
                            
                            insert_query = """
                            INSERT INTO gmcore.addresses (
                                id, legacy_id, addressable_id, addressable_type, type,
                                postal_code, street, number, address_line_2, neighborhood,
                                city, state, municipal_code, latitude, longitude, zone, region,
                                created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            
                            # Converter latitude/longitude se possível
                            lat = None
                            lon = None
                            if row[17]:  # Latitude
                                try:
                                    lat = float(str(row[17]).replace(',', '.'))
                                except:
                                    pass
                            if row[18]:  # Longitude
                                try:
                                    lon = float(str(row[18]).replace(',', '.'))
                                except:
                                    pass
                            
                            # Zone e Region são obrigatórios - usar neighborhood como zone e string vazia como region
                            zone_value = self.clean_string(row[4], 100) or ''  # neighborhood como zone (garantir que nunca seja None)
                            region_value = ''  # region vazio (não temos na origem)
                            
                            # Number é obrigatório - usar 'S/N' se não houver número
                            number_value = self.clean_string(row[2], 20) if row[2] and str(row[2]).strip() else 'S/N'
                            
                            # City é obrigatório - usar string vazia se não houver cidade
                            city_value = self.clean_string(row[6], 100) if row[6] and str(row[6]).strip() else ''
                            
                            # State é obrigatório - usar string vazia se não houver UF
                            state_value = self.clean_string(row[8], 2) if row[8] and str(row[8]).strip() else ''
                            
                            # Street é obrigatório - usar string vazia se não houver endereço
                            street_value = self.clean_string(row[1], 500) or ''
                            
                            # Neighborhood é obrigatório - usar string vazia se não houver bairro
                            neighborhood_value = self.clean_string(row[4], 100) or ''
                            
                            cursor_pg.execute(insert_query, (
                                str(uuid.uuid4()),  # id (gerado automaticamente)
                                legado_id,  # legacy_id
                                str(customer_id),  # addressable_id (UUID do customer)
                                'customers',  # addressable_type
                                'main',  # type
                                cep,  # postal_code (obrigatório, usar '00000000' se vazio)
                                street_value,  # street (obrigatório, usar '' se vazio)
                                number_value,  # number (obrigatório, usar 'S/N' se vazio)
                                self.clean_string(row[3], 200),  # address_line_2 (Complemento - pode ser NULL)
                                neighborhood_value,  # neighborhood (obrigatório, usar '' se vazio)
                                city_value,  # city (obrigatório, usar '' se vazio)
                                state_value,  # state (obrigatório, usar '' se vazio)
                                municipal_code,  # municipal_code (obrigatório, usar 0 se vazio)
                                lat,  # latitude
                                lon,  # longitude
                                zone_value,  # zone (obrigatório)
                                region_value,  # region (obrigatório)
                                row[19],  # created_at
                                row[20] if row[20] else row[19]  # updated_at
                            ))
                            
                            self.stats['addresses'] += 1
                            
                        except Exception as e:
                            error_msg = f"Erro ao inserir endereco principal cliente Id={legado_id}: {e}"
                            logger.error(error_msg)
                            print(f"ERRO - {error_msg}")
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            # Fazer rollback imediato para não abortar toda a transação
                            try:
                                conn_pg.rollback()
                                cursor_pg = conn_pg.cursor()
                            except Exception as rollback_error:
                                logger.error(f"Erro ao fazer rollback: {rollback_error}")
                            continue
                    
                    # Endereço de Cobrança
                    if row[9] and str(row[9]).strip():  # EnderecoCobranca
                        try:
                            import re
                            cep = re.sub(r'[^\d]', '', str(row[13])) if row[13] else None
                            if not cep or cep == '0' * len(cep):
                                cep = '00000000'  # Valor padrão (campo é NOT NULL)
                            
                            # Converter CodigoMunicipioCobranca para integer se possível
                            municipal_code = 0  # Valor padrão (campo é NOT NULL)
                            if row[15]:
                                try:
                                    codigo_str = re.sub(r'[^\d]', '', str(row[15]))
                                    if codigo_str:
                                        municipal_code = int(codigo_str[:10])
                                except:
                                    pass
                            
                            insert_query = """
                            INSERT INTO gmcore.addresses (
                                id, legacy_id, addressable_id, addressable_type, type,
                                postal_code, street, number, address_line_2, neighborhood,
                                city, state, municipal_code, latitude, longitude, zone, region,
                                created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            
                            # Zone e Region são obrigatórios - usar neighborhood como zone e string vazia como region
                            zone_value = self.clean_string(row[12], 100) or ''  # BairroCobranca como zone (garantir que nunca seja None)
                            region_value = ''  # region vazio (não temos na origem)
                            
                            # Number é obrigatório - usar 'S/N' se não houver número
                            number_value = self.clean_string(row[10], 20) if row[10] and str(row[10]).strip() else 'S/N'
                            
                            # City é obrigatório - usar string vazia se não houver cidade
                            city_value = self.clean_string(row[14], 100) if row[14] and str(row[14]).strip() else ''
                            
                            # State é obrigatório - usar string vazia se não houver UF
                            state_value = self.clean_string(row[16], 2) if row[16] and str(row[16]).strip() else ''
                            
                            # Street é obrigatório - usar string vazia se não houver endereço
                            street_value = self.clean_string(row[9], 500) or ''
                            
                            # Neighborhood é obrigatório - usar string vazia se não houver bairro
                            neighborhood_value = self.clean_string(row[12], 100) or ''
                            
                            cursor_pg.execute(insert_query, (
                                str(uuid.uuid4()),
                                legado_id,
                                str(customer_id),
                                'customers',
                                'billing',  # type = billing
                                cep,  # postal_code (obrigatório, usar '00000000' se vazio)
                                street_value,  # street (obrigatório, usar '' se vazio)
                                number_value,  # number (obrigatório, usar 'S/N' se vazio)
                                self.clean_string(row[11], 200),  # ComplementoCobranca (pode ser NULL)
                                neighborhood_value,  # neighborhood (obrigatório, usar '' se vazio)
                                city_value,  # city (obrigatório, usar '' se vazio)
                                state_value,  # state (obrigatório, usar '' se vazio)
                                municipal_code,  # municipal_code (obrigatório, usar 0 se vazio)
                                None,  # latitude (não temos para endereço de cobrança)
                                None,  # longitude (não temos para endereço de cobrança)
                                zone_value,  # zone (obrigatório)
                                region_value,  # region (obrigatório)
                                row[19],  # created_at
                                row[20] if row[20] else row[19]  # updated_at
                            ))
                            
                            self.stats['addresses'] += 1
                            
                        except Exception as e:
                            error_msg = f"Erro ao inserir endereco cobranca cliente Id={legado_id}: {e}"
                            logger.error(error_msg)
                            print(f"ERRO - {error_msg}")
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            # Fazer rollback imediato para não abortar toda a transação
                            try:
                                conn_pg.rollback()
                                cursor_pg = conn_pg.cursor()
                            except Exception as rollback_error:
                                logger.error(f"Erro ao fazer rollback: {rollback_error}")
                            continue
                
                # Commit do chunk apenas se não houve erro crítico
                try:
                    conn_pg.commit()
                    total_processed = self.stats['addresses']
                    print(f"[ETAPA 3] Chunk {chunk_num} processado: {total_processed} enderecos inseridos, {chunk_errors} erros")
                    logger.info(f"[ETAPA 3] Chunk {chunk_num} processado: {total_processed} enderecos inseridos, {chunk_errors} erros")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            print(f"\n[ETAPA 3] CONCLUIDA! Total de addresses migrados: {self.stats['addresses']}")
            logger.info(f"ETAPA 3 concluida: {self.stats['addresses']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step3_addresses()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 3: {e}")
            raise
        finally:
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
    
    def validate_step4_contacts(self):
        """Validação e relatório de qualidade - ETAPA 4"""
        print("\n" + "-"*80)
        print("RELATORIO DE QUALIDADE - ETAPA 4: CONTACTS")
        print("-"*80)
        
        try:
            conn_sql = DatabaseConnection.get_sql_server_prd_connection()
            cursor_sql = conn_sql.cursor()
            
            # Aplicar limite se especificado
            limit_clause = ""
            if self.limit_rows > 0:
                limit_clause = f" OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY"
            
            # Contar origem - emails (aplicando mesmo filtro e limite da migração)
            query_email = f"""
                SELECT COUNT(*) 
                FROM (
                    SELECT Id, Email, Telefone, Celular
                    FROM Cliente
                    WHERE (Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
                       OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
                       OR (Celular IS NOT NULL AND LTRIM(RTRIM(Celular)) != '')
                    ORDER BY Id
                    {limit_clause}
                ) AS limited
                WHERE Email IS NOT NULL AND LTRIM(RTRIM(Email)) != ''
            """
            cursor_sql.execute(query_email)
            origem_email = cursor_sql.fetchone()[0]
            
            # Contar origem - telefones
            query_phone = f"""
                SELECT COUNT(*) 
                FROM (
                    SELECT Id, Email, Telefone, Celular
                    FROM Cliente
                    WHERE (Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
                       OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
                       OR (Celular IS NOT NULL AND LTRIM(RTRIM(Celular)) != '')
                    ORDER BY Id
                    {limit_clause}
                ) AS limited
                WHERE Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != ''
            """
            cursor_sql.execute(query_phone)
            origem_phone = cursor_sql.fetchone()[0]
            
            # Contar origem - celulares
            query_cellphone = f"""
                SELECT COUNT(*) 
                FROM (
                    SELECT Id, Email, Telefone, Celular
                    FROM Cliente
                    WHERE (Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
                       OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
                       OR (Celular IS NOT NULL AND LTRIM(RTRIM(Celular)) != '')
                    ORDER BY Id
                    {limit_clause}
                ) AS limited
                WHERE Celular IS NOT NULL AND LTRIM(RTRIM(Celular)) != ''
            """
            cursor_sql.execute(query_cellphone)
            origem_cellphone = cursor_sql.fetchone()[0]
            
            origem_total = origem_email + origem_phone + origem_cellphone
            cursor_sql.close()
            conn_sql.close()
            
            # Contar destino
            conn_pg = DatabaseConnection.get_postgresql_hml_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute("SELECT COUNT(*) FROM gmcore.contacts WHERE contactable_type = 'customers'")
            destino_total = cursor_pg.fetchone()[0]
            
            # Contar por tipo
            cursor_pg.execute("SELECT COUNT(*) FROM gmcore.contacts WHERE contactable_type = 'customers' AND type = 'email'")
            destino_email = cursor_pg.fetchone()[0]
            
            cursor_pg.execute("SELECT COUNT(*) FROM gmcore.contacts WHERE contactable_type = 'customers' AND type = 'phone'")
            destino_phone = cursor_pg.fetchone()[0]
            
            cursor_pg.execute("SELECT COUNT(*) FROM gmcore.contacts WHERE contactable_type = 'customers' AND type = 'cellphone'")
            destino_cellphone = cursor_pg.fetchone()[0]
            
            # Verificar contactable_id preenchido
            cursor_pg.execute("SELECT COUNT(*) FROM gmcore.contacts WHERE contactable_type = 'customers' AND contactable_id IS NOT NULL")
            com_contactable_id = cursor_pg.fetchone()[0]
            
            cursor_pg.close()
            conn_pg.close()
            
            print(f"\nORIGEM (SQL Server PRD - Cliente):")
            print(f"  Emails: {origem_email}")
            print(f"  Telefones: {origem_phone}")
            print(f"  Celulares: {origem_cellphone}")
            print(f"  Total esperado: {origem_total}")
            
            print(f"\nDESTINO (PostgreSQL HML - gmcore.contacts):")
            print(f"  Emails (type='email'): {destino_email}")
            print(f"  Telefones (type='phone'): {destino_phone}")
            print(f"  Celulares (type='cellphone'): {destino_cellphone}")
            print(f"  Total inserido: {destino_total}")
            print(f"  Com contactable_id: {com_contactable_id}")
            
            diferenca = origem_total - destino_total
            
            if diferenca == 0:
                print(f"\nOK - Todos os contacts foram migrados com sucesso!")
                logger.info(f"VALIDACAO ETAPA 4: OK - Origem: {origem_total}, Destino: {destino_total}")
            else:
                print(f"\nAVISO - Diferenca encontrada: {diferenca} contacts")
                print(f"  {abs(diferenca)} contacts {'faltando' if diferenca > 0 else 'extras'} no destino")
                logger.warning(f"VALIDACAO ETAPA 4: Diferenca - Origem: {origem_total}, Destino: {destino_total}, Diferenca: {diferenca}")
            
            return diferenca == 0
            
        except Exception as e:
            logger.error(f"Erro na validacao ETAPA 4: {e}")
            print(f"ERRO na validacao: {e}")
            return False
    
    def step4_migrate_contacts(self):
        """ETAPA 4: Migrar contacts"""
        print("\n" + "="*80)
        print("ETAPA 4: MIGRANDO CONTACTS")
        print("="*80)
        logger.info("="*80)
        logger.info("ETAPA 4: Migrando contacts")
        logger.info("="*80)
        
        # Truncate apenas contacts de customers
        print("\n[ETAPA 4] Limpando contacts de customers...")
        conn_pg = DatabaseConnection.get_postgresql_hml_connection()
        cursor_pg = conn_pg.cursor()
        cursor_pg.execute("DELETE FROM gmcore.contacts WHERE contactable_type = 'customers'")
        conn_pg.commit()
        cursor_pg.close()
        conn_pg.close()
        
        # Buscar dados do SQL Server
        print("[ETAPA 4] Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            Id,
            Email,
            Telefone,
            Celular,
            DataInclusao,
            DataAlteracao
        FROM Cliente
        WHERE (Email IS NOT NULL AND LTRIM(RTRIM(Email)) != '')
           OR (Telefone IS NOT NULL AND LTRIM(RTRIM(Telefone)) != '')
           OR (Celular IS NOT NULL AND LTRIM(RTRIM(Celular)) != '')
        ORDER BY Id
        """
        
        # Adicionar LIMIT se especificado
        if self.limit_rows > 0:
            sql_query = sql_query.replace("ORDER BY Id", f"ORDER BY Id OFFSET 0 ROWS FETCH NEXT {self.limit_rows} ROWS ONLY")
        
        conn_sql = DatabaseConnection.get_sql_server_prd_connection()
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute(sql_query)
        
        # Processar em chunks
        conn_pg = DatabaseConnection.get_postgresql_hml_connection()
        cursor_pg = conn_pg.cursor()
        
        chunk_num = 0
        total_processed = 0
        
        try:
            rows_remaining = self.limit_rows if self.limit_rows > 0 else None
            while True:
                fetch_size = CHUNK_SIZE
                if rows_remaining is not None and rows_remaining > 0:
                    fetch_size = min(CHUNK_SIZE, rows_remaining)
                
                rows = cursor_sql.fetchmany(fetch_size)
                if not rows:
                    break
                
                if rows_remaining is not None:
                    rows_remaining -= len(rows)
                    if rows_remaining <= 0:
                        # Processar ultimo chunk e parar
                        pass
                
                chunk_num += 1
                print(f"[ETAPA 4] Processando chunk {chunk_num} ({len(rows)} registros)...")
                logger.info(f"[ETAPA 4] Processando chunk {chunk_num} ({len(rows)} registros)...")
                
                chunk_errors = 0
                for row in rows:
                    legado_id = row[0]
                    customer_id = self.customer_id_map.get(legado_id)
                    
                    if not customer_id:
                        continue
                    
                    import re
                    data_inclusao = row[4]
                    data_alteracao = row[5] if row[5] else row[4]
                    
                    # REGISTRO 1 - Email
                    if row[1] and str(row[1]).strip():
                        try:
                            email_value = str(row[1]).strip().lower()
                            
                            insert_query = """
                            INSERT INTO gmcore.contacts (
                                id, contactable_id, contactable_type, type, value,
                                created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """
                            
                            cursor_pg.execute(insert_query, (
                                str(uuid.uuid4()),  # id
                                str(customer_id),  # contactable_id
                                'customers',  # contactable_type
                                'email',  # type
                                email_value,  # value (em minusculo)
                                data_inclusao,  # created_at
                                data_alteracao  # updated_at
                            ))
                            
                            self.stats['contacts'] += 1
                            total_processed += 1
                            
                        except Exception as e:
                            error_msg = f"Erro ao inserir email para Cliente Id={legado_id}: {e}"
                            logger.error(error_msg)
                            print(f"ERRO - {error_msg}")
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            try:
                                conn_pg.rollback()
                                cursor_pg = conn_pg.cursor()
                            except Exception as rollback_error:
                                logger.error(f"Erro ao fazer rollback: {rollback_error}")
                    
                    # REGISTRO 2 - Telefone
                    if row[2] and str(row[2]).strip():
                        try:
                            telefone_value = re.sub(r'[^\d]', '', str(row[2]))
                            
                            if telefone_value:
                                insert_query = """
                                INSERT INTO gmcore.contacts (
                                    id, contactable_id, contactable_type, type, value,
                                    created_at, updated_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """
                                
                                cursor_pg.execute(insert_query, (
                                    str(uuid.uuid4()),  # id
                                    str(customer_id),  # contactable_id
                                    'customers',  # contactable_type
                                    'phone',  # type
                                    telefone_value,  # value (apenas numeros)
                                    data_inclusao,  # created_at
                                    data_alteracao  # updated_at
                                ))
                                
                                self.stats['contacts'] += 1
                                total_processed += 1
                            
                        except Exception as e:
                            error_msg = f"Erro ao inserir telefone para Cliente Id={legado_id}: {e}"
                            logger.error(error_msg)
                            print(f"ERRO - {error_msg}")
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            try:
                                conn_pg.rollback()
                                cursor_pg = conn_pg.cursor()
                            except Exception as rollback_error:
                                logger.error(f"Erro ao fazer rollback: {rollback_error}")
                    
                    # REGISTRO 3 - Celular
                    if row[3] and str(row[3]).strip():
                        try:
                            celular_value = re.sub(r'[^\d]', '', str(row[3]))
                            
                            if celular_value:
                                insert_query = """
                                INSERT INTO gmcore.contacts (
                                    id, contactable_id, contactable_type, type, value,
                                    created_at, updated_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """
                                
                                cursor_pg.execute(insert_query, (
                                    str(uuid.uuid4()),  # id
                                    str(customer_id),  # contactable_id
                                    'customers',  # contactable_type
                                    'cellphone',  # type
                                    celular_value,  # value (apenas numeros)
                                    data_inclusao,  # created_at
                                    data_alteracao  # updated_at
                                ))
                                
                                self.stats['contacts'] += 1
                                total_processed += 1
                            
                        except Exception as e:
                            error_msg = f"Erro ao inserir celular para Cliente Id={legado_id}: {e}"
                            logger.error(error_msg)
                            print(f"ERRO - {error_msg}")
                            self.stats['errors'].append(error_msg)
                            chunk_errors += 1
                            try:
                                conn_pg.rollback()
                                cursor_pg = conn_pg.cursor()
                            except Exception as rollback_error:
                                logger.error(f"Erro ao fazer rollback: {rollback_error}")
                
                # Commit do chunk apenas se não houve erro crítico
                try:
                    conn_pg.commit()
                    print(f"[ETAPA 4] Chunk {chunk_num} processado: {total_processed} contacts inseridos, {chunk_errors} erros")
                    logger.info(f"[ETAPA 4] Chunk {chunk_num} processado: {total_processed} contacts inseridos, {chunk_errors} erros")
                except Exception as commit_error:
                    logger.error(f"Erro no commit do chunk {chunk_num}: {commit_error}")
                    conn_pg.rollback()
                    cursor_pg = conn_pg.cursor()
            
            print(f"\n[ETAPA 4] CONCLUIDA! Total de contacts migrados: {self.stats['contacts']}")
            logger.info(f"ETAPA 4 concluida: {self.stats['contacts']} registros")
            
            # Validação e relatório de qualidade
            self.validate_step4_contacts()
            
        except Exception as e:
            conn_pg.rollback()
            logger.error(f"Erro na ETAPA 4: {e}")
            raise
        finally:
            cursor_sql.close()
            conn_sql.close()
            cursor_pg.close()
            conn_pg.close()
    
    def run(self):
        """Executa a migração completa"""
        print("\n" + "="*80)
        print("INICIANDO MIGRACAO: SQL Server PRD -> PostgreSQL HML (gmcore)")
        print("="*80)
        print(f"Data/Hora: {datetime.now()}")
        print("="*80)
        logger.info("="*80)
        logger.info("INICIANDO MIGRACAO")
        logger.info(f"Data/Hora: {datetime.now()}")
        logger.info("="*80)
        
        start_time = datetime.now()
        
        try:
            # ETAPA 1: Customer Segments (primeiro, pois pode ser referenciado)
            self.step1_migrate_customer_segments()
            
            # ETAPA 2: Customers (segundo, pois addresses referencia customers)
            self.step2_migrate_customers()
            
            # ETAPA 3: Addresses (terceiro, referencia customers)
            self.step3_migrate_addresses()
            
            # ETAPA 4: Contacts (quarto, referencia customers)
            self.step4_migrate_contacts()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            print("\n" + "="*80)
            print("MIGRACAO CONCLUIDA COM SUCESSO!")
            print("="*80)
            logger.info("="*80)
            logger.info("MIGRACAO CONCLUIDA COM SUCESSO!")
            logger.info("="*80)
            
            print(f"\nDuracao total: {duration}")
            print(f"\nESTATISTICAS FINAIS:")
            print(f"  Customer Segments: {self.stats['customer_segments']}")
            print(f"  Customers: {self.stats['customers']}")
            print(f"  Addresses: {self.stats['addresses']}")
            print(f"  Contacts: {self.stats['contacts']}")
            print(f"  Erros: {len(self.stats['errors'])}")
            
            logger.info(f"Duracao: {duration}")
            logger.info(f"Customer Segments: {self.stats['customer_segments']}")
            logger.info(f"Customers: {self.stats['customers']}")
            logger.info(f"Addresses: {self.stats['addresses']}")
            logger.info(f"Contacts: {self.stats['contacts']}")
            logger.info(f"Erros: {len(self.stats['errors'])}")
            
            if self.stats['errors']:
                print(f"\nAVISO - Total de erros: {len(self.stats['errors'])}")
                print("Verifique o arquivo customers_to_core_log.txt para detalhes")
                logger.warning(f"Total de erros: {len(self.stats['errors'])}")
            else:
                print("\nOK - Nenhum erro encontrado!")
                logger.info("Nenhum erro encontrado!")
            
        except Exception as e:
            logger.error("="*80)
            logger.error("ERRO CRITICO NA MIGRACAO!")
            logger.error("="*80)
            logger.error(f"Erro: {str(e)}")
            print(f"\nERRO CRITICO: {e}")
            raise


if __name__ == "__main__":
    import sys
    
    # Verificar se foi passado limite via argumento
    limit_rows = 0
    if '--limit' in sys.argv:
        idx = sys.argv.index('--limit')
        if idx + 1 < len(sys.argv):
            try:
                limit_rows = int(sys.argv[idx + 1])
            except ValueError:
                print("AVISO: Valor invalido para --limit, usando 0 (todos os dados)")
    
    migration = CustomersMigration(limit_rows=limit_rows)
    
    # Verificar se deve executar apenas uma etapa específica
    if len(sys.argv) > 1 and sys.argv[1] == "--step3":
        print("\n" + "="*80)
        print("EXECUTANDO APENAS ETAPA 3: MIGRACAO DE ADDRESSES")
        print("="*80)
        print(f"Data/Hora: {datetime.now()}")
        print("="*80)
        logger.info("="*80)
        logger.info("EXECUTANDO APENAS ETAPA 3: MIGRACAO DE ADDRESSES")
        logger.info(f"Data/Hora: {datetime.now()}")
        logger.info("="*80)
        
        start_time = datetime.now()
        
        try:
            # Carregar mapeamento de customer IDs (necessário para addresses)
            print("\n[ETAPA 3] Carregando mapeamento de customers...")
            conn_pg = DatabaseConnection.get_postgresql_hml_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute("SELECT id, legacy_id FROM gmcore.customers")
            for row in cursor_pg.fetchall():
                migration.customer_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            print(f"[ETAPA 3] {len(migration.customer_id_map)} customers carregados")
            
            # Executar apenas a etapa 3
            migration.step3_migrate_addresses()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            print("\n" + "="*80)
            print("ETAPA 3 CONCLUIDA!")
            print("="*80)
            logger.info("="*80)
            logger.info("ETAPA 3 CONCLUIDA!")
            logger.info("="*80)
            
            print(f"\nDuracao: {duration}")
            print(f"\nESTATISTICAS ETAPA 3:")
            print(f"  Addresses: {migration.stats['addresses']}")
            print(f"  Erros: {len(migration.stats['errors'])}")
            
            logger.info(f"Duracao: {duration}")
            logger.info(f"Addresses: {migration.stats['addresses']}")
            logger.info(f"Erros: {len(migration.stats['errors'])}")
            
            if migration.stats['errors']:
                print(f"\nAVISO - Total de erros: {len(migration.stats['errors'])}")
                print("Verifique o arquivo customers_to_core_log.txt para detalhes")
                logger.warning(f"Total de erros: {len(migration.stats['errors'])}")
            else:
                print("\nOK - Nenhum erro encontrado!")
                logger.info("Nenhum erro encontrado!")
                
        except Exception as e:
            logger.error("="*80)
            logger.error("ERRO CRITICO NA ETAPA 3!")
            logger.error("="*80)
            logger.error(f"Erro: {str(e)}")
            print(f"\nERRO CRITICO: {e}")
            raise
    elif len(sys.argv) > 1 and sys.argv[1] == "--step4":
        print("\n" + "="*80)
        print("EXECUTANDO APENAS ETAPA 4: MIGRACAO DE CONTACTS")
        print("="*80)
        print(f"Data/Hora: {datetime.now()}")
        print("="*80)
        logger.info("="*80)
        logger.info("EXECUTANDO APENAS ETAPA 4: MIGRACAO DE CONTACTS")
        logger.info(f"Data/Hora: {datetime.now()}")
        logger.info("="*80)
        
        start_time = datetime.now()
        
        try:
            # Carregar mapeamento de customer IDs (necessário para contacts)
            print("\n[ETAPA 4] Carregando mapeamento de customers...")
            conn_pg = DatabaseConnection.get_postgresql_hml_connection()
            cursor_pg = conn_pg.cursor()
            cursor_pg.execute("SELECT id, legacy_id FROM gmcore.customers")
            for row in cursor_pg.fetchall():
                migration.customer_id_map[row[1]] = row[0]
            cursor_pg.close()
            conn_pg.close()
            print(f"[ETAPA 4] {len(migration.customer_id_map)} customers carregados")
            
            # Executar apenas a etapa 4
            migration.step4_migrate_contacts()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            print("\n" + "="*80)
            print("ETAPA 4 CONCLUIDA!")
            print("="*80)
            logger.info("="*80)
            logger.info("ETAPA 4 CONCLUIDA!")
            logger.info("="*80)
            
            print(f"\nDuracao: {duration}")
            print(f"\nESTATISTICAS ETAPA 4:")
            print(f"  Contacts: {migration.stats['contacts']}")
            print(f"  Erros: {len(migration.stats['errors'])}")
            
            logger.info(f"Duracao: {duration}")
            logger.info(f"Contacts: {migration.stats['contacts']}")
            logger.info(f"Erros: {len(migration.stats['errors'])}")
            
            if migration.stats['errors']:
                print(f"\nAVISO - Total de erros: {len(migration.stats['errors'])}")
                print("Verifique o arquivo customers_to_core_log.txt para detalhes")
                logger.warning(f"Total de erros: {len(migration.stats['errors'])}")
            else:
                print("\nOK - Nenhum erro encontrado!")
                logger.info("Nenhum erro encontrado!")
                
        except Exception as e:
            logger.error("="*80)
            logger.error("ERRO CRITICO NA ETAPA 4!")
            logger.error("="*80)
            logger.error(f"Erro: {str(e)}")
            print(f"\nERRO CRITICO: {e}")
            raise
    else:
        # Executar migração completa
        migration.run()

