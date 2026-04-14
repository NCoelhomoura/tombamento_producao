"""
Script de Migração: Cliente (SQL Server) → Core (PostgreSQL)
Executa a migração completa conforme o PLANO_MIGRACAO.md
"""

import uuid
import re
import sys
import os
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import logging

# Adicionar diretório utils ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database_connection import DatabaseConnection
from municipio_lookup import load_municipio_lookup, city_code_legacy_str

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class ClienteMigration:
    """Classe para executar a migração de Cliente para Core"""
    
    def __init__(self):
        self.stats = {
            'customers': 0,
            'address_main': 0,
            'address_billing': 0,
            'contacts': 0,
            'emails': 0,
            'phones': 0,
            'shareholders': 0,
            'shareholder_addresses': 0,
            'shareholder_phones': 0,
            'shareholder_pii': 0,
            'errors': []
        }
    
    def clean_string(self, value: Optional[str], max_length: Optional[int] = None) -> Optional[str]:
        """Limpa e trunca string"""
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            return None
        if max_length and len(cleaned) > max_length:
            cleaned = cleaned[:max_length]
        return cleaned
    
    def clean_cpf_cnpj(self, value: Optional[str]) -> Optional[str]:
        """Remove formatação de CPF/CNPJ e retorna como string (não INT para evitar overflow)"""
        if not value:
            return None
        cleaned = re.sub(r'[^\d]', '', str(value))
        if not cleaned or cleaned == '0' * len(cleaned):
            return None
        # Retornar como string para evitar overflow em BIGINT
        return cleaned
    
    def clean_phone(self, value: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        """Extrai DDD e número do telefone"""
        if not value:
            return None, None
        cleaned = re.sub(r'[^\d]', '', str(value))
        # Telefone brasileiro válido: 10 dígitos (fixo) ou 11 dígitos (celular)
        # Ignorar valores muito curtos (< 10) ou muito longos (> 11)
        if len(cleaned) < 10 or len(cleaned) > 11:
            return None, None
        # Ignorar se for apenas zeros
        if cleaned == '0' * len(cleaned):
            return None, None
        area_code = cleaned[:2] if len(cleaned) >= 2 else None
        number = cleaned[2:] if len(cleaned) > 2 else None
        return area_code, number
    
    def split_emails(self, value: Optional[str]) -> List[str]:
        """Divide string de emails separados por vírgula"""
        if not value:
            return []
        emails = [e.strip().lower() for e in str(value).split(',')]
        # Filtrar emails válidos: deve ter '@', não ser "PENDENTE" ou valores inválidos comuns
        valid_emails = []
        invalid_patterns = ['pendente', 'n/a', 'na', 'null', 'none', '']
        for e in emails:
            if e and '@' in e and e not in invalid_patterns:
                # Validar formato básico de email
                if self.is_valid_email(e):
                    valid_emails.append(e)
        return valid_emails
    
    def is_valid_email(self, email: str) -> bool:
        """Valida formato básico de email"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    def convert_status(self, ativo: Optional[bool]) -> str:
        """Converte status booleano para string"""
        if ativo is True:
            return 'active'
        return 'inactive'
    
    def convert_marital_status(self, est_civil: Optional[int]) -> Optional[str]:
        """Converte código de estado civil"""
        mapping = {
            1: 'Solteiro',
            2: 'Casado',
            3: 'Divorciado',
            4: 'Viúvo'
        }
        return mapping.get(est_civil)
    
    def execute_query(self, query: str, is_postgres: bool = True, description: str = "") -> List[Dict]:
        """Executa query e trata erros"""
        try:
            if is_postgres:
                return DatabaseConnection.execute_postgresql_query(query)
            else:
                return DatabaseConnection.execute_sql_server_query(query)
        except Exception as e:
            error_msg = f"Erro ao executar {description}: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            raise
    
    def step1_migrate_customers(self) -> Dict[str, uuid.UUID]:
        """ETAPA 1: Migrar tabela principal customers"""
        print("\n" + "="*80)
        print(">>> ETAPA 1: MIGRANDO TABELA CUSTOMERS")
        print("="*80)
        logger.info("="*60)
        logger.info("ETAPA 1: Migrando tabela customers")
        logger.info("="*60)
        
        # Buscar dados do SQL Server
        print("[ETAPA 1] Buscando dados do SQL Server...")
        logger.info("Buscando dados do SQL Server...")
        sql_query = """
        SELECT 
            Id, Codigo, NomeSistema, TipoPessoa, CpfCnpj,
            InscricaoEstadual, InscricaoMunicipal, RazaoSocial, NomeFantasia,
            Ativo, DataInclusao, DataAlteracao, DataAtivacao, Observacoes
        FROM Cliente
        ORDER BY Id
        """
        
        clientes = self.execute_query(sql_query, is_postgres=False, description="buscar clientes do SQL Server")
        print(f"[ETAPA 1] Encontrados {len(clientes)} clientes no SQL Server")
        logger.info(f"Encontrados {len(clientes)} clientes no SQL Server")
        
        logger.info("Limpando tabelas na tabela destino...")

        sql_query_truncate = """
        TRUNCATE TABLE core.customers CASCADE;
        """
        self.execute_query(sql_query_truncate, is_postgres=True, description="truncar tabela customers")

        # Mapear customer_id para legado_cliente_id
        customer_id_map = {}
        
        # Inserir em lotes
        batch_size = 100
        total_batches = (len(clientes) + batch_size - 1) // batch_size
        print(f"[ETAPA 1] Iniciando insercao em {total_batches} lotes de {batch_size} registros...")
        conn = DatabaseConnection.get_postgresql_connection()
        cursor = conn.cursor()
        
        try:
            for i in range(0, len(clientes), batch_size):
                batch = clientes[i:i+batch_size]
                batch_num = i//batch_size + 1
                print(f"[ETAPA 1] Processando lote {batch_num}/{total_batches} ({len(batch)} registros)...")
                logger.info(f"Processando lote {batch_num} ({len(batch)} registros)...")
                
                for cliente in batch:
                    try:
                        customer_id = uuid.uuid4()
                        customer_id_map[cliente['Id']] = customer_id
                        
                        # Debug: mostrar primeiro cliente
                        if self.stats['customers'] == 0:
                            print(f"[ETAPA 1] DEBUG - Primeiro cliente: Id={cliente.get('Id')}, CpfCnpj={cliente.get('CpfCnpj')}")
                        
                        # Preparar dados
                        tax_id_str = self.clean_cpf_cnpj(cliente.get('CpfCnpj'))
                        tax_id = None
                        # O campo tax_id é VARCHAR(14) no PostgreSQL, pode armazenar CPF (11 dígitos) e CNPJ (14 dígitos)
                        if tax_id_str:
                            # Validar tamanho: CPF tem 11 dígitos, CNPJ tem 14 dígitos
                            if len(tax_id_str) <= 14:
                                tax_id = tax_id_str  # Armazenar como string
                            else:
                                # Se tiver mais de 14 dígitos, truncar (caso raro)
                                tax_id = tax_id_str[:14]
                                if self.stats['customers'] < 3:  # Mostrar apenas nos primeiros
                                    print(f"[ETAPA 1] AVISO - CPF/CNPJ muito grande, truncado para Id={cliente.get('Id')}: {len(tax_id_str)} dígitos")
                        
                        tax_id_type = None
                        if cliente.get('TipoPessoa') == 'F':
                            tax_id_type = 'CPF'
                        elif cliente.get('TipoPessoa') == 'J':
                            tax_id_type = 'CNPJ'
                        
                        registration_state = None
                        if cliente.get('InscricaoEstadual'):
                            try:
                                # Limpar formatação (pontos, traços, etc)
                                insc_est_str = re.sub(r'[^\d]', '', str(cliente['InscricaoEstadual']))
                                if insc_est_str and insc_est_str != '0' * len(insc_est_str):
                                    # Limite de INTEGER: 2.147.483.647 (10 dígitos)
                                    if len(insc_est_str) <= 10:
                                        insc_est_val = int(insc_est_str)
                                        if insc_est_val <= 2147483647:  # Limite de INTEGER
                                            registration_state = insc_est_val
                                        else:
                                            if self.stats['customers'] < 3:  # Mostrar apenas nos primeiros
                                                print(f"[ETAPA 1] AVISO - Inscrição Estadual excede INTEGER para Id={cliente.get('Id')}: {insc_est_str}")
                                    else:
                                        # Inscrição Estadual com mais de 10 dígitos excede INTEGER
                                        if self.stats['customers'] < 3:  # Mostrar apenas nos primeiros
                                            print(f"[ETAPA 1] AVISO - Inscrição Estadual muito grande para INTEGER (Id={cliente.get('Id')}): {len(insc_est_str)} dígitos")
                            except (ValueError, OverflowError, TypeError) as e:
                                if self.stats['customers'] < 3:  # Mostrar apenas nos primeiros
                                    print(f"[ETAPA 1] AVISO - Erro ao converter Inscrição Estadual para Id={cliente.get('Id')}: {str(e)}")
                        
                        registration_municipal = None
                        if cliente.get('InscricaoMunicipal'):
                            try:
                                # Limpar formatação (pontos, traços, etc)
                                insc_mun_str = re.sub(r'[^\d]', '', str(cliente['InscricaoMunicipal']))
                                if insc_mun_str and insc_mun_str != '0' * len(insc_mun_str):
                                    # Limite de INTEGER: 2.147.483.647 (10 dígitos)
                                    if len(insc_mun_str) <= 10:
                                        insc_mun_val = int(insc_mun_str)
                                        if insc_mun_val <= 2147483647:  # Limite de INTEGER
                                            registration_municipal = insc_mun_val
                                        else:
                                            if self.stats['customers'] < 3:  # Mostrar apenas nos primeiros
                                                print(f"[ETAPA 1] AVISO - Inscrição Municipal excede INTEGER para Id={cliente.get('Id')}: {insc_mun_str}")
                                    else:
                                        # Inscrição Municipal com mais de 10 dígitos excede INTEGER
                                        if self.stats['customers'] < 3:  # Mostrar apenas nos primeiros
                                            print(f"[ETAPA 1] AVISO - Inscrição Municipal muito grande para INTEGER (Id={cliente.get('Id')}): {len(insc_mun_str)} dígitos")
                            except (ValueError, OverflowError, TypeError) as e:
                                if self.stats['customers'] < 3:  # Mostrar apenas nos primeiros
                                    print(f"[ETAPA 1] AVISO - Erro ao converter Inscrição Municipal para Id={cliente.get('Id')}: {str(e)}")
                        
                        legal_name = self.clean_string(cliente.get('RazaoSocial'), 255)
                        trade_name = self.clean_string(cliente.get('NomeFantasia'), 255)
                        status = self.convert_status(cliente.get('Ativo'))
                        
                        insert_query = """
                        INSERT INTO core.customers (
                            customer_id, legado_cliente_id, legado_cliente_code,
                            legado__nomesistema, entity_type, tax_id_type, tax_id,
                            registration_state, registration_municipal,
                            legal_name, trade_name, status,
                            created_at, updated_at, activated_date, coment
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """
                        
                        cursor.execute(insert_query, (
                            str(customer_id),
                            cliente['Id'],
                            cliente.get('Codigo'),
                            self.clean_string(cliente.get('NomeSistema')),
                            cliente.get('TipoPessoa'),
                            tax_id_type,
                            tax_id,
                            registration_state,
                            registration_municipal,
                            legal_name,
                            trade_name,
                            status,
                            cliente.get('DataInclusao'),
                            cliente.get('DataAlteracao'),
                            cliente.get('DataAtivacao'),
                            self.clean_string(cliente.get('Observacoes'))
                        ))
                        
                        self.stats['customers'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir cliente Id={cliente.get('Id')}: {str(e)}"
                        print(f"[ETAPA 1] ERRO no cliente Id={cliente.get('Id')}: {str(e)}")
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                        # Fazer rollback apenas deste registro e continuar
                        try:
                            conn.rollback()
                            cursor = conn.cursor()
                        except:
                            pass
                        continue  # Pular este registro e continuar
                
                # Commit apenas se não houve erro crítico
                try:
                    conn.commit()
                except Exception as e:
                    print(f"[ETAPA 1] ERRO no commit: {str(e)}")
                    conn.rollback()
                    cursor = conn.cursor()
                print(f"[ETAPA 1] Lote {batch_num}/{total_batches} processado com sucesso!")
                logger.info(f"Lote {batch_num} processado com sucesso")
            
            print(f"[ETAPA 1] CONCLUIDA! Total de customers migrados: {self.stats['customers']}")
            logger.info(f"[OK] Migracao de customers concluida: {self.stats['customers']} registros")
            return customer_id_map
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Erro na migração de customers: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def step2_migrate_addresses(self, customer_id_map: Dict[int, uuid.UUID]):
        """ETAPA 2: Migrar endereços"""
        print("\n" + "="*80)
        print(">>> ETAPA 2: MIGRANDO ENDERECOS")
        print("="*80)
        logger.info("="*60)
        logger.info("ETAPA 2: Migrando endereços")
        logger.info("="*60)
        
        print("[ETAPA 2] Carregando lookup Municipio (origem SQL Server)...")
        conn_mun = DatabaseConnection.get_sql_server_prd_connection()
        cur_mun = conn_mun.cursor()
        try:
            municipio_lookup = load_municipio_lookup(cur_mun)
        finally:
            cur_mun.close()
            conn_mun.close()
        
        # Buscar dados do SQL Server
        print("[ETAPA 2] Buscando dados de enderecos do SQL Server...")
        sql_query = """
        SELECT 
            Id, Codigo, Endereco, Numero, Complemento, Bairro, CEP,
            Cidade, CodigoMunicipio, UF,
            EnderecoCobranca, NumeroCobranca, ComplementoCobranca,
            BairroCobranca, CepCobranca, CidadeCobranca,
            CodigoMunicipioCobranca, UFCobranca,
            DataInclusao, DataAlteracao
        FROM Cliente
        ORDER BY Id
        """
        
        clientes = self.execute_query(sql_query, is_postgres=False, description="buscar endereços do SQL Server")
        print(f"[ETAPA 2] Processando {len(clientes)} registros de enderecos...")
        
        conn = DatabaseConnection.get_postgresql_connection()
        cursor = conn.cursor()        
        
        logger.info("Limpando tabela address na tabela destino...")

        sql_query_truncate = """
        TRUNCATE TABLE core.address CASCADE;
        """
        self.execute_query(sql_query_truncate, is_postgres=True, description="truncar tabela address")
        
        logger.info("Tabela address limpa com sucesso!")
        
        try:
            processed = 0
            for cliente in clientes:
                processed += 1
                if processed % 500 == 0:
                    print(f"[ETAPA 2] Processados {processed}/{len(clientes)} registros...")
                cliente_id = cliente['Id']
                customer_id = customer_id_map.get(cliente_id)
                
                if not customer_id:
                    continue
                
                # Endereço Principal
                if cliente.get('Endereco') and cliente.get('Endereco').strip():
                    try:
                        cep = re.sub(r'[^\d]', '', str(cliente.get('CEP', ''))) if cliente.get('CEP') else None
                        if not cep or cep == '0' * len(cep):
                            cep = None
                        
                        insert_query = """
                        INSERT INTO core.address (
                            address_id, address_type, addressable_type, addressable_id,
                            legacy_cliente_id, legacy_cliente_code,
                            street, house_num, complement1, district,
                            cep, city, city_code, region_code, country,
                            status, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """
                        
                        cursor.execute(insert_query, (
                            str(uuid.uuid4()),
                            'main',
                            'customers',
                            cliente_id,  # addressable_id é BIGINT, usar legado_cliente_id
                            cliente_id,
                            cliente.get('Codigo'),
                            self.clean_string(cliente.get('Endereco'), 200),
                            self.clean_string(cliente.get('Numero'), 20),
                            self.clean_string(cliente.get('Complemento')),
                            self.clean_string(cliente.get('Bairro')),
                            cep,
                            self.clean_string(cliente.get('Cidade')),
                            city_code_legacy_str(
                                cliente.get('CodigoMunicipio'),
                                cliente.get('UF'),
                                cliente.get('Cidade'),
                                municipio_lookup=municipio_lookup,
                            ),
                            self.clean_string(cliente.get('UF'), 2),
                            'BR',
                            'active',
                            cliente.get('DataInclusao'),
                            cliente.get('DataAlteracao')
                        ))
                        
                        self.stats['address_main'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir endereço principal cliente Id={cliente_id}: {str(e)}"
                        print(f"[ETAPA 2] ERRO no endereço principal cliente Id={cliente_id}: {str(e)}")
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                        try:
                            conn.rollback()
                            cursor = conn.cursor()
                        except:
                            pass
                        # Continuar mesmo com erro
                
                # Endereço de Cobrança
                if cliente.get('EnderecoCobranca') and cliente.get('EnderecoCobranca').strip():
                    try:
                        cep = re.sub(r'[^\d]', '', str(cliente.get('CepCobranca', ''))) if cliente.get('CepCobranca') else None
                        if not cep or cep == '0' * len(cep):
                            cep = None
                        
                        insert_query = """
                        INSERT INTO core.address (
                            address_id, address_type, addressable_type, addressable_id,
                            legacy_cliente_id, legacy_cliente_code,
                            street, house_num, complement1, district,
                            cep, city, city_code, region_code, country,
                            status, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """
                        
                        cursor.execute(insert_query, (
                            str(uuid.uuid4()),
                            'billing',
                            'customers',
                            cliente_id,  # addressable_id é BIGINT, usar legado_cliente_id
                            cliente_id,
                            cliente.get('Codigo'),
                            self.clean_string(cliente.get('EnderecoCobranca'), 200),
                            self.clean_string(cliente.get('NumeroCobranca'), 20),
                            self.clean_string(cliente.get('ComplementoCobranca')),
                            self.clean_string(cliente.get('BairroCobranca')),
                            cep,
                            self.clean_string(cliente.get('CidadeCobranca')),
                            city_code_legacy_str(
                                cliente.get('CodigoMunicipioCobranca'),
                                cliente.get('UFCobranca'),
                                cliente.get('CidadeCobranca'),
                                municipio_lookup=municipio_lookup,
                            ),
                            self.clean_string(cliente.get('UFCobranca'), 2),
                            'BR',
                            'active',
                            cliente.get('DataInclusao'),
                            cliente.get('DataAlteracao')
                        ))
                        
                        self.stats['address_billing'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir endereço cobrança cliente Id={cliente_id}: {str(e)}"
                        print(f"[ETAPA 2] ERRO no endereço cobrança cliente Id={cliente_id}: {str(e)}")
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                        try:
                            conn.rollback()
                            cursor = conn.cursor()
                        except:
                            pass
                        # Continuar mesmo com erro
            
            # Commit ao final
            try:
                conn.commit()
            except Exception as e:
                print(f"[ETAPA 2] ERRO no commit: {str(e)}")
                conn.rollback()
                cursor = conn.cursor()
            print(f"[ETAPA 2] CONCLUIDA!")
            print(f"[ETAPA 2] Enderecos principais: {self.stats['address_main']}")
            print(f"[ETAPA 2] Enderecos de cobranca: {self.stats['address_billing']}")
            logger.info(f"[OK] Enderecos principais: {self.stats['address_main']}")
            logger.info(f"[OK] Enderecos de cobranca: {self.stats['address_billing']}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Erro na migração de endereços: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def step3_migrate_contacts(self, customer_id_map: Dict[int, uuid.UUID]):
        """ETAPA 3: Migrar contatos"""
        print("\n" + "="*80)
        print(">>> ETAPA 3: MIGRANDO CONTATOS")
        print("="*80)
        logger.info("="*60)
        logger.info("ETAPA 3: Migrando contatos")
        logger.info("="*60)
        
        print("[ETAPA 3] Buscando dados de contatos do SQL Server...")
        sql_query = """
        SELECT 
            Id, PessoaContato, FuncaoPgtoFaturamento,
            ResponsavelCompra, FuncaoResponsavel, Observacoes,
            DataInclusao, DataAlteracao
        FROM Cliente
        WHERE (PessoaContato IS NOT NULL AND LTRIM(RTRIM(PessoaContato)) != '')
           OR (ResponsavelCompra IS NOT NULL AND LTRIM(RTRIM(ResponsavelCompra)) != '')
        ORDER BY Id
        """
        
        clientes = self.execute_query(sql_query, is_postgres=False, description="buscar contatos do SQL Server")
        print(f"[ETAPA 3] Processando {len(clientes)} registros de contatos...")
        
        conn = DatabaseConnection.get_postgresql_connection()
        cursor = conn.cursor()
        
        logger.info("Limpando tabela contact na tabela destino...")

        sql_query_truncate = """
        TRUNCATE TABLE core.contact CASCADE;
        """
        self.execute_query(sql_query_truncate, is_postgres=True, description="truncar tabela contact")
        
        logger.info("Tabela contact limpa com sucesso!")

        try:
            processed = 0
            for cliente in clientes:
                processed += 1
                if processed % 500 == 0:
                    print(f"[ETAPA 3] Processados {processed}/{len(clientes)} registros...")
                cliente_id = cliente['Id']
                customer_id = customer_id_map.get(cliente_id)
                
                if not customer_id:
                    continue
                
                # Pessoa de Contato
                if cliente.get('PessoaContato') and cliente.get('PessoaContato').strip():
                    try:
                        insert_query = """
                        INSERT INTO core.contact (
                            contact_id, customer_id, contact_name,
                            job_title, coment, status, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        cursor.execute(insert_query, (
                            str(uuid.uuid4()),
                            str(customer_id),
                            self.clean_string(cliente.get('PessoaContato')),
                            self.clean_string(cliente.get('FuncaoPgtoFaturamento'), 200),
                            self.clean_string(cliente.get('Observacoes')),
                            'active',
                            cliente.get('DataInclusao'),
                            cliente.get('DataAlteracao')
                        ))
                        
                        self.stats['contacts'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir contato cliente Id={cliente_id}: {str(e)}"
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                
                # Responsável pela Compra
                if cliente.get('ResponsavelCompra') and cliente.get('ResponsavelCompra').strip():
                    try:
                        insert_query = """
                        INSERT INTO core.contact (
                            contact_id, customer_id, contact_name,
                            job_title, coment, status, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        cursor.execute(insert_query, (
                            str(uuid.uuid4()),
                            str(customer_id),
                            self.clean_string(cliente.get('ResponsavelCompra')),
                            self.clean_string(cliente.get('FuncaoResponsavel'), 200),
                            None,
                            'active',
                            cliente.get('DataInclusao'),
                            cliente.get('DataAlteracao')
                        ))
                        
                        self.stats['contacts'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir responsável cliente Id={cliente_id}: {str(e)}"
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
            
            conn.commit()
            print(f"[ETAPA 3] CONCLUIDA! Total de contatos migrados: {self.stats['contacts']}")
            logger.info(f"[OK] Contatos migrados: {self.stats['contacts']}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Erro na migração de contatos: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def step4_migrate_emails(self, customer_id_map: Dict[int, uuid.UUID]):
        """ETAPA 4: Migrar emails"""
        print("\n" + "="*80)
        print(">>> ETAPA 4: MIGRANDO EMAILS")
        print("="*80)
        logger.info("="*60)
        logger.info("ETAPA 4: Migrando emails")
        logger.info("="*60)
        
        print("[ETAPA 4] Buscando dados de emails do SQL Server...")
        sql_query = """
        SELECT 
            Id, Email, EmailResponsavel, EmailCobranca, EmailUsuarioComercialResponsavel,
            DataInclusao, DataAlteracao
        FROM Cliente
        WHERE Email IS NOT NULL OR EmailResponsavel IS NOT NULL 
           OR EmailCobranca IS NOT NULL OR EmailUsuarioComercialResponsavel IS NOT NULL
        ORDER BY Id
        """
        
        clientes = self.execute_query(sql_query, is_postgres=False, description="buscar emails do SQL Server")
        print(f"[ETAPA 4] Processando {len(clientes)} registros de emails...")
        
        conn = DatabaseConnection.get_postgresql_connection()
        cursor = conn.cursor()
        
        logger.info("Limpando tabela contact_email na tabela destino...")

        sql_query_truncate = """
        TRUNCATE TABLE core.contact_email CASCADE;
        """
        self.execute_query(sql_query_truncate, is_postgres=True, description="truncar tabela contact_email")
        
        logger.info("Tabela contact_email limpa com sucesso!")

        try:
            processed = 0
            for cliente in clientes:
                processed += 1
                if processed % 500 == 0:
                    print(f"[ETAPA 4] Processados {processed}/{len(clientes)} registros...")
                cliente_id = cliente['Id']
                customer_id = customer_id_map.get(cliente_id)
                
                if not customer_id:
                    continue
                
                # Email Principal
                if cliente.get('Email'):
                    email = self.clean_string(cliente.get('Email'))
                    if email:
                        email = email.lower()
                        # Validar formato de email e ignorar valores inválidos como "PENDENTE"
                        invalid_patterns = ['pendente', 'n/a', 'na', 'null', 'none']
                        if email not in invalid_patterns and self.is_valid_email(email):
                            try:
                                insert_query = """
                                INSERT INTO core.contact_email (
                                    contact_email_id, customer_id, email_type,
                                    email, status, created_at, updated_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """
                                
                                cursor.execute(insert_query, (
                                    str(uuid.uuid4()),
                                    str(customer_id),
                                    'main',
                                    email,
                                    'active',
                                    cliente.get('DataInclusao'),
                                    cliente.get('DataAlteracao')
                                ))
                                
                                self.stats['emails'] += 1
                                
                            except Exception as e:
                                error_msg = f"Erro ao inserir email principal cliente Id={cliente_id}: {str(e)}"
                                logger.error(error_msg)
                                self.stats['errors'].append(error_msg)
                
                # Email Responsável
                if cliente.get('EmailResponsavel'):
                    email = self.clean_string(cliente.get('EmailResponsavel'))
                    if email:
                        email = email.lower()
                        invalid_patterns = ['pendente', 'n/a', 'na', 'null', 'none']
                        if email not in invalid_patterns and self.is_valid_email(email):
                            try:
                                insert_query = """
                                INSERT INTO core.contact_email (
                                    contact_email_id, customer_id, email_type,
                                    email, status, created_at, updated_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """
                                
                                cursor.execute(insert_query, (
                                    str(uuid.uuid4()),
                                    str(customer_id),
                                    'responsible',
                                    email,
                                    'active',
                                    cliente.get('DataInclusao'),
                                    cliente.get('DataAlteracao')
                                ))
                                
                                self.stats['emails'] += 1
                                
                            except Exception as e:
                                error_msg = f"Erro ao inserir email responsável cliente Id={cliente_id}: {str(e)}"
                                logger.error(error_msg)
                                self.stats['errors'].append(error_msg)
                
                # Emails de Cobrança (pode ter múltiplos)
                if cliente.get('EmailCobranca'):
                    emails = self.split_emails(cliente.get('EmailCobranca'))
                    for email in emails:
                        # split_emails já valida o formato, então podemos inserir diretamente
                        try:
                            insert_query = """
                            INSERT INTO core.contact_email (
                                contact_email_id, customer_id, email_type,
                                email, status, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """
                            
                            cursor.execute(insert_query, (
                                str(uuid.uuid4()),
                                str(customer_id),
                                'billing',
                                email,
                                'active',
                                cliente.get('DataInclusao'),
                                cliente.get('DataAlteracao')
                            ))
                            
                            self.stats['emails'] += 1
                            
                        except Exception as e:
                            error_msg = f"Erro ao inserir email cobrança cliente Id={cliente_id}: {str(e)}"
                            logger.error(error_msg)
                            self.stats['errors'].append(error_msg)
                
                # Email Comercial
                if cliente.get('EmailUsuarioComercialResponsavel'):
                    email = self.clean_string(cliente.get('EmailUsuarioComercialResponsavel'))
                    if email:
                        email = email.lower()
                        invalid_patterns = ['pendente', 'n/a', 'na', 'null', 'none']
                        if email not in invalid_patterns and self.is_valid_email(email):
                            try:
                                insert_query = """
                                INSERT INTO core.contact_email (
                                    contact_email_id, customer_id, email_type,
                                    email, status, created_at, updated_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """
                                
                                cursor.execute(insert_query, (
                                    str(uuid.uuid4()),
                                    str(customer_id),
                                    'commercial',
                                    email,
                                    'active',
                                    cliente.get('DataInclusao'),
                                    cliente.get('DataAlteracao')
                                ))
                                
                                self.stats['emails'] += 1
                                
                            except Exception as e:
                                error_msg = f"Erro ao inserir email comercial cliente Id={cliente_id}: {str(e)}"
                                logger.error(error_msg)
                                self.stats['errors'].append(error_msg)
            
            conn.commit()
            print(f"[ETAPA 4] CONCLUIDA! Total de emails migrados: {self.stats['emails']}")
            logger.info(f"[OK] Emails migrados: {self.stats['emails']}")
            
        except Exception as e:
            # conn.rollback()
            logger.error(f"Erro na migração de emails: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def step5_migrate_phones(self, customer_id_map: Dict[int, uuid.UUID]):
        """ETAPA 5: Migrar telefones"""
        print("\n" + "="*80)
        print(">>> ETAPA 5: MIGRANDO TELEFONES")
        print("="*80)
        logger.info("="*60)
        logger.info("ETAPA 5: Migrando telefones")
        logger.info("="*60)
        
        print("[ETAPA 5] Buscando dados de telefones do SQL Server...")
        sql_query = """
        SELECT 
            Id, Telefone, Celular, TelefoneResponsavel,
            DataInclusao, DataAlteracao
        FROM Cliente
        WHERE Telefone IS NOT NULL OR Celular IS NOT NULL OR TelefoneResponsavel IS NOT NULL
        ORDER BY Id
        """
        
        clientes = self.execute_query(sql_query, is_postgres=False, description="buscar telefones do SQL Server")
        print(f"[ETAPA 5] Processando {len(clientes)} registros de telefones...")
        
        conn = DatabaseConnection.get_postgresql_connection()
        cursor = conn.cursor()
        
        logger.info("Limpando tabela contact_phone na tabela destino...")

        sql_query_truncate = """
        TRUNCATE TABLE core.contact_phone CASCADE;
        """
        self.execute_query(sql_query_truncate, is_postgres=True, description="truncar tabela contact_phone")
        
        logger.info("Tabela contact_phone limpa com sucesso!")
        
        try:
            processed = 0
            for cliente in clientes:
                processed += 1
                if processed % 500 == 0:
                    print(f"[ETAPA 5] Processados {processed}/{len(clientes)} registros...")
                cliente_id = cliente['Id']
                customer_id = customer_id_map.get(cliente_id)
                
                if not customer_id:
                    continue
                
                # Telefone
                if cliente.get('Telefone'):
                    area_code, number = self.clean_phone(cliente.get('Telefone'))
                    # if area_code and number:
                    try:
                        insert_query = """
                        INSERT INTO core.contact_phone (
                            contact_phone_id, customer_id, phone_type,
                            country_code, area_code, number,
                            status, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        cursor.execute(insert_query, (
                            str(uuid.uuid4()),
                            str(customer_id),
                            'phone',
                            '55',
                            area_code,
                            number,
                            'active',
                            cliente.get('DataInclusao'),
                            cliente.get('DataAlteracao')
                        ))
                        
                        self.stats['phones'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir telefone cliente Id={cliente_id}: {str(e)}"
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                
                # Celular
                if cliente.get('Celular'):
                    area_code, number = self.clean_phone(cliente.get('Celular'))
                    # if area_code and number:
                    try:
                        insert_query = """
                        INSERT INTO core.contact_phone (
                            contact_phone_id, customer_id, phone_type,
                            country_code, area_code, number,
                            status, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        cursor.execute(insert_query, (
                            str(uuid.uuid4()),
                            str(customer_id),
                            'mobile',
                            '55',
                            area_code,
                            number,
                            'active',
                            cliente.get('DataInclusao'),
                            cliente.get('DataAlteracao')
                        ))
                        
                        self.stats['phones'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir celular cliente Id={cliente_id}: {str(e)}"
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                
                # Telefone Responsável
                if cliente.get('TelefoneResponsavel'):
                    area_code, number = self.clean_phone(cliente.get('TelefoneResponsavel'))
                    # if area_code and number:
                    try:
                        insert_query = """
                        INSERT INTO core.contact_phone (
                            contact_phone_id, customer_id, phone_type,
                            country_code, area_code, number,
                            status, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        cursor.execute(insert_query, (
                            str(uuid.uuid4()),
                            str(customer_id),
                            'responsible',
                            '55',
                            area_code,
                            number,
                            'active',
                            cliente.get('DataInclusao'),
                            cliente.get('DataAlteracao')
                        ))
                        
                        self.stats['phones'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir telefone responsável cliente Id={cliente_id}: {str(e)}"
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
            
            conn.commit()
            print(f"[ETAPA 5] CONCLUIDA! Total de telefones migrados: {self.stats['phones']}")
            logger.info(f"[OK] Telefones migrados: {self.stats['phones']}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Erro na migração de telefones: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def step6_migrate_shareholders(self, customer_id_map: Dict[int, uuid.UUID]):
        """ETAPA 6: Migrar sócios"""
        print("\n" + "="*80)
        print(">>> ETAPA 6: MIGRANDO SOCIOS")
        print("="*80)
        logger.info("="*60)
        logger.info("ETAPA 6: Migrando sócios")
        logger.info("="*60)
        
        print("[ETAPA 6] Carregando lookup Municipio (origem SQL Server)...")
        conn_mun = DatabaseConnection.get_sql_server_prd_connection()
        cur_mun = conn_mun.cursor()
        try:
            municipio_lookup = load_municipio_lookup(cur_mun)
        finally:
            cur_mun.close()
            conn_mun.close()
        
        print("[ETAPA 6] Buscando dados de socios do SQL Server...")
        sql_query = """
        SELECT 
            Id, Codigo, NomeSocio, CargoSocio, DataAtivacao, DataInativacao,
            EnderecoSocio, NumeroSocio, ComplementoSocio, BairroSocio,
            CEPSocio, CidadeSocio, CodigoMunicipioSocio, UFSocio,
            TelefoneSocio, CPFSocio, RGSocio, NacionalidadeSocio,
            EstCivilSocio, DataInclusao, DataAlteracao
        FROM Cliente
        WHERE NomeSocio IS NOT NULL 
          AND LTRIM(RTRIM(NomeSocio)) != ''
          AND LTRIM(RTRIM(UPPER(NomeSocio))) != 'PENDENTE'
        ORDER BY Id
        """
        
        clientes = self.execute_query(sql_query, is_postgres=False, description="buscar sócios do SQL Server")
        print(f"[ETAPA 6] Processando {len(clientes)} registros de socios...")
        
        conn = DatabaseConnection.get_postgresql_connection()
        cursor = conn.cursor()
        
        # logger.info("Limpando tabela contact na tabela destino...")

        # sql_query_truncate = """
        # TRUNCATE TABLE core.contact CASCADE;
        # """
        # self.execute_query(sql_query_truncate, is_postgres=True, description="truncar tabela contact")
        
        # logger.info("Tabela contact limpa com sucesso!")

        
        logger.info("Limpando tabela shareholder na tabela destino...")

        sql_query_truncate = """
        TRUNCATE TABLE core.shareholder CASCADE;
        """
        self.execute_query(sql_query_truncate, is_postgres=True, description="truncar tabela shareholder")
        
        logger.info("Tabela shareholder limpa com sucesso!")
        
        logger.info("Limpando tabela shareholder_phone na tabela destino...")

        sql_query_truncate = """
        TRUNCATE TABLE core.shareholder_phone CASCADE;
        """
        self.execute_query(sql_query_truncate, is_postgres=True, description="truncar tabela shareholder_phone")
        
        logger.info("Tabela shareholder_phone limpa com sucesso!")

        logger.info("Limpando tabela shareholder_pii na tabela destino...")

        sql_query_truncate = """
        TRUNCATE TABLE core.shareholder_pii CASCADE;
        """
        self.execute_query(sql_query_truncate, is_postgres=True, description="truncar tabela shareholder_pii")
        
        logger.info("Tabela shareholder_pii limpa com sucesso!")
        
        try:
            shareholder_map = {}  # Map cliente_id -> shareholder_id
            processed = 0
            
            for cliente in clientes:
                processed += 1
                if processed % 500 == 0:
                    print(f"[ETAPA 6] Processados {processed}/{len(clientes)} registros...")
                cliente_id = cliente['Id']
                customer_id = customer_id_map.get(cliente_id)
                
                if not customer_id:
                    continue
                
                # Criar Contact do Sócio
                contact_id = uuid.uuid4()
                try:
                    insert_contact = """
                    INSERT INTO core.contact (
                        contact_id, customer_id, contact_name, job_title,
                        status, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    cursor.execute(insert_contact, (
                        str(contact_id),
                        str(customer_id),
                        self.clean_string(cliente.get('NomeSocio')),
                        self.clean_string(cliente.get('CargoSocio'), 200),
                        'active',
                        cliente.get('DataInclusao'),
                        cliente.get('DataAlteracao')
                    ))
                    
                except Exception as e:
                    error_msg = f"Erro ao criar contact do sócio cliente Id={cliente_id}: {str(e)}"
                    print(f"[ETAPA 6] ERRO ao criar contact do sócio cliente Id={cliente_id}: {str(e)}")
                    logger.error(error_msg)
                    self.stats['errors'].append(error_msg)
                    try:
                        conn.rollback()
                        cursor = conn.cursor()
                    except:
                        pass
                    continue
                
                # Criar Shareholder
                shareholder_id = uuid.uuid4()
                shareholder_map[cliente_id] = shareholder_id
                
                try:                            
                    insert_shareholder = """
                    INSERT INTO core.shareholder (
                        shareholder_id, customer_id, contact_id, role_label,
                        start_date, end_date, status, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    cursor.execute(insert_shareholder, (
                        str(shareholder_id),
                        str(customer_id),
                        str(contact_id),
                        self.clean_string(cliente.get('CargoSocio'), 200),
                        cliente.get('DataAtivacao'),
                        cliente.get('DataInativacao'),
                        'active',
                        cliente.get('DataInclusao'),
                        cliente.get('DataAlteracao')
                    ))
                    
                    self.stats['shareholders'] += 1
                    
                except Exception as e:
                    error_msg = f"Erro ao criar shareholder cliente Id={cliente_id}: {str(e)}"
                    print(f"[ETAPA 6] ERRO ao criar shareholder cliente Id={cliente_id}: {str(e)}")
                    logger.error(error_msg)
                    self.stats['errors'].append(error_msg)
                    try:
                        conn.rollback()
                        cursor = conn.cursor()
                    except:
                        pass
                    continue
                
                # Endereço do Sócio
                if cliente.get('EnderecoSocio') and cliente.get('EnderecoSocio').strip().upper() != 'PENDENTE':
                    try:
                        cep = re.sub(r'[^\d]', '', str(cliente.get('CEPSocio', ''))) if cliente.get('CEPSocio') else None
                        if not cep or cep == '0' * len(cep):
                            cep = None
                        
                        insert_address = """
                        INSERT INTO core.address (
                            address_id, address_type, addressable_type, addressable_id,
                            legacy_cliente_id, legacy_cliente_code,
                            street, house_num, complement1, district,
                            cep, city, city_code, region_code, country,
                            status, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """
                        
                        # Para shareholder, addressable_id precisa ser um número (BIGINT)
                        # Vamos usar o legado_cliente_id como referência
                        cursor.execute(insert_address, (
                            str(uuid.uuid4()),
                            'main',
                            'shareholder',
                            cliente_id,  # addressable_id é BIGINT, usar legado_cliente_id como referência
                            cliente_id,
                            cliente.get('Codigo'),
                            self.clean_string(cliente.get('EnderecoSocio'), 200),
                            self.clean_string(cliente.get('NumeroSocio'), 20),
                            self.clean_string(cliente.get('ComplementoSocio')),
                            self.clean_string(cliente.get('BairroSocio')),
                            cep,
                            self.clean_string(cliente.get('CidadeSocio')),
                            city_code_legacy_str(
                                cliente.get('CodigoMunicipioSocio'),
                                cliente.get('UFSocio'),
                                cliente.get('CidadeSocio'),
                                municipio_lookup=municipio_lookup,
                            ),
                            self.clean_string(cliente.get('UFSocio'), 2),
                            'BR',
                            'active',
                            cliente.get('DataInclusao'),
                            cliente.get('DataAlteracao')
                        ))
                        
                        self.stats['shareholder_addresses'] += 1
                        
                    except Exception as e:
                        error_msg = f"Erro ao inserir endereço sócio cliente Id={cliente_id}: {str(e)}"
                        print(f"[ETAPA 6] ERRO no endereço sócio cliente Id={cliente_id}: {str(e)}")
                        logger.error(error_msg)
                        self.stats['errors'].append(error_msg)
                        try:
                            conn.rollback()
                            cursor = conn.cursor()
                        except:
                            pass
                        # Continuar mesmo com erro
                
                # Telefone do Sócio
                if cliente.get('TelefoneSocio'):
                    area_code, number = self.clean_phone(cliente.get('TelefoneSocio'))
                    if area_code and number and number != '00000000000':
                        try:
                            insert_phone = """
                            INSERT INTO core.shareholder_phone (
                                shareholder_phone__id, shareholder_id, phone_type,
                                country_code, area_code, number,
                                status, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            
                            cursor.execute(insert_phone, (
                                str(uuid.uuid4()),
                                str(shareholder_id),
                                'phone',
                                '55',
                                area_code,
                                number,
                                'active',
                                cliente.get('DataInclusao'),
                                cliente.get('DataAlteracao')
                            ))
                            
                            self.stats['shareholder_phones'] += 1
                            
                        except Exception as e:
                            error_msg = f"Erro ao inserir telefone sócio cliente Id={cliente_id}: {str(e)}"
                            print(f"[ETAPA 6] ERRO no telefone sócio cliente Id={cliente_id}: {str(e)}")
                            logger.error(error_msg)
                            self.stats['errors'].append(error_msg)
                            try:
                                conn.rollback()
                                cursor = conn.cursor()
                            except:
                                pass
                            # Continuar mesmo com erro
                
                # PII do Sócio
                cpf = self.clean_cpf_cnpj(cliente.get('CPFSocio'))
                # Validar CPF: deve ter exatamente 11 dígitos e não ser apenas zeros
                if cpf and cpf != '0' * len(cpf):
                    cpf_str = None
                    if len(cpf) == 11:
                        cpf_str = cpf
                    elif len(cpf) == 10:
                        # CPF com 10 dígitos pode ser completado com zero à esquerda
                        cpf_str = cpf.zfill(11)
                    # Ignorar CPFs com tamanho diferente de 10 ou 11 dígitos
                    
                    if cpf_str:
                        try:                        
                            insert_pii = """
                            INSERT INTO core.shareholder_pii (
                                shareholder_id, cpf, rg, nationality,
                                marital_status, job_title, status,
                                created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                        
                            cursor.execute(insert_pii, (
                                str(shareholder_id),
                                cpf_str,
                                self.clean_string(cliente.get('RGSocio')),
                                self.clean_string(cliente.get('NacionalidadeSocio')),
                                self.convert_marital_status(cliente.get('EstCivilSocio')),
                                self.clean_string(cliente.get('CargoSocio'), 200),
                                'active',
                                cliente.get('DataInclusao'),
                                cliente.get('DataAlteracao')
                            ))
                            
                            self.stats['shareholder_pii'] += 1
                            
                        except Exception as e:
                            error_msg = f"Erro ao inserir PII sócio cliente Id={cliente_id}: {str(e)}"
                            print(f"[ETAPA 6] ERRO no PII sócio cliente Id={cliente_id}: {str(e)}")
                            logger.error(error_msg)
                            self.stats['errors'].append(error_msg)
                            try:
                                conn.rollback()
                                cursor = conn.cursor()
                            except:
                                pass
                            # Continuar mesmo com erro
            
            # Commit ao final
            try:
                conn.commit()
            except Exception as e:
                print(f"[ETAPA 6] ERRO no commit: {str(e)}")
                conn.rollback()
                cursor = conn.cursor()
            print(f"[ETAPA 6] CONCLUIDA!")
            print(f"[ETAPA 6] Socios: {self.stats['shareholders']}")
            print(f"[ETAPA 6] Enderecos de socios: {self.stats['shareholder_addresses']}")
            print(f"[ETAPA 6] Telefones de socios: {self.stats['shareholder_phones']}")
            print(f"[ETAPA 6] PII de socios: {self.stats['shareholder_pii']}")
            logger.info(f"[OK] Socios migrados: {self.stats['shareholders']}")
            logger.info(f"[OK] Enderecos de socios: {self.stats['shareholder_addresses']}")
            logger.info(f"[OK] Telefones de socios: {self.stats['shareholder_phones']}")
            logger.info(f"[OK] PII de socios: {self.stats['shareholder_pii']}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Erro na migração de sócios: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def validate_migration(self):
        """Validação final da migração"""
        print("\n" + "="*80)
        print(">>> VALIDACAO FINAL")
        print("="*80)
        logger.info("="*60)
        logger.info("VALIDAÇÃO FINAL")
        logger.info("="*60)
        
        try:
            # Contagem geral
            validation_query = """
            SELECT 
                'customers' as tabela, COUNT(*)::text as total FROM core.customers
            UNION ALL
            SELECT 'address (main)', COUNT(*)::text FROM core.address WHERE addressable_type='customers' AND address_type='main'
            UNION ALL
            SELECT 'address (billing)', COUNT(*)::text FROM core.address WHERE addressable_type='customers' AND address_type='billing'
            UNION ALL
            SELECT 'contact', COUNT(*)::text FROM core.contact
            UNION ALL
            SELECT 'contact_email', COUNT(*)::text FROM core.contact_email
            UNION ALL
            SELECT 'contact_phone', COUNT(*)::text FROM core.contact_phone
            UNION ALL
            SELECT 'shareholder', COUNT(*)::text FROM core.shareholder;
            """
            
            results = DatabaseConnection.execute_postgresql_query(validation_query)
            
            logger.info("\n[RESUMO] Resumo da Migracao:")
            for row in results:
                logger.info(f"   {row['tabela']}: {row['total']}")
            
            # Verificar FKs órfãs
            # addressable_id é BIGINT e contém legado_cliente_id, não customer_id UUID
            fk_query = """
            SELECT 'address sem customer' as erro, COUNT(*)::text as total
            FROM core.address a
            LEFT JOIN core.customers c ON a.addressable_id = c.legado_cliente_id
            WHERE a.addressable_type = 'customers' AND c.customer_id IS NULL;
            """
            
            fk_results = DatabaseConnection.execute_postgresql_query(fk_query)
            if fk_results and int(fk_results[0]['total']) > 0:
                logger.warning(f"[AVISO] {fk_results[0]['erro']}: {fk_results[0]['total']}")
            else:
                logger.info("[OK] Integridade referencial OK")
            
        except Exception as e:
            logger.error(f"Erro na validação: {str(e)}")
    
    def run(self):
        """Executa a migração completa"""
        print("\n" + "="*80)
        print("="*80)
        print("INICIANDO MIGRACAO: Cliente (SQL Server) -> Core (PostgreSQL)")
        print("="*80)
        print(f"Data/Hora: {datetime.now()}")
        print("="*80)
        logger.info("="*60)
        logger.info("INICIANDO MIGRAÇÃO: Cliente → Core")
        logger.info("="*60)
        logger.info(f"Data/Hora: {datetime.now()}")
        
        start_time = datetime.now()
        
        try:
            # ETAPA 1: Customers
            customer_id_map = self.step1_migrate_customers()
            
            # ETAPA 2: Addresses
            self.step2_migrate_addresses(customer_id_map)
            
            # ETAPA 3: Contacts
            self.step3_migrate_contacts(customer_id_map)
            
            # ETAPA 4: Emails
            self.step4_migrate_emails(customer_id_map)
            
            # ETAPA 5: Phones
            self.step5_migrate_phones(customer_id_map)
            
            # ETAPA 6: Shareholders
            self.step6_migrate_shareholders(customer_id_map)
            
            # Validação
            self.validate_migration()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            print("\n" + "="*80)
            print("="*80)
            print("MIGRACAO CONCLUIDA COM SUCESSO!")
            print("="*80)
            logger.info("="*60)
            logger.info("MIGRAÇÃO CONCLUÍDA COM SUCESSO!")
            logger.info("="*60)
            print(f"Duracao total: {duration}")
            print(f"\n[ESTATISTICAS FINAIS]")
            logger.info(f"Duração: {duration}")
            logger.info(f"\n[ESTATISTICAS] Estatisticas:")
            print(f"   Customers: {self.stats['customers']}")
            print(f"   Enderecos (main): {self.stats['address_main']}")
            print(f"   Enderecos (billing): {self.stats['address_billing']}")
            print(f"   Contatos: {self.stats['contacts']}")
            print(f"   Emails: {self.stats['emails']}")
            print(f"   Telefones: {self.stats['phones']}")
            print(f"   Socios: {self.stats['shareholders']}")
            print(f"   Enderecos socios: {self.stats['shareholder_addresses']}")
            print(f"   Telefones socios: {self.stats['shareholder_phones']}")
            print(f"   PII socios: {self.stats['shareholder_pii']}")
            logger.info(f"   Customers: {self.stats['customers']}")
            logger.info(f"   Enderecos (main): {self.stats['address_main']}")
            logger.info(f"   Enderecos (billing): {self.stats['address_billing']}")
            logger.info(f"   Contatos: {self.stats['contacts']}")
            logger.info(f"   Emails: {self.stats['emails']}")
            logger.info(f"   Telefones: {self.stats['phones']}")
            logger.info(f"   Socios: {self.stats['shareholders']}")
            logger.info(f"   Enderecos socios: {self.stats['shareholder_addresses']}")
            logger.info(f"   Telefones socios: {self.stats['shareholder_phones']}")
            logger.info(f"   PII socios: {self.stats['shareholder_pii']}")
            
            if self.stats['errors']:
                print(f"\n[AVISO] Total de erros: {len(self.stats['errors'])}")
                print("Verifique o arquivo de log para detalhes dos erros")
                logger.warning(f"\n[AVISO] Total de erros: {len(self.stats['errors'])}")
                logger.warning("Verifique o log para detalhes dos erros")
            else:
                print("\n[OK] Nenhum erro encontrado!")
                logger.info("\n[OK] Nenhum erro encontrado!")
            
        except Exception as e:
            logger.error("="*60)
            logger.error("ERRO CRÍTICO NA MIGRAÇÃO!")
            logger.error("="*60)
            logger.error(f"Erro: {str(e)}")
            raise


if __name__ == "__main__":
    migration = ClienteMigration()
    migration.run()

