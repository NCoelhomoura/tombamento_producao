"""
Script para criar dicionário de migração de dados - STORES
Compara estruturas das tabelas origem (SQL Server PRD) e destino (PostgreSQL HML)
"""

import sys
import io
import os
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Adicionar diretório utils ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from database_connection import DatabaseConnection
import pyodbc


def get_sql_server_table_structure(table_name):
    """Obtém a estrutura de uma tabela do SQL Server PRD"""
    conn = None
    try:
        conn = DatabaseConnection.get_sql_server_prd_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            CASE 
                WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES'
                ELSE 'NO'
            END AS IS_PRIMARY_KEY
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN (
            SELECT ku.TABLE_NAME, ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
        ) pk ON c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME
        WHERE c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
        """
        
        cursor.execute(query, (table_name,))
        columns = cursor.fetchall()
        
        structure = []
        for col in columns:
            col_name = col[0]
            data_type = col[1]
            char_max_len = col[2]
            numeric_precision = col[3]
            numeric_scale = col[4]
            is_nullable = col[5]
            column_default = col[6]
            is_pk = col[7]
            
            # Construir tipo completo
            if data_type in ['varchar', 'nvarchar', 'char', 'nchar']:
                if char_max_len:
                    if char_max_len == -1:
                        type_str = f"{data_type}(MAX)"
                    else:
                        type_str = f"{data_type}({char_max_len})"
                else:
                    type_str = data_type
            elif data_type in ['decimal', 'numeric']:
                if numeric_precision and numeric_scale:
                    type_str = f"{data_type}({numeric_precision},{numeric_scale})"
                elif numeric_precision:
                    type_str = f"{data_type}({numeric_precision})"
                else:
                    type_str = data_type
            elif data_type in ['float', 'real']:
                if numeric_precision:
                    type_str = f"{data_type}({numeric_precision})"
                else:
                    type_str = data_type
            else:
                type_str = data_type
            
            structure.append({
                'name': col_name,
                'type': type_str,
                'size': char_max_len if char_max_len else (f"{numeric_precision},{numeric_scale}" if numeric_precision and numeric_scale else (numeric_precision if numeric_precision else None)),
                'nullable': is_nullable,
                'default': column_default,
                'primary_key': is_pk == 'YES'
            })
        
        cursor.close()
        conn.close()
        
        return structure
        
    except Exception as e:
        print(f"ERRO ao obter estrutura da tabela {table_name} do SQL Server: {e}")
        if conn:
            conn.close()
        raise


def get_postgresql_table_structure(table_name, schema='gmcore'):
    """Obtém a estrutura de uma tabela do PostgreSQL HML"""
    conn = None
    try:
        conn = DatabaseConnection.get_postgresql_hml_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT 
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            CASE 
                WHEN pk.column_name IS NOT NULL THEN 'YES'
                ELSE 'NO'
            END AS is_primary_key
        FROM information_schema.columns c
        LEFT JOIN (
            SELECT ku.table_name, ku.column_name
            FROM information_schema.table_constraints tc
            INNER JOIN information_schema.key_column_usage ku
                ON tc.constraint_type = 'PRIMARY KEY'
                AND tc.constraint_name = ku.constraint_name
                AND tc.table_schema = ku.table_schema
        ) pk ON c.table_name = pk.table_name AND c.column_name = pk.column_name
        WHERE c.table_schema = %s
        AND c.table_name = %s
        ORDER BY c.ordinal_position
        """
        
        cursor.execute(query, (schema, table_name))
        columns = cursor.fetchall()
        
        structure = []
        for col in columns:
            col_name = col[0]
            data_type = col[1]
            char_max_len = col[2]
            numeric_precision = col[3]
            numeric_scale = col[4]
            is_nullable = col[5]
            column_default = col[6]
            is_pk = col[7]
            
            # Construir tipo completo
            if data_type in ['character varying', 'varchar']:
                if char_max_len:
                    type_str = f"VARCHAR({char_max_len})"
                else:
                    type_str = "VARCHAR"
            elif data_type in ['character', 'char']:
                if char_max_len:
                    type_str = f"CHAR({char_max_len})"
                else:
                    type_str = "CHAR"
            elif data_type in ['numeric', 'decimal']:
                if numeric_precision and numeric_scale:
                    type_str = f"NUMERIC({numeric_precision},{numeric_scale})"
                elif numeric_precision:
                    type_str = f"NUMERIC({numeric_precision})"
                else:
                    type_str = "NUMERIC"
            elif data_type == 'integer':
                type_str = "INTEGER"
            elif data_type == 'bigint':
                type_str = "BIGINT"
            elif data_type == 'smallint':
                type_str = "SMALLINT"
            elif data_type == 'double precision':
                type_str = "DOUBLE PRECISION"
            elif data_type == 'real':
                type_str = "REAL"
            elif data_type == 'boolean':
                type_str = "BOOLEAN"
            elif data_type == 'date':
                type_str = "DATE"
            elif data_type == 'timestamp without time zone':
                type_str = "TIMESTAMP"
            elif data_type == 'timestamp with time zone':
                type_str = "TIMESTAMP WITH TIME ZONE"
            elif data_type == 'text':
                type_str = "TEXT"
            elif data_type == 'uuid':
                type_str = "UUID"
            elif data_type in ['json', 'jsonb']:
                type_str = data_type.upper()
            else:
                type_str = data_type.upper()
            
            structure.append({
                'name': col_name,
                'type': type_str,
                'size': char_max_len if char_max_len else (f"{numeric_precision},{numeric_scale}" if numeric_precision and numeric_scale else (numeric_precision if numeric_precision else None)),
                'nullable': is_nullable,
                'default': column_default,
                'primary_key': is_pk == 'YES'
            })
        
        cursor.close()
        conn.close()
        
        return structure
        
    except Exception as e:
        print(f"ERRO ao obter estrutura da tabela {table_name} do PostgreSQL: {e}")
        if conn:
            conn.close()
        raise


def format_size(size):
    """Formata o tamanho para exibição"""
    if size is None:
        return "-"
    if isinstance(size, int):
        return str(size)
    return str(size)


def create_dictionary_file():
    """Cria o arquivo de dicionário de migração"""
    
    # Mapeamento de tabelas: Destino : Origem
    mappings = [
        {'destino': 'stores', 'origem': 'Estabelecimento'},
        {'destino': 'store_brands', 'origem': 'Bandeira'},
        {'destino': 'retail_chains', 'origem': 'Rede'},
        {'destino': 'store_cnpjs', 'origem': 'Estabelecimento'},
        {'destino': 'addresses', 'origem': 'Estabelecimento'},
        {'destino': 'contacts', 'origem': 'Estabelecimento'},
        {'destino': 'store_segments', 'origem': 'CanalEstabelecimento'}
    ]
    
    output_lines = []
    output_lines.append("="*100)
    output_lines.append("DICIONARIO DE MIGRACAO DE DADOS - STORES")
    output_lines.append("="*100)
    output_lines.append("")
    output_lines.append("ORIGEM: SQL Server PRD (Database: FINANCEIRO)")
    output_lines.append("DESTINO: PostgreSQL HML (Database: supera_dev_seed, Schema: gmcore)")
    output_lines.append("")
    output_lines.append("="*100)
    output_lines.append("")
    
    for mapping in mappings:
        destino_table = mapping['destino']
        origem_table = mapping['origem']
        
        print(f"\nProcessando: {origem_table} -> {destino_table}")
        
        try:
            # Obter estruturas
            origem_structure = get_sql_server_table_structure(origem_table)
            destino_structure = get_postgresql_table_structure(destino_table)
            
            # Adicionar informações da tabela
            output_lines.append("="*100)
            output_lines.append(f"TABELA DESTINO: {destino_table.upper()}")
            output_lines.append(f"TABELA ORIGEM: {origem_table}")
            output_lines.append("="*100)
            output_lines.append("")
            
            # Estrutura origem
            output_lines.append(f"ORIGEM - SQL Server PRD - Tabela: {origem_table}")
            output_lines.append("-"*100)
            output_lines.append(f"{'Nome':<40} {'Tipo':<30} {'Tamanho':<15} {'Nullable':<10} {'PK':<5}")
            output_lines.append("-"*100)
            
            origem_fields = []
            for col in origem_structure:
                nullable_str = "SIM" if col['nullable'] == 'YES' else "NAO"
                pk_str = "SIM" if col['primary_key'] else "NAO"
                size_str = format_size(col['size'])
                output_lines.append(f"{col['name']:<40} {col['type']:<30} {size_str:<15} {nullable_str:<10} {pk_str:<5}")
                origem_fields.append(col['name'])
            
            output_lines.append("")
            output_lines.append("")
            
            # Estrutura destino
            output_lines.append(f"DESTINO - PostgreSQL HML - Tabela: gmcore.{destino_table}")
            output_lines.append("-"*100)
            output_lines.append(f"{'Nome':<40} {'Tipo':<30} {'Tamanho':<15} {'Nullable':<10} {'PK':<5}")
            output_lines.append("-"*100)
            
            destino_fields = []
            for col in destino_structure:
                nullable_str = "SIM" if col['nullable'] == 'YES' else "NAO"
                pk_str = "SIM" if col['primary_key'] else "NAO"
                size_str = format_size(col['size'])
                output_lines.append(f"{col['name']:<40} {col['type']:<30} {size_str:<15} {nullable_str:<10} {pk_str:<5}")
                destino_fields.append(col['name'])
            
            output_lines.append("")
            output_lines.append("")
            
            # Campos disponíveis
            output_lines.append("CAMPOS DISPONIVEIS:")
            output_lines.append(f"  Origem ({origem_table}): {', '.join(origem_fields)}")
            output_lines.append(f"  Destino ({destino_table}): {', '.join(destino_fields)}")
            output_lines.append("")
            output_lines.append("")
            
        except Exception as e:
            print(f"ERRO ao processar {origem_table} -> {destino_table}: {e}")
            output_lines.append(f"ERRO ao processar {origem_table} -> {destino_table}: {e}")
            output_lines.append("")
    
    # Adicionar seção de mapeamento detalhado
    output_lines.append("="*100)
    output_lines.append("MAPEAMENTO DETALHADO DE CAMPOS")
    output_lines.append("="*100)
    output_lines.append("")
    output_lines.append("Formato: origem.dbo.tabela.campo : destino.schema.tabela.campo")
    output_lines.append("")
    
    # Mapeamentos manuais específicos
    manual_mappings = {
        'stores': {
            'legacy_id': 'Id',
            'name': 'NomeFantasia',
            'store_brand_id': '[PREENCHIDO PELA LOGICA DE MIGRACAO - Estabelecimento.IdBandeira -> store_brands.legacy_id -> store_brands.id]',
            'created_at': 'DataInclusao',
            'updated_at': 'DataAlteracao',
            'is_active': 'Ativo'
        },
        'store_brands': {
            'legacy_id': 'Id',
            'description': 'NomeFantasia',
            'abras_code': '[PREENCHIDO PELA LOGICA DE MIGRACAO - valor padrão "empty", campo não pode ser nulo]',
            'retail_chain_id': '[PREENCHIDO PELA LOGICA DE MIGRACAO - Bandeira.IdRede -> retail_chains.legacy_id -> retail_chains.id]',
            'store_segment_id': '[PREENCHIDO PELA LOGICA DE MIGRACAO - Estabelecimento.IdCanalEstabelecimento -> store_segments.legacy_id -> store_segments.id]',
            'is_active': 'Ativo',
            'created_at': 'DataInclusao',
            'updated_at': 'DataAlteracao'
        },
        'retail_chains': {
            'legacy_id': 'Id',
            'name': 'Nome',
            'description': 'Codigo',
            'is_active': 'Ativo',
            'created_at': 'DataInclusao',
            'updated_at': 'DataAlteracao'
        },
        'store_cnpjs': {
            'cnpj': 'Cnpj',
            'store_id': '[PREENCHIDO PELA LOGICA DE MIGRACAO - Estabelecimento.Id -> stores.legacy_id -> stores.id]',
            'is_main': '[PREENCHIDO PELA LOGICA DE MIGRACAO - sempre true para CNPJ principal]',
            'is_active': 'Ativo',
            'created_at': 'DataInclusao',
            'updated_at': 'DataAlteracao'
        },
        'addresses': {
            'legacy_id': 'Id',
            'addressable_type': '[PREENCHIDO PELA LOGICA DE MIGRACAO - sempre "stores"]',
            'addressable_id': '[PREENCHIDO PELA LOGICA DE MIGRACAO - Estabelecimento.Id -> stores.legacy_id -> stores.id]',
            'type': '[PREENCHIDO PELA LOGICA DE MIGRACAO - sempre "main"]',
            'postal_code': 'CEP',
            'street': 'Endereco',
            'number': 'Numero',
            'address_line_2': 'Complemento',
            'neighborhood': 'Bairro',
            'city': 'Cidade',
            'state': 'UF',
            'municipal_code': '[PREENCHIDO PELA LOGICA DE MIGRACAO - valor padrão 0, não há campo correspondente na origem]',
            'latitude': 'Latitude',
            'longitude': 'Longitude',
            'zone': 'Bairro',
            'region': '[PREENCHIDO PELA LOGICA DE MIGRACAO - string vazia]',
            'created_at': 'DataInclusao',
            'updated_at': 'DataAlteracao'
        },
        'contacts': {
            'contactable_type': '[PREENCHIDO PELA LOGICA DE MIGRACAO - sempre "stores"]',
            'contactable_id': '[PREENCHIDO PELA LOGICA DE MIGRACAO - Estabelecimento.Id -> stores.legacy_id -> stores.id]',
            'type_email': '[PREENCHIDO PELA LOGICA DE MIGRACAO - tipo "email", value = Estabelecimento.Email em minusculo]',
            'type_phone': '[PREENCHIDO PELA LOGICA DE MIGRACAO - tipo "phone", value = Estabelecimento.Telefone (apenas numeros ddd+number)]',
            'type_cellphone_gerente': '[PREENCHIDO PELA LOGICA DE MIGRACAO - tipo "cellphone", value = Estabelecimento.CelularGerente (apenas numeros ddd+number)]',
            'created_at': 'DataInclusao',
            'updated_at': 'DataAlteracao'
        },
        'store_segments': {
            'legacy_id': 'Id',
            'name': 'Nome',
            'is_active': 'Ativo',
            'created_at': 'DataInclusao',
            'updated_at': 'DataAlteracao'
        }
    }
    
    for mapping in mappings:
        destino_table = mapping['destino']
        origem_table = mapping['origem']
        
        output_lines.append(f"MIGRACAO: {origem_table.upper()} : {destino_table.upper()}")
        output_lines.append("-"*100)
        
        try:
            origem_structure = get_sql_server_table_structure(origem_table)
            destino_structure = get_postgresql_table_structure(destino_table)
            
            origem_dict = {col['name'].lower(): col for col in origem_structure}
            destino_dict = {col['name'].lower(): col for col in destino_structure}
            
            # Mapear campos
            mappings_list = []
            unmapped_destino = []
            unmapped_origem = []
            mapped_fields = set()  # Para rastrear campos já mapeados
            
            # Tratamento especial para contacts (múltiplos registros por estabelecimento)
            if destino_table == 'contacts':
                output_lines.append(f"  [GERADO AUTOMATICAMENTE] : supera_dev_seed.gmcore.{destino_table}.id")
                output_lines.append(f"  [PREENCHIDO PELA LOGICA DE MIGRACAO - sempre \"stores\"] : supera_dev_seed.gmcore.{destino_table}.contactable_type")
                output_lines.append(f"  [PREENCHIDO PELA LOGICA DE MIGRACAO - Estabelecimento.Id -> stores.legacy_id -> stores.id] : supera_dev_seed.gmcore.{destino_table}.contactable_id")
                output_lines.append(f"")
                output_lines.append(f"  REGISTRO 1 - Email:")
                output_lines.append(f"    FINANCEIRO.dbo.{origem_table}.Email : supera_dev_seed.gmcore.{destino_table}.value (type=\"email\", value em minusculo)")
                output_lines.append(f"")
                output_lines.append(f"  REGISTRO 2 - Telefone:")
                output_lines.append(f"    FINANCEIRO.dbo.{origem_table}.Telefone : supera_dev_seed.gmcore.{destino_table}.value (type=\"phone\", apenas numeros ddd+number)")
                output_lines.append(f"")
                output_lines.append(f"  REGISTRO 3 - Celular Gerente:")
                output_lines.append(f"    FINANCEIRO.dbo.{origem_table}.CelularGerente : supera_dev_seed.gmcore.{destino_table}.value (type=\"cellphone\", apenas numeros ddd+number)")
                output_lines.append(f"")
                output_lines.append(f"  FINANCEIRO.dbo.{origem_table}.DataInclusao : supera_dev_seed.gmcore.{destino_table}.created_at")
                output_lines.append(f"  FINANCEIRO.dbo.{origem_table}.DataAlteracao : supera_dev_seed.gmcore.{destino_table}.updated_at")
                mapped_fields.add('email')
                mapped_fields.add('telefone')
                mapped_fields.add('celulargerente')
                mapped_fields.add('datainclusao')
                mapped_fields.add('dataalteracao')
                continue
            
            for dest_col in destino_structure:
                dest_name_lower = dest_col['name'].lower()
                dest_name = dest_col['name']
                
                # Se for ID gerado automaticamente (UUID)
                if dest_name_lower == 'id' and dest_col['type'] == 'UUID':
                    output_lines.append(f"  [GERADO AUTOMATICAMENTE] : supera_dev_seed.gmcore.{destino_table}.id")
                    mapped_fields.add('id')
                    continue
                
                # Verificar mapeamento manual primeiro
                mapped = False
                if destino_table in manual_mappings and dest_name_lower in manual_mappings[destino_table]:
                    origem_field = manual_mappings[destino_table][dest_name_lower]
                    origem_field_lower = origem_field.lower()
                    
                    # Verificar se é um campo preenchido pela lógica de migração
                    if origem_field.startswith('[PREENCHIDO PELA LOGICA'):
                        output_lines.append(f"  {origem_field} : supera_dev_seed.gmcore.{destino_table}.{dest_name}")
                        mapped = True
                    # Verificar se o campo existe na origem
                    elif origem_field_lower in origem_dict:
                        output_lines.append(f"  FINANCEIRO.dbo.{origem_table}.{origem_field} : supera_dev_seed.gmcore.{destino_table}.{dest_name}")
                        mapped = True
                        mapped_fields.add(origem_field_lower)
                
                # Se não mapeado manualmente, tentar encontrar correspondência automática
                if not mapped:
                    for orig_col in origem_structure:
                        orig_name_lower = orig_col['name'].lower()
                        orig_name = orig_col['name']
                        
                        # Pular se já foi mapeado
                        if orig_name_lower in mapped_fields:
                            continue
                        
                        # Mapear legacy_id
                        if dest_name_lower == 'legacy_id' and orig_name_lower == 'id':
                            output_lines.append(f"  FINANCEIRO.dbo.{origem_table}.{orig_name} : supera_dev_seed.gmcore.{destino_table}.{dest_name}")
                            mapped = True
                            mapped_fields.add(orig_name_lower)
                            break
                        
                        # Mapear por nome similar
                        if orig_name_lower == dest_name_lower or orig_name_lower.replace('_', '') == dest_name_lower.replace('_', ''):
                            output_lines.append(f"  FINANCEIRO.dbo.{origem_table}.{orig_name} : supera_dev_seed.gmcore.{destino_table}.{dest_name}")
                            mapped = True
                            mapped_fields.add(orig_name_lower)
                            break
                
                if not mapped:
                    unmapped_destino.append(dest_name)
            
            # Campos da origem não mapeados
            for orig_col in origem_structure:
                orig_name_lower = orig_col['name'].lower()
                orig_name = orig_col['name']
                
                # Pular ID (já mapeado) e campos já mapeados
                if orig_name_lower == 'id' or orig_name_lower in mapped_fields:
                    continue
                
                mapped = False
                for dest_col in destino_structure:
                    dest_name_lower = dest_col['name'].lower()
                    
                    # Verificar mapeamento manual reverso
                    if destino_table in manual_mappings:
                        for dest_field, orig_field in manual_mappings[destino_table].items():
                            if orig_name_lower == orig_field.lower() and dest_name_lower == dest_field.lower():
                                mapped = True
                                break
                    
                    if not mapped and (orig_name_lower == dest_name_lower or orig_name_lower.replace('_', '') == dest_name_lower.replace('_', '')):
                        mapped = True
                        break
                
                if not mapped:
                    unmapped_origem.append(orig_name)
            
            if unmapped_destino:
                output_lines.append("")
                output_lines.append("  CAMPOS DESTINO NAO MAPEADOS:")
                for field in unmapped_destino:
                    output_lines.append(f"    - {destino_table}.{field}")
            
            if unmapped_origem:
                output_lines.append("")
                output_lines.append("  CAMPOS ORIGEM NAO MAPEADOS:")
                for field in unmapped_origem:
                    output_lines.append(f"    - {origem_table}.{field}")
            
            output_lines.append("")
            output_lines.append("")
            
        except Exception as e:
            print(f"ERRO ao criar mapeamento para {origem_table} -> {destino_table}: {e}")
            output_lines.append(f"ERRO ao criar mapeamento: {e}")
            output_lines.append("")
    
    # Escrever arquivo
    output_file = os.path.join(os.path.dirname(__file__), 'stores_dictionary.txt')
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(output_lines))
    
    print(f"\nDicionario criado com sucesso: {output_file}")


if __name__ == "__main__":
    try:
        create_dictionary_file()
    except Exception as e:
        print(f"\nERRO ao criar dicionario: {e}")
        import traceback
        traceback.print_exc()

