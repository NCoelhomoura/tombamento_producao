"""
Script para criar dicionário de migração de dados
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
        {'destino': 'addresses', 'origem': 'Cliente'},
        {'destino': 'customer_segments', 'origem': 'SegmentoProduto'},
        {'destino': 'customers', 'origem': 'Cliente'}
    ]
    
    output_lines = []
    output_lines.append("="*100)
    output_lines.append("DICIONARIO DE MIGRACAO DE DADOS")
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
        
        print(f"\nProcessando: {destino_table} <- {origem_table}")
        
        try:
            # Obter estruturas
            origem_structure = get_sql_server_table_structure(origem_table)
            destino_structure = get_postgresql_table_structure(destino_table)
            
            # Cabeçalho da seção
            output_lines.append("="*100)
            output_lines.append(f"TABELA DESTINO: {destino_table.upper()}")
            output_lines.append(f"TABELA ORIGEM: {origem_table}")
            output_lines.append("="*100)
            output_lines.append("")
            
            # Tabela de origem
            output_lines.append(f"ORIGEM - SQL Server PRD - Tabela: {origem_table}")
            output_lines.append("-"*100)
            output_lines.append(f"{'Nome':<40} {'Tipo':<30} {'Tamanho':<15} {'Nullable':<10} {'PK':<5}")
            output_lines.append("-"*100)
            
            for col in origem_structure:
                pk_mark = "SIM" if col['primary_key'] else "NAO"
                nullable_mark = "SIM" if col['nullable'] == 'YES' else "NAO"
                size_str = format_size(col['size'])
                output_lines.append(f"{col['name']:<40} {col['type']:<30} {size_str:<15} {nullable_mark:<10} {pk_mark:<5}")
            
            output_lines.append("")
            output_lines.append("")
            
            # Tabela de destino
            output_lines.append(f"DESTINO - PostgreSQL HML - Tabela: gmcore.{destino_table}")
            output_lines.append("-"*100)
            output_lines.append(f"{'Nome':<40} {'Tipo':<30} {'Tamanho':<15} {'Nullable':<10} {'PK':<5}")
            output_lines.append("-"*100)
            
            for col in destino_structure:
                pk_mark = "SIM" if col['primary_key'] else "NAO"
                nullable_mark = "SIM" if col['nullable'] == 'YES' else "NAO"
                size_str = format_size(col['size'])
                output_lines.append(f"{col['name']:<40} {col['type']:<30} {size_str:<15} {nullable_mark:<10} {pk_mark:<5}")
            
            output_lines.append("")
            output_lines.append("")
            
        except Exception as e:
            output_lines.append(f"ERRO ao processar {destino_table} <- {origem_table}: {e}")
            output_lines.append("")
    
    # Adicionar seção de mapeamento detalhado
    output_lines.append("")
    output_lines.append("="*100)
    output_lines.append("MAPEAMENTO DETALHADO CAMPO A CAMPO")
    output_lines.append("="*100)
    output_lines.append("")
    output_lines.append("Formato: origem.dbo.tabela.campo : destino.schema.tabela.campo")
    output_lines.append("")
    
    for mapping in mappings:
        destino_table = mapping['destino']
        origem_table = mapping['origem']
        
        print(f"\nCriando mapeamento detalhado: {destino_table} <- {origem_table}")
        
        try:
            origem_structure = get_sql_server_table_structure(origem_table)
            destino_structure = get_postgresql_table_structure(destino_table)
            
            output_lines.append("")
            output_lines.append("="*100)
            output_lines.append(f"MIGRACAO: {origem_table.upper()} -> {destino_table.upper()}")
            output_lines.append("="*100)
            output_lines.append("")
            output_lines.append(f"ORIGEM: FINANCEIRO.dbo.{origem_table}")
            output_lines.append(f"DESTINO: supera_dev_seed.gmcore.{destino_table}")
            output_lines.append("")
            output_lines.append("-"*100)
            output_lines.append(f"{'ORIGEM':<50} : {'DESTINO':<50}")
            output_lines.append("-"*100)
            
            # Criar mapeamento baseado em nomes similares e lógica comum
            origem_fields = {col['name'].lower(): col for col in origem_structure}
            destino_fields = {col['name'].lower(): col for col in destino_structure}
            
            # Listar todos os campos de origem
            output_lines.append("")
            output_lines.append("CAMPOS DISPONIVEIS NA ORIGEM:")
            for col in origem_structure:
                origem_path = f"FINANCEIRO.dbo.{origem_table}.{col['name']}"
                output_lines.append(f"  {origem_path}")
            
            output_lines.append("")
            output_lines.append("CAMPOS DISPONIVEIS NO DESTINO:")
            for col in destino_structure:
                destino_path = f"supera_dev_seed.gmcore.{destino_table}.{col['name']}"
                output_lines.append(f"  {destino_path}")
            
            output_lines.append("")
            output_lines.append("MAPEAMENTO SUGERIDO (baseado em nomes similares):")
            output_lines.append("-"*100)
            
            # Tentar fazer mapeamento automático baseado em nomes similares
            mapped_origem = set()
            mapped_destino = set()
            
            for dest_col in destino_structure:
                dest_name_lower = dest_col['name'].lower()
                dest_type_lower = dest_col['type'].lower()
                
                # Ignorar campos ID que são UUIDs gerados automaticamente
                # Exceto legacy_id que deve receber o ID da origem
                if dest_name_lower == 'id' and ('uuid' in dest_type_lower or dest_col['type'] == 'UUID'):
                    output_lines.append(f"{'[GERADO AUTOMATICAMENTE]':<50} : supera_dev_seed.gmcore.{destino_table}.{dest_col['name']}")
                    mapped_destino.add(dest_col['name'])
                    continue
                
                # Ignorar campos que são preenchidos pela lógica de migração
                if dest_name_lower in ['addressable_id', 'addressable_type']:
                    output_lines.append(f"{'[PREENCHIDO PELA LOGICA DE MIGRACAO]':<50} : supera_dev_seed.gmcore.{destino_table}.{dest_col['name']}")
                    mapped_destino.add(dest_col['name'])
                    continue
                
                best_match = None
                best_score = 0
                
                # Procurar correspondência na origem
                for orig_col in origem_structure:
                    orig_name_lower = orig_col['name'].lower()
                    
                    # Mapeamento especial para legacy_id
                    if dest_name_lower == 'legacy_id':
                        if orig_name_lower == 'id':
                            origem_path = f"FINANCEIRO.dbo.{origem_table}.{orig_col['name']}"
                            destino_path = f"supera_dev_seed.gmcore.{destino_table}.{dest_col['name']}"
                            output_lines.append(f"{origem_path:<50} : {destino_path:<50}")
                            mapped_origem.add(orig_col['name'])
                            mapped_destino.add(dest_col['name'])
                            break
                        continue
                    
                    # Mapeamentos específicos por nome exato (prioridade muito alta)
                    exact_mappings = {
                        'address_line_2': ['complemento'],
                        'complement1': ['complemento'],
                        'complement2': ['complementocobranca']
                    }
                    
                    # Verificar mapeamentos exatos primeiro
                    score = 0
                    for dest_key, orig_keys in exact_mappings.items():
                        if dest_key == dest_name_lower:
                            for orig_key in orig_keys:
                                if orig_key in orig_name_lower:
                                    score = 95  # Prioridade muito alta
                                    break
                            if score >= 95:
                                break
                    
                    # Se não encontrou mapeamento exato, usar lógica geral
                    if score < 95:
                        # Pontuação baseada em correspondência exata ou parcial
                        if dest_name_lower == orig_name_lower:
                            score = 100
                        elif dest_name_lower in orig_name_lower or orig_name_lower in dest_name_lower:
                            score = 50
                        elif dest_name_lower.replace('_', '') == orig_name_lower.replace('_', ''):
                            score = 40
                        else:
                            # Verificar palavras-chave comuns
                            common_keywords = {
                                'name': ['nome', 'razaosocial', 'nomefantasia'],
                                'email': ['email'],
                                'phone': ['telefone', 'celular'],
                                'address': ['endereco'],
                                'street': ['endereco'],
                                'city': ['cidade'],
                                'state': ['uf'],
                                'postal': ['cep'],
                                'postal_code': ['cep'],
                                'number': ['numero'],
                                'neighborhood': ['bairro'],
                                'district': ['bairro'],
                                'complement': ['complemento'],
                                'municipal_code': ['codigomunicipio'],
                                'created': ['datainclusao'],
                                'updated': ['dataalteracao'],
                                'active': ['ativo'],
                                'is_active': ['ativo'],
                                'cnpj': ['cpfcnpj'],
                                'registration': ['inscricao'],
                                'state_registration': ['inscricaestadual'],
                                'municipal_registration': ['inscricaomunicipal'],
                                'legal_name': ['razaosocial'],
                                'trade_name': ['nomefantasia'],
                                'latitude': ['latitude'],
                                'longitude': ['longitude']
                            }
                            
                            for key, keywords in common_keywords.items():
                                if key in dest_name_lower:
                                    for keyword in keywords:
                                        if keyword in orig_name_lower:
                                            score = max(score, 30)
                                            break
                    
                    if score > best_score:
                        best_score = score
                        best_match = orig_col
                
                # Aplicar mapeamento se encontrou correspondência
                if best_match and best_score >= 30 and dest_name_lower != 'id':
                    origem_path = f"FINANCEIRO.dbo.{origem_table}.{best_match['name']}"
                    destino_path = f"supera_dev_seed.gmcore.{destino_table}.{dest_col['name']}"
                    output_lines.append(f"{origem_path:<50} : {destino_path:<50}")
                    mapped_origem.add(best_match['name'])
                    mapped_destino.add(dest_col['name'])
                elif dest_name_lower not in mapped_destino and dest_name_lower != 'id':
                    # Campo de destino não mapeado (será listado depois)
                    pass
            
            # Campos de destino não mapeados
            output_lines.append("")
            output_lines.append("CAMPOS DE DESTINO NAO MAPEADOS:")
            for col in destino_structure:
                if col['name'] not in mapped_destino:
                    destino_path = f"supera_dev_seed.gmcore.{destino_table}.{col['name']}"
                    output_lines.append(f"  {destino_path:<50} : [MAPEAMENTO MANUAL NECESSARIO]")
            
            # Campos de origem não mapeados
            output_lines.append("")
            output_lines.append("CAMPOS DE ORIGEM NAO MAPEADOS:")
            for col in origem_structure:
                if col['name'] not in mapped_origem:
                    origem_path = f"FINANCEIRO.dbo.{origem_table}.{col['name']}"
                    output_lines.append(f"  {origem_path:<50} : [NAO MAPEADO]")
            
            output_lines.append("")
            output_lines.append("")
            
        except Exception as e:
            output_lines.append(f"ERRO ao criar mapeamento {destino_table} <- {origem_table}: {e}")
            output_lines.append("")
    
    # Escrever arquivo
    output_content = "\n".join(output_lines)
    
    with open('customers_dictionary.txt', 'w', encoding='utf-8') as f:
        f.write(output_content)
    
    print(f"\nOK - Dicionario criado com sucesso: customers_dictionary.txt")
    print(f"Total de linhas: {len(output_lines)}")


if __name__ == "__main__":
    print("\n" + "="*100)
    print("CRIANDO DICIONARIO DE MIGRACAO DE DADOS")
    print("="*100)
    
    create_dictionary_file()
    
    print("\n" + "="*100)
    print("PROCESSO CONCLUIDO")
    print("="*100)

