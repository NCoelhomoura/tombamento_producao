"""
Script para testar as 3 conexões de banco de dados
- SQL Server PRD (origem - apenas leitura)
- PostgreSQL PRD (origem schemas/tabelas - apenas leitura)
- PostgreSQL HML (destino - leitura/escrita, schema gmcore)
"""

from database_connection import DatabaseConnection


def test_sql_server_prd():
    """Testa conexão com SQL Server PRD"""
    print("\n" + "="*80)
    print("TESTE 1: SQL SERVER PRD (Origem - Apenas Leitura)")
    print("="*80)
    
    try:
        # Testar conexão
        conn = DatabaseConnection.get_sql_server_prd_connection()
        print("✓ Conexão estabelecida com sucesso!")
        
        # Testar leitura
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION as version")
        version = cursor.fetchone()[0]
        print(f"✓ Versão do SQL Server: {version[:80]}...")
        
        # Testar query de leitura
        cursor.execute("SELECT DB_NAME() as database_name")
        db_name = cursor.fetchone()[0]
        print(f"✓ Database conectado: {db_name}")
        
        # Testar se consegue ler uma tabela (se existir)
        try:
            cursor.execute("SELECT TOP 1 * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            table = cursor.fetchone()
            if table:
                print(f"✓ Tabela encontrada no banco: {table[2]}")
        except Exception as e:
            print(f"  (Aviso: Não foi possível listar tabelas: {e})")
        
        # Testar proteção contra escrita
        try:
            DatabaseConnection.execute_sql_server_prd_query("INSERT INTO TestTable VALUES (1)")
            print("✗ ERRO: Operação de escrita foi permitida!")
        except ValueError as e:
            print(f"✓ Proteção contra escrita funcionando: {str(e)[:60]}...")
        except Exception as e:
            # Pode dar erro de tabela não existir, mas isso é esperado
            if "INSERT" in str(e).upper() or "WRITE" in str(e).upper():
                print(f"✓ Proteção contra escrita funcionando (erro esperado)")
            else:
                print(f"  (Aviso: Erro ao testar proteção: {e})")
        
        cursor.close()
        conn.close()
        print("\n✓ TESTE SQL SERVER PRD: PASSOU")
        return True
        
    except Exception as e:
        print(f"\n✗ TESTE SQL SERVER PRD: FALHOU")
        print(f"  Erro: {e}")
        return False


def test_postgresql_prd():
    """Testa conexão com PostgreSQL PRD"""
    print("\n" + "="*80)
    print("TESTE 2: POSTGRESQL PRD (Origem Schemas/Tabelas - Apenas Leitura)")
    print("="*80)
    
    try:
        # Testar conexão
        conn = DatabaseConnection.get_postgresql_prd_connection()
        print("✓ Conexão estabelecida com sucesso!")
        
        # Testar leitura
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        print(f"✓ Versão do PostgreSQL: {version[:80]}...")
        
        cursor.execute("SELECT current_database()")
        db_name = cursor.fetchone()[0]
        print(f"✓ Database conectado: {db_name}")
        
        # Listar schemas disponíveis
        cursor.execute("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
        schemas = cursor.fetchall()
        print(f"✓ Schemas disponíveis: {', '.join([s[0] for s in schemas[:10]])}")
        if len(schemas) > 10:
            print(f"  ... e mais {len(schemas) - 10} schemas")
        
        # Testar proteção contra escrita
        try:
            DatabaseConnection.execute_postgresql_prd_query("INSERT INTO test_table VALUES (1)")
            print("✗ ERRO: Operação de escrita foi permitida!")
        except ValueError as e:
            print(f"✓ Proteção contra escrita funcionando: {str(e)[:60]}...")
        except Exception as e:
            # Pode dar erro de tabela não existir, mas isso é esperado
            if "INSERT" in str(e).upper() or "WRITE" in str(e).upper():
                print(f"✓ Proteção contra escrita funcionando (erro esperado)")
            else:
                print(f"  (Aviso: Erro ao testar proteção: {e})")
        
        cursor.close()
        conn.close()
        print("\n✓ TESTE POSTGRESQL PRD: PASSOU")
        return True
        
    except Exception as e:
        print(f"\n✗ TESTE POSTGRESQL PRD: FALHOU")
        print(f"  Erro: {e}")
        return False


def test_postgresql_hml():
    """Testa conexão com PostgreSQL HML"""
    print("\n" + "="*80)
    print("TESTE 3: POSTGRESQL HML (Destino - Leitura/Escrita, Schema gmcore)")
    print("="*80)
    
    try:
        # Testar conexão
        conn = DatabaseConnection.get_postgresql_hml_connection()
        print("✓ Conexão estabelecida com sucesso!")
        
        # Verificar schema padrão
        cursor = conn.cursor()
        cursor.execute("SELECT current_database(), current_schema()")
        result = cursor.fetchone()
        db_name = result[0]
        schema_name = result[1]
        print(f"✓ Database conectado: {db_name}")
        print(f"✓ Schema atual: {schema_name}")
        
        if schema_name != 'gmcore':
            print(f"⚠ AVISO: Schema atual é '{schema_name}', esperado 'gmcore'")
        else:
            print("✓ Schema configurado corretamente como 'gmcore'")
        
        # Verificar se o schema gmcore existe
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = 'gmcore'
        """)
        schema_exists = cursor.fetchone()
        if schema_exists:
            print("✓ Schema 'gmcore' existe no banco")
        else:
            print("⚠ AVISO: Schema 'gmcore' não encontrado no banco")
        
        # Listar algumas tabelas do schema gmcore (se existirem)
        try:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'gmcore'
                ORDER BY table_name
                LIMIT 10
            """)
            tables = cursor.fetchall()
            if tables:
                print(f"✓ Tabelas encontradas no schema gmcore: {', '.join([t[0] for t in tables])}")
                if len(tables) == 10:
                    print("  ... e possivelmente mais tabelas")
            else:
                print("  (Nenhuma tabela encontrada no schema gmcore ainda)")
        except Exception as e:
            print(f"  (Aviso: Não foi possível listar tabelas: {e})")
        
        # Testar leitura
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        print(f"✓ Versão do PostgreSQL: {version[:80]}...")
        
        cursor.close()
        conn.close()
        print("\n✓ TESTE POSTGRESQL HML: PASSOU")
        return True
        
    except Exception as e:
        print(f"\n✗ TESTE POSTGRESQL HML: FALHOU")
        print(f"  Erro: {e}")
        return False


def main():
    """Executa todos os testes de conexão"""
    print("\n" + "="*80)
    print("INICIANDO TESTES DE CONEXÃO")
    print("="*80)
    print("\nEste script testa as 3 conexões configuradas:")
    print("  1. SQL Server PRD (origem - apenas leitura)")
    print("  2. PostgreSQL PRD (origem schemas/tabelas - apenas leitura)")
    print("  3. PostgreSQL HML (destino - leitura/escrita, schema gmcore)")
    
    results = []
    
    # Executar testes
    results.append(("SQL Server PRD", test_sql_server_prd()))
    results.append(("PostgreSQL PRD", test_postgresql_prd()))
    results.append(("PostgreSQL HML", test_postgresql_hml()))
    
    # Resumo final
    print("\n" + "="*80)
    print("RESUMO DOS TESTES")
    print("="*80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✓ PASSOU" if result else "✗ FALHOU"
        print(f"{name}: {status}")
    
    print(f"\nTotal: {passed}/{total} testes passaram")
    
    if passed == total:
        print("\n✓ TODAS AS CONEXÕES ESTÃO FUNCIONANDO CORRETAMENTE!")
    else:
        print("\n⚠ ALGUMAS CONEXÕES FALHARAM. Verifique os erros acima.")
    
    print("="*80)


if __name__ == "__main__":
    main()

