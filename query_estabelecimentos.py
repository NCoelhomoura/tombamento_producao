"""
Script para executar query de estabelecimentos na origem (SQL Server PRD)
e exibir os resultados para acompanhamento.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.database_connection import DatabaseConnection
import pandas as pd

def query_estabelecimentos():
    """Executa query de estabelecimentos com filtros de data"""
    
    # Parâmetros de data
    data_aviso_previo_min = '2025-01-01'
    data_inicio_operacao_max = '2026-01-13'
    
    # Query simplificada - apenas IdEstabelecimento (sem subquery)
    query = """
    SELECT DISTINCT
        v.IdEstabelecimento
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE (CONVERT(DATE, v.DataAvisoPrevio) >= ? OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= ?
    """
    
    print("=" * 80)
    print("QUERY DE ESTABELECIMENTOS - ViewOrcamentosLojas")
    print("=" * 80)
    print(f"\nParâmetros:")
    print(f"  DataAvisoPrevio >= '{data_aviso_previo_min}'")
    print(f"  DataInicioOperacao <= '{data_inicio_operacao_max}'")
    print("\n" + "=" * 80)
    print("\nQuery SQL:")
    query_display = query.replace('?', f"'{data_aviso_previo_min}'", 1).replace('?', f"'{data_inicio_operacao_max}'", 1)
    print(query_display)
    print("\n" + "=" * 80)
    
    try:
        # Conectar ao SQL Server PRD
        print("\nConectando ao SQL Server PRD...")
        conn = DatabaseConnection.get_sql_server_prd_connection()
        cursor = conn.cursor()
        
        # Executar query com parâmetros
        print("Executando query...")
        cursor.execute(query, (data_aviso_previo_min, data_inicio_operacao_max))
        
        # Buscar todos os resultados
        print("Buscando resultados...")
        all_rows = cursor.fetchall()
        
        # Extrair IdEstabelecimento únicos (já são únicos devido ao DISTINCT)
        id_estabelecimento_list = [row[0] for row in all_rows if row[0] is not None]
        
        # Fechar conexão
        cursor.close()
        conn.close()
        
        # Exibir resultados
        print("\n" + "=" * 80)
        print("RESULTADOS")
        print("=" * 80)
        print(f"\nTotal de IdEstabelecimento únicos: {len(id_estabelecimento_list):,}")
        
        # Primeiros e últimos IDs
        print("\n" + "-" * 80)
        print("PRIMEIROS 10 IdEstabelecimento:")
        print("-" * 80)
        print(id_estabelecimento_list[:10])
        
        print("\n" + "-" * 80)
        print("ÚLTIMOS 10 IdEstabelecimento:")
        print("-" * 80)
        print(id_estabelecimento_list[-10:])
        
        # Estatísticas
        print("\n" + "-" * 80)
        print("ESTATÍSTICAS:")
        print("-" * 80)
        if id_estabelecimento_list:
            print(f"  Mínimo: {min(id_estabelecimento_list)}")
            print(f"  Máximo: {max(id_estabelecimento_list)}")
            print(f"  Total de IDs únicos: {len(id_estabelecimento_list):,}")
        
        print("\n" + "=" * 80)
        print("QUERY CONCLUÍDA COM SUCESSO!")
        print("=" * 80)
        
        return id_estabelecimento_list
        
    except Exception as e:
        print(f"\n{'=' * 80}")
        print("ERRO AO EXECUTAR QUERY")
        print("=" * 80)
        print(f"\nErro: {str(e)}")
        print(f"Tipo: {type(e).__name__}")
        import traceback
        print("\nTraceback:")
        traceback.print_exc()
        return None

if __name__ == "__main__":
    id_list = query_estabelecimentos()
    
    if id_list is not None:
        print(f"\nLista de IdEstabelecimento criada com {len(id_list):,} IDs únicos.")
        print("\nPara salvar em CSV, descomente a linha abaixo:")
        print("# pd.DataFrame({'IdEstabelecimento': id_list}).to_csv('estabelecimentos_resultado.csv', index=False)")
