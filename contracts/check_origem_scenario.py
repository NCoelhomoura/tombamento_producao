"""
Script para verificar na origem se existe registro com os valores do scenario.
"""

import sys
import os

# Adicionar diretório raiz do projeto ao path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Adicionar diretório utils ao path
utils_path = os.path.join(project_root, 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from utils.database_connection import DatabaseConnection

def check_origem(id_orcamento, frequencia, horas, valor_hora, data_inicio, data_aviso_previo=None):
    """
    Verifica na origem se existe registro com esses valores.
    """
    print("="*100)
    print(f"VERIFICANDO NA ORIGEM")
    print("="*100)
    print(f"IdOrcamento: {id_orcamento}")
    print(f"Frequencia: {frequencia}")
    print(f"Horas: {horas}")
    print(f"ValorHora: {valor_hora}")
    print(f"DataInicioOperacao: {data_inicio}")
    print(f"DataAvisoPrevio: {data_aviso_previo}")
    print()
    
    # Normalizar valor_hora (pode vir com vírgula)
    try:
        valor_hora_float = float(str(valor_hora).replace(',', '.'))
    except ValueError:
        print(f"[ERRO] Nao foi possivel converter ValorHora: {valor_hora}")
        return
    
    # Normalizar horas
    try:
        horas_float = float(str(horas).replace(',', '.'))
    except ValueError:
        print(f"[ERRO] Nao foi possivel converter Horas: {horas}")
        return
    
    # Normalizar data_inicio
    data_inicio_str = data_inicio.split()[0] if ' ' in str(data_inicio) else str(data_inicio)
    
    conn_sql = DatabaseConnection.get_sql_server_prd_connection()
    cursor_sql = conn_sql.cursor()
    
    # Buscar registros na origem
    query = """
        SELECT 
            v.IdOrcamentoLoja,
            v.IdOrcamento,
            v.IdEstabelecimento,
            v.Frequencia,
            v.Horas,
            v.ValorHora,
            v.DataInicioOperacao,
            v.DataAvisoPrevio,
            v.NomeTarefa,
            v.NomeCliente,
            v.StatusPedido
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        WHERE v.IdOrcamento = ?
            AND v.Frequencia = ?
            AND CAST(v.Horas AS FLOAT) = ?
            AND CAST(v.ValorHora AS FLOAT) = ?
            AND CONVERT(DATE, v.DataInicioOperacao) = ?
    """
    
    params = [id_orcamento, frequencia, horas_float, valor_hora_float, data_inicio_str]
    
    if data_aviso_previo is None:
        query += " AND v.DataAvisoPrevio IS NULL"
    else:
        data_aviso_str = data_aviso_previo.split()[0] if ' ' in str(data_aviso_previo) else str(data_aviso_previo)
        query += " AND CONVERT(DATE, v.DataAvisoPrevio) = ?"
        params.append(data_aviso_str)
    
    query += " ORDER BY v.IdOrcamentoLoja"
    
    print(f"[QUERY] Executando query na origem...")
    print(f"Query: {query}")
    print(f"Params: {params}")
    print()
    
    cursor_sql.execute(query, params)
    rows = cursor_sql.fetchall()
    
    print(f"[RESULTADO] Encontrados {len(rows)} registros na origem")
    print("-"*100)
    
    if rows:
        print("Registros encontrados:")
        for idx, row in enumerate(rows, 1):
            print(f"\n  {idx}. IdOrcamentoLoja: {row[0]}")
            print(f"     IdOrcamento: {row[1]}")
            print(f"     IdEstabelecimento: {row[2]}")
            print(f"     Frequencia: {row[3]}")
            print(f"     Horas: {row[4]}")
            print(f"     ValorHora: {row[5]}")
            print(f"     DataInicioOperacao: {row[6]}")
            print(f"     DataAvisoPrevio: {row[7]}")
            print(f"     NomeTarefa: {row[8]}")
            print(f"     NomeCliente: {row[9]}")
            print(f"     StatusPedido: {row[10]}")
    else:
        print("[AVISO] Nenhum registro encontrado na origem com esses valores exatos!")
        print("\nBuscando registros similares (mesmo IdOrcamento, Frequencia e DataInicioOperacao)...")
        
        # Buscar registros similares
        query_similar = """
            SELECT 
                v.IdOrcamentoLoja,
                v.IdOrcamento,
                v.IdEstabelecimento,
                v.Frequencia,
                v.Horas,
                v.ValorHora,
                v.DataInicioOperacao,
                v.DataAvisoPrevio
            FROM ViewOrcamentosLojas v
            INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
            WHERE v.IdOrcamento = ?
                AND v.Frequencia = ?
                AND CONVERT(DATE, v.DataInicioOperacao) = ?
            ORDER BY v.IdOrcamentoLoja
        """
        
        cursor_sql.execute(query_similar, [id_orcamento, frequencia, data_inicio_str])
        similar_rows = cursor_sql.fetchall()
        
        print(f"\n[INFO] Encontrados {len(similar_rows)} registros com IdOrcamento={id_orcamento}, Frequencia={frequencia}, DataInicioOperacao={data_inicio_str}:")
        for idx, row in enumerate(similar_rows[:10], 1):  # Mostrar apenas primeiros 10
            print(f"  {idx}. IdOrcamentoLoja: {row[0]}, Horas: {row[4]}, ValorHora: {row[5]}, DataAvisoPrevio: {row[7]}")
        if len(similar_rows) > 10:
            print(f"  ... e mais {len(similar_rows) - 10} registros")
    
    cursor_sql.close()
    conn_sql.close()
    print("\n" + "="*100)

if __name__ == "__main__":
    # Valores do scenario a buscar
    id_orcamento = 6053
    frequencia = 1
    horas = 1
    valor_hora = 38.18
    data_inicio = '2025-02-01'
    data_aviso_previo = None
    
    try:
        check_origem(id_orcamento, frequencia, horas, valor_hora, data_inicio, data_aviso_previo)
    except Exception as e:
        print(f"[ERRO] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
