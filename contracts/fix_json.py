"""
Script para corrigir o JSON com todos os IdOrcamento que atendem aos filtros.
"""

import sys
import os
import json

# Adicionar diretório raiz do projeto ao path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Adicionar diretório utils ao path
utils_path = os.path.join(project_root, 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from utils.database_connection import DatabaseConnection

# Configurar destino como PRD
DatabaseConnection.set_destino('PRD')

def fix_json():
    """
    Busca todos os IdOrcamento que atendem aos filtros e atualiza o JSON.
    """
    print("="*100)
    print("CORRIGINDO JSON COM TODOS OS IdOrcamento")
    print("="*100)
    
    # Filtros aplicados
    data_aviso_previo_min = '2026-01-01'
    data_inicio_operacao_max = '2026-01-31'
    status_pedido_filter = [6, 7, 8]
    
    print(f"\nFiltros aplicados:")
    print(f"  - data_aviso_previo_min: {data_aviso_previo_min}")
    print(f"  - data_inicio_operacao_max: {data_inicio_operacao_max}")
    print(f"  - status_pedido_filter: {status_pedido_filter}")
    
    # Buscar todos os IdOrcamento que atendem aos filtros
    conn_sql = DatabaseConnection.get_sql_server_prd_connection()
    cursor_sql = conn_sql.cursor()
    
    query = """
        SELECT DISTINCT
            v.IdOrcamento,
            v.IdCliente,
            v.IdEstabelecimento,
            v.IdBandeira,
            v.IdRede
        FROM ViewOrcamentosLojas v
        INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
        WHERE (CONVERT(DATE, v.DataAvisoPrevio) >= ? OR v.DataAvisoPrevio IS NULL)
            AND CONVERT(DATE, v.DataInicioOperacao) <= ?
            AND v.StatusPedido IN (?,?,?)
            AND v.IdCliente IS NOT NULL
    """
    
    params = [data_aviso_previo_min, data_inicio_operacao_max, status_pedido_filter[0], status_pedido_filter[1], status_pedido_filter[2]]
    
    print(f"\n[QUERY] Executando query para coletar todos os IDs...")
    cursor_sql.execute(query, params)
    rows = cursor_sql.fetchall()
    
    # Coletar IDs únicos
    id_orcamento_set = set()
    id_cliente_set = set()
    id_estabelecimento_set = set()
    id_bandeira_set = set()
    id_rede_set = set()
    
    for row in rows:
        if row[0]: id_orcamento_set.add(row[0])
        if row[1]: id_cliente_set.add(row[1])
        if row[2]: id_estabelecimento_set.add(row[2])
        if row[3]: id_bandeira_set.add(row[3])
        if row[4]: id_rede_set.add(row[4])
    
    aggregated_ids = {
        'IdOrcamento': sorted(list(id_orcamento_set)),
        'IdCliente': sorted(list(id_cliente_set)),
        'IdEstabelecimento': sorted(list(id_estabelecimento_set)),
        'IdBandeira': sorted(list(id_bandeira_set)),
        'IdRede': sorted(list(id_rede_set))
    }
    
    print(f"\n[RESULTADO] IDs coletados:")
    print(f"  - IdOrcamento: {len(aggregated_ids['IdOrcamento'])}")
    print(f"  - IdCliente: {len(aggregated_ids['IdCliente'])}")
    print(f"  - IdEstabelecimento: {len(aggregated_ids['IdEstabelecimento'])}")
    print(f"  - IdBandeira: {len(aggregated_ids['IdBandeira'])}")
    print(f"  - IdRede: {len(aggregated_ids['IdRede'])}")
    
    if len(aggregated_ids['IdOrcamento']) > 0:
        print(f"\n[IdOrcamento] Primeiros 10: {aggregated_ids['IdOrcamento'][:10]}")
        print(f"[IdOrcamento] Últimos 10: {aggregated_ids['IdOrcamento'][-10:]}")
    
    cursor_sql.close()
    conn_sql.close()
    
    # Atualizar JSON
    json_path = os.path.join(os.path.dirname(__file__), 'contracts_filter_main.json')
    
    filter_data = {
        'filters_applied': {
            'id_orcamento': [],  # Lista vazia = todos os orçamentos
            'data_aviso_previo_min': data_aviso_previo_min,
            'data_inicio_operacao_max': data_inicio_operacao_max,
            'status_pedido': status_pedido_filter,
            'limit_rows': 0,
            'clear_data': True
        },
        'aggregated_ids': aggregated_ids,
        'execution_info': {
            'timestamp': '2026-01-29T21:30:00',
            'total_contracts_migrated': 0
        }
    }
    
    print(f"\n[SALVANDO] Atualizando JSON: {json_path}")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(filter_data, f, indent=2, ensure_ascii=False)
    
    print(f"[OK] JSON atualizado com sucesso!")
    print(f"  - Total de IdOrcamento: {len(aggregated_ids['IdOrcamento'])}")
    print(f"  - Total de IdCliente: {len(aggregated_ids['IdCliente'])}")
    print(f"  - Total de IdEstabelecimento: {len(aggregated_ids['IdEstabelecimento'])}")
    print("\n" + "="*100)

if __name__ == "__main__":
    try:
        fix_json()
    except Exception as e:
        print(f"[ERRO] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
