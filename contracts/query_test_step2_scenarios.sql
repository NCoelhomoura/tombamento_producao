-- ============================================================================
-- QUERY PARA TESTAR NO SSMS - STEP 2: CONTRACT_SCENARIOS
-- ============================================================================
-- Esta query é equivalente à query usada no step2_migrate_contract_scenarios
-- Substitua os valores dos filtros conforme necessário
-- ============================================================================

SELECT 
    v.IdOrcamento,
    v.Frequencia,
    v.Horas,
    v.ValorHora,
    CONVERT(DATE,v.DataInicioOperacao) AS DataInicioOperacao,
    CONVERT(DATE,v.DataAvisoPrevio) AS DataAvisoPrevio,
    MAX(v.NomeTarefa) AS NomeTarefa,
    MAX(v.NomeCliente) AS NomeCliente,
    MAX(v.StatusPedido) AS StatusPedido,
    MAX(v.DataInclusaoOrcamentoLojas) AS DataInclusaoOrcamentoLojas,
    MAX(v.DataAlteracaoOrcamentoLojas) AS DataAlteracaoOrcamentoLojas,
    MAX(v.IdTarefa) AS IdTarefa,
    MAX(v.IdCliente) AS IdCliente,
    COUNT(*) AS total_lojas  -- Adicionado para comparar com sua query
FROM ViewOrcamentosLojas v
INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
WHERE 
    -- Filtros aplicados (ajuste conforme necessário)
    (CONVERT(DATE, v.DataAvisoPrevio) >= '2026-01-01' OR v.DataAvisoPrevio IS NULL)
    AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-31'
    AND v.StatusPedido IN (6, 7, 8)
    AND v.IdCliente IS NOT NULL
    -- Adicione filtro de IdOrcamento se necessário:
    -- AND v.IdOrcamento IN (6053, 13174, ...)
GROUP BY 
    v.IdOrcamento,
    v.Frequencia,
    v.Horas,
    v.ValorHora,
    CONVERT(DATE,v.DataInicioOperacao),
    CONVERT(DATE,v.DataAvisoPrevio)
ORDER BY 
    v.IdOrcamento, 
    v.Frequencia, 
    v.Horas, 
    v.ValorHora, 
    CONVERT(DATE,v.DataInicioOperacao), 
    CONVERT(DATE,v.DataAvisoPrevio)

-- ============================================================================
-- QUERY ALTERNATIVA: Com filtro adicional de DataAvisoPrevio IS NULL
-- (conforme sua query de exemplo)
-- ============================================================================

SELECT 
    v.IdOrcamento,
    v.Frequencia,
    v.Horas,
    v.ValorHora,
    CONVERT(DATE,v.DataInicioOperacao) AS DataInicioOperacao,
    CONVERT(DATE,v.DataAvisoPrevio) AS DataAvisoPrevio,
    MAX(v.NomeTarefa) AS NomeTarefa,
    MAX(v.NomeCliente) AS NomeCliente,
    MAX(v.StatusPedido) AS StatusPedido,
    MAX(v.DataInclusaoOrcamentoLojas) AS DataInclusaoOrcamentoLojas,
    MAX(v.DataAlteracaoOrcamentoLojas) AS DataAlteracaoOrcamentoLojas,
    MAX(v.IdTarefa) AS IdTarefa,
    MAX(v.IdCliente) AS IdCliente,
    COUNT(*) AS total_lojas
FROM ViewOrcamentosLojas v
INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
WHERE 
    (CONVERT(DATE, v.DataAvisoPrevio) >= '2026-01-01' OR v.DataAvisoPrevio IS NULL)
    AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-31'
    AND v.StatusPedido IN (6, 7, 8)
    AND v.IdCliente IS NOT NULL
    AND v.DataAvisoPrevio IS NULL  -- Filtro adicional conforme sua query
GROUP BY 
    v.IdOrcamento,
    v.Frequencia,
    v.Horas,
    v.ValorHora,
    CONVERT(DATE,v.DataInicioOperacao),
    CONVERT(DATE,v.DataAvisoPrevio)
ORDER BY 
    v.IdOrcamento, 
    v.Frequencia, 
    v.Horas, 
    v.ValorHora, 
    CONVERT(DATE,v.DataInicioOperacao), 
    CONVERT(DATE,v.DataAvisoPrevio)

-- ============================================================================
-- VERIFICAÇÃO: Contar total de linhas retornadas
-- ============================================================================

SELECT COUNT(*) AS total_cenarios_unicos
FROM (
    SELECT 
        v.IdOrcamento,
        v.Frequencia,
        v.Horas,
        v.ValorHora,
        CONVERT(DATE,v.DataInicioOperacao) AS DataInicioOperacao,
        CONVERT(DATE,v.DataAvisoPrevio) AS DataAvisoPrevio
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 
        (CONVERT(DATE, v.DataAvisoPrevio) >= '2026-01-01' OR v.DataAvisoPrevio IS NULL)
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-31'
        AND v.StatusPedido IN (6, 7, 8)
        AND v.IdCliente IS NOT NULL
        AND v.DataAvisoPrevio IS NULL
    GROUP BY 
        v.IdOrcamento,
        v.Frequencia,
        v.Horas,
        v.ValorHora,
        CONVERT(DATE,v.DataInicioOperacao),
        CONVERT(DATE,v.DataAvisoPrevio)
) AS cenarios_unicos
