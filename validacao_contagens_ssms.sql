-- ================================================================================
-- SCRIPT DE VALIDAÇÃO DE CONTAGENS - STORES MIGRATION
-- Data/Hora da Execução: 2026-01-14 14:04:22
-- Filtros Aplicados: DataAvisoPrevio >= '2025-01-01', DataInicioOperacao <= '2026-01-14'
-- ================================================================================

-- ================================================================================
-- 1. QUERY BASE - ViewOrcamentosLojas (mesma query usada no código)
-- ================================================================================
-- Esta query retorna todas as linhas da ViewOrcamentosLojas com os filtros aplicados
-- Resultado esperado: 60306 linhas

SELECT DISTINCT
    v.IdOrcamento,
    v.IdCliente,
    v.IdEstabelecimento,
    v.IdBandeira,
    v.IdRede
FROM ViewOrcamentosLojas v
INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
WHERE 1 = 1 
    AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
    AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14';

-- Contagem total de linhas
SELECT COUNT(*) AS TotalLinhas
FROM (
    SELECT DISTINCT
        v.IdOrcamento,
        v.IdCliente,
        v.IdEstabelecimento,
        v.IdBandeira,
        v.IdRede
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 1 = 1 
        AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14'
) base;
-- Esperado: 60306 linhas


-- ================================================================================
-- 2. CONTAGENS DE IDs ÚNICOS DA ViewOrcamentosLojas
-- ================================================================================

-- 2.1 IdOrcamento únicos
SELECT COUNT(DISTINCT base.IdOrcamento) AS TotalIdOrcamento
FROM (
    SELECT DISTINCT
        v.IdOrcamento,
        v.IdCliente,
        v.IdEstabelecimento,
        v.IdBandeira,
        v.IdRede
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 1 = 1 
        AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14'
) base;
-- Esperado: 773 registros

-- 2.2 IdCliente únicos
SELECT COUNT(DISTINCT base.IdCliente) AS TotalIdCliente
FROM (
    SELECT DISTINCT
        v.IdOrcamento,
        v.IdCliente,
        v.IdEstabelecimento,
        v.IdBandeira,
        v.IdRede
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 1 = 1 
        AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14'
) base
WHERE base.IdCliente IS NOT NULL;
-- Esperado: 628 registros

-- 2.3 IdEstabelecimento únicos
SELECT COUNT(DISTINCT base.IdEstabelecimento) AS TotalIdEstabelecimento
FROM (
    SELECT DISTINCT
        v.IdOrcamento,
        v.IdCliente,
        v.IdEstabelecimento,
        v.IdBandeira,
        v.IdRede
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 1 = 1 
        AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14'
) base
WHERE base.IdEstabelecimento IS NOT NULL;
-- Esperado: 11270 registros

-- 2.4 IdBandeira únicos
SELECT COUNT(DISTINCT base.IdBandeira) AS TotalIdBandeira
FROM (
    SELECT DISTINCT
        v.IdOrcamento,
        v.IdCliente,
        v.IdEstabelecimento,
        v.IdBandeira,
        v.IdRede
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 1 = 1 
        AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14'
) base
WHERE base.IdBandeira IS NOT NULL;
-- Esperado: 1037 registros

-- 2.5 IdRede únicos
SELECT COUNT(DISTINCT base.IdRede) AS TotalIdRede
FROM (
    SELECT DISTINCT
        v.IdOrcamento,
        v.IdCliente,
        v.IdEstabelecimento,
        v.IdBandeira,
        v.IdRede
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 1 = 1 
        AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14'
) base
WHERE base.IdRede IS NOT NULL;
-- Esperado: 974 registros


-- ================================================================================
-- 3. VALIDAÇÃO DAS TABELAS DE ORIGEM (SQL Server PRD)
-- ================================================================================

-- 3.1 Store Segments (origem)
SELECT COUNT(*) AS TotalStoreSegments
FROM SegmentoEstabelecimento;
-- Esperado: 14 registros

-- 3.2 Retail Chains (Rede) - filtrado por IdRede da ViewOrcamentosLojas
-- Primeiro, vamos obter os IdRede únicos
DECLARE @IdRedeList TABLE (IdRede INT);
INSERT INTO @IdRedeList (IdRede)
SELECT DISTINCT base.IdRede
FROM (
    SELECT DISTINCT
        v.IdOrcamento,
        v.IdCliente,
        v.IdEstabelecimento,
        v.IdBandeira,
        v.IdRede
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 1 = 1 
        AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14'
) base
WHERE base.IdRede IS NOT NULL;

-- Contar Redes filtradas
SELECT COUNT(*) AS TotalRetailChains
FROM Rede r
WHERE r.Id IN (SELECT IdRede FROM @IdRedeList);
-- Esperado: 974 registros

-- 3.3 Store Brands (Bandeira) - filtrado por IdBandeira da ViewOrcamentosLojas
DECLARE @IdBandeiraList TABLE (IdBandeira INT);
INSERT INTO @IdBandeiraList (IdBandeira)
SELECT DISTINCT base.IdBandeira
FROM (
    SELECT DISTINCT
        v.IdOrcamento,
        v.IdCliente,
        v.IdEstabelecimento,
        v.IdBandeira,
        v.IdRede
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 1 = 1 
        AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14'
) base
WHERE base.IdBandeira IS NOT NULL;

-- Contar Bandeiras filtradas
SELECT COUNT(*) AS TotalStoreBrands
FROM Bandeira b
WHERE b.Id IN (SELECT IdBandeira FROM @IdBandeiraList);
-- Esperado: 1037 registros

-- 3.4 Stores (Estabelecimento) - filtrado por IdEstabelecimento da ViewOrcamentosLojas
DECLARE @IdEstabelecimentoList TABLE (IdEstabelecimento INT);
INSERT INTO @IdEstabelecimentoList (IdEstabelecimento)
SELECT DISTINCT base.IdEstabelecimento
FROM (
    SELECT DISTINCT
        v.IdOrcamento,
        v.IdCliente,
        v.IdEstabelecimento,
        v.IdBandeira,
        v.IdRede
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 1 = 1 
        AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14'
) base
WHERE base.IdEstabelecimento IS NOT NULL;

-- Contar Estabelecimentos filtrados
SELECT COUNT(*) AS TotalStores
FROM Estabelecimento e
WHERE e.Id IN (SELECT IdEstabelecimento FROM @IdEstabelecimentoList);
-- Esperado: 11270 registros

-- 3.5 Store CNPJs - filtrado por IdEstabelecimento
SELECT COUNT(*) AS TotalStoreCNPJs
FROM EstabelecimentoCNPJ ec
WHERE ec.IdEstabelecimento IN (SELECT IdEstabelecimento FROM @IdEstabelecimentoList);
-- Esperado: 11269 registros (pode ser menor que Stores se algum estabelecimento não tiver CNPJ)

-- 3.6 Addresses (Enderecos) - filtrado por IdEstabelecimento
SELECT COUNT(*) AS TotalAddresses
FROM Endereco e
WHERE e.IdEstabelecimento IN (SELECT IdEstabelecimento FROM @IdEstabelecimentoList);
-- Esperado: 11270 registros

-- 3.7 Contacts (Contatos) - filtrado por IdEstabelecimento
SELECT COUNT(*) AS TotalContacts
FROM Contato c
WHERE c.IdEstabelecimento IN (SELECT IdEstabelecimento FROM @IdEstabelecimentoList);
-- Esperado: 7956 registros


-- ================================================================================
-- 4. RESUMO COMPARATIVO (ORIGEM vs DESTINO)
-- ================================================================================
-- Execute as queries abaixo e compare com os valores do log:

-- ORIGEM (SQL Server PRD):
-- Store Segments: 14
-- Retail Chains: 974
-- Store Brands: 1037
-- Stores: 11270
-- Store CNPJs: 11269
-- Addresses: 11270
-- Contacts: 7956

-- DESTINO (PostgreSQL HML - conforme log):
-- Store Segments: 14
-- Retail Chains: 974
-- Store Brands: 1037
-- Stores: 11270
-- Store CNPJs: 11269
-- Addresses: 11270
-- Contacts: 7956

-- ================================================================================
-- 5. QUERY ALTERNATIVA PARA VALIDAÇÃO RÁPIDA (sem usar variáveis de tabela)
-- ================================================================================
-- Use esta query se as variáveis de tabela não funcionarem:

-- Retail Chains
SELECT COUNT(*) AS TotalRetailChains
FROM Rede r
WHERE r.Id IN (
    SELECT DISTINCT v.IdRede
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 1 = 1 
        AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14'
        AND v.IdRede IS NOT NULL
);

-- Store Brands
SELECT COUNT(*) AS TotalStoreBrands
FROM Bandeira b
WHERE b.Id IN (
    SELECT DISTINCT v.IdBandeira
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 1 = 1 
        AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14'
        AND v.IdBandeira IS NOT NULL
);

-- Stores
SELECT COUNT(*) AS TotalStores
FROM Estabelecimento e
WHERE e.Id IN (
    SELECT DISTINCT v.IdEstabelecimento
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 1 = 1 
        AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14'
        AND v.IdEstabelecimento IS NOT NULL
);

-- Store CNPJs
SELECT COUNT(*) AS TotalStoreCNPJs
FROM EstabelecimentoCNPJ ec
WHERE ec.IdEstabelecimento IN (
    SELECT DISTINCT v.IdEstabelecimento
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 1 = 1 
        AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14'
        AND v.IdEstabelecimento IS NOT NULL
);

-- Addresses
SELECT COUNT(*) AS TotalAddresses
FROM Endereco e
WHERE e.IdEstabelecimento IN (
    SELECT DISTINCT v.IdEstabelecimento
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 1 = 1 
        AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14'
        AND v.IdEstabelecimento IS NOT NULL
);

-- Contacts
SELECT COUNT(*) AS TotalContacts
FROM Contato c
WHERE c.IdEstabelecimento IN (
    SELECT DISTINCT v.IdEstabelecimento
    FROM ViewOrcamentosLojas v
    INNER JOIN Orcamento o ON o.Id = v.IdOrcamento
    WHERE 1 = 1 
        AND (CONVERT(DATE, v.DataAvisoPrevio) >= '2025-01-01' OR v.DataAvisoPrevio IS NULL) 
        AND CONVERT(DATE, v.DataInicioOperacao) <= '2026-01-14'
        AND v.IdEstabelecimento IS NOT NULL
);

-- ================================================================================
-- FIM DO SCRIPT
-- ================================================================================
