# Migration ERP to Supera CORE

Sistema de migração de dados do SQL Server PRD para PostgreSQL (HML/PRD), migrando dados de clientes e estabelecimentos para o schema `core`/`gmcore`.

## 📋 Índice

- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Configuração](#configuração)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Uso do Orquestrador](#uso-do-orquestrador)
- [Exemplos de Execução](#exemplos-de-execução)
- [Logs e Relatórios](#logs-e-relatórios)
- [Troubleshooting](#troubleshooting)

## 🔧 Pré-requisitos

- Python 3.7 ou superior
- Acesso às bases de dados:
  - SQL Server PRD (origem)
  - PostgreSQL HML (destino para testes)
  - PostgreSQL PRD (destino para produção)
- Credenciais de acesso configuradas no arquivo `diretrizes_migracao.txt`

## 📦 Instalação

### 1. Clonar o Repositório

```bash
git clone https://github.com/tarcisiomacci/migration-erp-to-supera-core.git
cd migration-erp-to-supera-core
```

### 2. Instalar Dependências

Crie um ambiente virtual (recomendado):

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

Instale as bibliotecas necessárias:

```bash
pip install pyodbc
pip install psycopg2-binary
```

**Nota:** Se encontrar problemas ao instalar `pyodbc` no Windows, você pode precisar baixar o driver ODBC para SQL Server:
- [Microsoft ODBC Driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

## ⚙️ Configuração

### 1. Configurar Credenciais de Banco de Dados

Edite o arquivo `diretrizes_migracao.txt` e configure as credenciais:

```
conexões

ORIGEM SQL PRD
Host: [seu-host-sql-server]
Database: FINANCEIRO
User: [seu-usuario]
Senha: [sua-senha]

DESTINO HML
host: [seu-host-postgresql-hml]
database: supera_dev_seed
schema: gmcore
port: 5432
user: [seu-usuario]
senha: [sua-senha]

DESTINO SCHEMAS TABELAS PRD
Server=[seu-host-postgresql-prd]
Port=5432
Database=gmcoredb
User Id=[seu-usuario]
Password=[sua-senha]
```

### 2. Configurar o Orquestrador

O arquivo `orchestrator_tasks.py` possui configurações padrão que podem ser ajustadas:

```python
# Limite padrão de linhas (0 = todos os dados)
LIMIT_ROWS = 0

# Destino padrão ('HML' ou 'PRD')
DESTINO_PADRAO = 'PRD'
```

**Localização:** Linhas 12-13 do arquivo `orchestrator_tasks.py`

## 📁 Estrutura do Projeto

```
migration-erp-to-supera-core/
│
├── README.md                          # Este arquivo
├── diretrizes_migracao.txt            # Credenciais de conexão
├── orchestrator_tasks.py              # Orquestrador principal
├── log_execution.txt                  # Log de execução (gerado automaticamente)
│
├── utils/                             # Utilitários
│   ├── database_connection.py         # Gerenciamento de conexões
│   └── test_connections.py            # Teste de conexões
│
├── customers/                         # Migração de clientes
│   ├── customers_to_core.py          # Script de migração
│   ├── customers_dictionary.txt       # Dicionário de mapeamento
│   └── customers_to_core_log.txt     # Log específico (legado)
│
├── stores/                            # Migração de estabelecimentos
│   ├── stores_to_core.py             # Script de migração
│   ├── stores_dictionary.txt         # Dicionário de mapeamento
│   └── create_stores_dictionary.py   # Gerador de dicionário
│
└── clone_tables/                     # Clonagem de tabelas
    ├── clone_tables_from_prd.py      # Script de clonagem
    └── clone_tables.log              # Log de clonagem
```

## 🚀 Uso do Orquestrador

### Comandos Básicos

#### Executar todas as tasks (customers + stores)

```bash
python orchestrator_tasks.py
```

#### Executar com limite de linhas

```bash
python orchestrator_tasks.py --limit 100
```

#### Executar em ambiente específico

```bash
# HML (Homologação)
python orchestrator_tasks.py --destino HML

# PRD (Produção)
python orchestrator_tasks.py --destino PRD
```

#### Executar task específica

```bash
# Apenas customers
python orchestrator_tasks.py customers

# Apenas stores
python orchestrator_tasks.py stores
```

#### Executar etapa específica de uma task

```bash
# Etapa 1 de customers (customer_segments)
python orchestrator_tasks.py customers 1

# Etapa 4 de stores (stores)
python orchestrator_tasks.py stores 4
```

#### Combinar opções

```bash
# Customers, etapa 2, limite 50, destino PRD
python orchestrator_tasks.py customers 2 --limit 50 --destino PRD

# Stores, todas as etapas, limite 1000, destino HML
python orchestrator_tasks.py stores --limit 1000 --destino HML
```

### Parâmetros Disponíveis

| Parâmetro | Descrição | Valores | Padrão |
|-----------|-----------|---------|--------|
| `task` | Task a executar | `customers`, `stores` ou omitir (todas) | Todas |
| `step` | Etapa específica | Número da etapa (1-7) ou omitir (todas) | Todas |
| `--limit` | Limite de linhas | Número > 0 ou omitir (todos) | 0 (todos) |
| `--destino` | Ambiente destino | `HML` ou `PRD` | `PRD` |

## 📊 Exemplos de Execução

### Exemplo 1: Teste rápido em HML (10 linhas)

```bash
python orchestrator_tasks.py --limit 10 --destino HML
```

**Resultado esperado:**
- Migra 10 registros de customers e stores
- Executa em ambiente de homologação
- Gera logs detalhados

### Exemplo 2: Migração completa em PRD

```bash
python orchestrator_tasks.py --destino PRD
```

**Resultado esperado:**
- Migra todos os dados disponíveis
- Executa em ambiente de produção
- Pode levar várias horas dependendo do volume

### Exemplo 3: Migração incremental (1000 linhas)

```bash
python orchestrator_tasks.py --limit 1000 --destino PRD
```

**Resultado esperado:**
- Migra 1000 registros de cada tabela
- Útil para testes ou migrações incrementais
- Tempo de execução reduzido

### Exemplo 4: Apenas customers, etapa 2 (customers)

```bash
python orchestrator_tasks.py customers 2 --limit 50 --destino PRD
```

**Resultado esperado:**
- Executa apenas a etapa 2 (migração de customers)
- Processa 50 registros
- Útil para debug ou correções pontuais

## 📝 Logs e Relatórios

### Arquivo de Log Principal

O arquivo `log_execution.txt` na raiz do projeto contém:
- Data/hora de execução
- Ambiente e schema utilizados
- Limite de linhas configurado
- Progresso de cada etapa
- Erros e avisos
- Relatórios de qualidade por etapa
- Estatísticas finais

### Relatórios de Qualidade

Cada etapa gera um relatório de qualidade comparando:
- Quantidade de registros na origem
- Quantidade de registros no destino
- Diferenças encontradas
- Status (OK ou AVISO)

### Exemplo de Saída no Terminal

```
================================================================================
ETAPA 2: MIGRANDO CUSTOMERS
================================================================================
[ETAPA 2] Limpando tabela customers...
OK - Tabela core.customers truncada
[ETAPA 2] Buscando dados do SQL Server...
[ETAPA 2] Processando chunk 1 (50 registros)...
[ETAPA 2] Chunk 1 processado: 50 registros inseridos, 0 erros

[ETAPA 2] CONCLUIDA! Total de customers migrados: 50

--------------------------------------------------------------------------------
RELATORIO DE QUALIDADE - ETAPA 2: CUSTOMERS
--------------------------------------------------------------------------------
ORIGEM (SQL Server PRD - Cliente):
  Total de registros: 50

DESTINO (PostgreSQL PRD - core.customers):
  Total de registros: 50

OK - Todos os registros foram migrados com sucesso!
```

## 🔍 Troubleshooting

### Erro: "column legacy_id does not exist"

**Causa:** Tentativa de inserir `legacy_id` em ambiente PRD onde a coluna não existe.

**Solução:** O código já trata isso automaticamente. Se o erro persistir, verifique:
- O destino configurado (`--destino PRD`)
- O schema correto está sendo usado (`core` para PRD, `gmcore` para HML)

### Erro: "Retail chain não encontrado"

**Causa:** Alguns `store_brands` referenciam `retail_chains` que não foram migradas (devido ao limite de linhas).

**Solução:** 
- Aumente o limite de linhas: `--limit 1000`
- Ou execute sem limite: `python orchestrator_tasks.py --destino PRD`

### Erro: "Store brand não encontrado"

**Causa:** Alguns `stores` referenciam `store_brands` que não foram migradas.

**Solução:** 
- O sistema cria automaticamente um `store_brand` padrão chamado "Teste store_brands"
- Verifique se a etapa 3 (store_brands) foi executada antes da etapa 4 (stores)

### Erro de Conexão

**Causa:** Credenciais incorretas ou servidor inacessível.

**Solução:**
1. Verifique as credenciais em `diretrizes_migracao.txt`
2. Teste a conexão: `python utils/test_connections.py`
3. Verifique se a VPN está conectada (se necessário)

### Erro: "pyodbc" não encontrado

**Causa:** Biblioteca não instalada.

**Solução:**
```bash
pip install pyodbc
```

No Windows, pode ser necessário instalar o driver ODBC:
- [Download ODBC Driver](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

### Log muito grande

**Causa:** Muitas execuções acumuladas.

**Solução:** O log é limpo automaticamente no início de cada execução do orquestrador. Se necessário, delete manualmente: `log_execution.txt`

## 📋 Etapas de Migração

### Customers (4 etapas)

1. **customer_segments** - Segmentos de produtos
2. **customers** - Clientes
3. **addresses** - Endereços (polimórfico)
4. **contacts** - Contatos (polimórfico)

### Stores (7 etapas)

1. **store_segments** - Segmentos de estabelecimentos
2. **retail_chains** - Redes de varejo
3. **store_brands** - Bandeiras
4. **stores** - Estabelecimentos
5. **store_cnpjs** - CNPJs dos estabelecimentos
6. **addresses** - Endereços (polimórfico)
7. **contacts** - Contatos (polimórfico)

## 🔐 Segurança

⚠️ **IMPORTANTE:**
- Nunca commite credenciais no Git
- Use variáveis de ambiente ou arquivos de configuração locais
- O arquivo `diretrizes_migracao.txt` contém informações sensíveis
- Adicione `diretrizes_migracao.txt` ao `.gitignore` se ainda não estiver

## 📞 Suporte

Para dúvidas ou problemas:
1. Verifique os logs em `log_execution.txt`
2. Consulte a seção [Troubleshooting](#troubleshooting)
3. Revise os dicionários de mapeamento em `customers/` e `stores/`

## 📄 Licença

Este projeto é de uso interno da GM GROUP.

---

**Última atualização:** 2025-11-28
