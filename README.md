# 🚀 Sistema de Migração de Dados - ERP para CORE

Sistema de migração de dados do SQL Server PRD para PostgreSQL (HML/PRD), migrando dados de **customers**, **stores**, **users** e **contracts** para o schema `core`/`gmcore`.

---

## 📋 Índice

- [Como Começar](#como-começar)
  - [Clonar o Repositório](#1-clonar-o-repositório)
  - [Instalar Dependências](#2-instalar-dependências)
  - [Configurar Credenciais](#3-configurar-credenciais)
- [Comandos de Migração](#comandos-de-migração)
  - [1. Executar Tudo](#1-executar-tudo)
  - [2. Com Limitador de Linhas](#2-com-limitador-de-linhas)
  - [3. Filtrar por Orçamento](#3-filtrar-por-orçamento)
  - [4. Filtrar por Períodos e Datas](#4-filtrar-por-períodos-e-datas)
  - [5. Executar uma Task Específica](#5-executar-uma-task-específica)
  - [6. Executar uma Etapa Específica](#6-executar-uma-etapa-específica)
  - [7. Usar Flag --clear-data](#7-usar-flag---clear-data)
  - [8. Comandos Compostos](#8-comandos-compostos)
- [Entendendo as Tasks](#entendendo-as-tasks)
- [Troubleshooting](#troubleshooting)
- [Documentação Adicional](#documentação-adicional)

---

## 🎯 Como Começar

### 1. Clonar o Repositório

```bash
git clone <url-do-repositorio>
cd app_migracao_core
```

### 2. Instalar Dependências

**Criar ambiente virtual (recomendado):**

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

**Instalar bibliotecas necessárias:**

```bash
pip install pyodbc psycopg2-binary pandas
```

> **Nota para Windows:** Se encontrar problemas ao instalar `pyodbc`, você pode precisar baixar o driver ODBC para SQL Server:
> - [Microsoft ODBC Driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

### 3. Configurar Credenciais

Edite o arquivo `diretrizes_migracao.txt` e configure as credenciais de acesso aos bancos de dados:

```
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

DESTINO PRD
Server=[seu-host-postgresql-prd]
Port=5432
Database=gmcoredb
User Id=[seu-usuario]
Password=[sua-senha]
```

---

## 🎮 Comandos de Migração

Os comandos seguem uma lógica do **mais geral** para o **mais específico**. Vamos começar pelos mais simples:

### 1. Executar Tudo

Migra **todas as tasks** (customers, stores, users, contracts) com **todos os dados**:

```bash
python orchestrator_tasks.py
```

**O que acontece:**
- ✅ Executa customers (todas as etapas)
- ✅ Executa stores (todas as etapas)
- ✅ Executa users (se necessário)
- ✅ Executa contracts (todas as etapas)
- ✅ Usa ambiente padrão (PRD)
- ✅ Migra todos os dados disponíveis

**Quando usar:** Migração completa inicial ou reset completo do ambiente.

---

### 2. Com Limitador de Linhas

Limita a quantidade de registros migrados (útil para testes):

```bash
# Migrar apenas 10 registros de cada tabela
python orchestrator_tasks.py --limit 10

# Migrar 100 registros em ambiente HML
python orchestrator_tasks.py --limit 100 --destino HML
```

**O que acontece:**
- ✅ Executa todas as tasks
- ✅ Limita a quantidade de registros por tabela
- ⚠️ **Importante:** `users` sempre executa FULL (sem limite) quando automático

**Quando usar:** Testes rápidos, validação de lógica, migrações incrementais.

**Exemplos práticos:**
```bash
# Teste rápido (10 registros)
python orchestrator_tasks.py --limit 10 --destino HML

# Migração incremental (1000 registros)
python orchestrator_tasks.py --limit 1000 --destino PRD
```

---

### 3. Filtrar por Orçamento

Migra apenas dados relacionados a um ou mais orçamentos específicos:

```bash
# Um único orçamento
python orchestrator_tasks.py --id-orcamento 6192

# Múltiplos orçamentos (separados por vírgula)
python orchestrator_tasks.py --id-orcamento 6192,6193,6194
```

**O que acontece:**
- ✅ Executa todas as tasks
- ✅ Filtra apenas dados relacionados aos orçamentos especificados
- ✅ Customers: apenas clientes dos orçamentos
- ✅ Stores: apenas lojas dos orçamentos
- ✅ Contracts: apenas os orçamentos especificados

**Quando usar:** Migração de um contrato específico, testes com dados reais de produção.

**Exemplo prático:**
```bash
# Migrar tudo relacionado ao orçamento 6192
python orchestrator_tasks.py --id-orcamento 6192 --destino HML
```

---

### 4. Filtrar por Períodos e Datas

Filtra contratos por datas de aviso prévio e início de operação:

```bash
# Filtrar por data de aviso prévio (mínima)
python orchestrator_tasks.py --data-aviso-previo 2025-01-01

# Filtrar por data de início de operação (máxima)
python orchestrator_tasks.py --data-inicio-operacao 2026-01-13

# Combinar ambos os filtros
python orchestrator_tasks.py --data-aviso-previo 2025-01-01 --data-inicio-operacao 2026-01-13
```

**O que acontece:**
- ✅ Filtra contratos onde `DataAvisoPrevio >= data especificada` OU `DataAvisoPrevio IS NULL`
- ✅ Filtra contratos onde `DataInicioOperacao <= data especificada`
- ✅ Migra apenas customers, stores e contracts relacionados aos contratos filtrados

**Quando usar:** Migração de contratos em períodos específicos, relatórios por período.

**Exemplo prático:**
```bash
# Migrar contratos com aviso prévio a partir de 01/01/2025 e início até 13/01/2026
python orchestrator_tasks.py --data-aviso-previo 2025-01-01 --data-inicio-operacao 2026-01-13
```

---

### 5. Executar uma Task Específica

Executa apenas uma task (customers, stores, users ou contracts):

```bash
# Apenas customers
python orchestrator_tasks.py customers

# Apenas stores
python orchestrator_tasks.py stores

# Apenas users
python orchestrator_tasks.py users

# Apenas contracts
python orchestrator_tasks.py contracts
```

**O que acontece:**
- ✅ Executa apenas a task especificada (todas as etapas)
- ✅ Se for `contracts`, executa automaticamente as dependências (customers, stores, users) se necessário

**Quando usar:** Atualização de uma área específica, correções pontuais.

**Exemplos práticos:**
```bash
# Atualizar apenas customers
python orchestrator_tasks.py customers --destino HML

# Atualizar apenas stores com limite
python orchestrator_tasks.py stores --limit 100
```

---

### 6. Executar uma Etapa Específica

Executa apenas uma etapa dentro de uma task:

```bash
# Etapa 1 de customers (customer_segments)
python orchestrator_tasks.py customers 1

# Etapa 2 de customers (customers)
python orchestrator_tasks.py customers 2

# Etapa 4 de stores (stores)
python orchestrator_tasks.py stores 4

# Etapa 1 de contracts (contracts)
python orchestrator_tasks.py contracts 1
```

**Etapas disponíveis:**

**Customers (4 etapas):**
- `1` - customer_segments
- `2` - customers
- `3` - addresses
- `4` - contacts

**Stores (7 etapas):**
- `1` - store_segments
- `2` - retail_chains
- `3` - store_brands
- `4` - stores
- `5` - store_cnpjs
- `6` - addresses
- `7` - contacts

**Contracts (8 etapas):**
- `1` - contracts
- `2` - contract_scenarios
- `3` - contract_scenario_stores
- `4` - contract_sellers
- `5` - contract_team_members
- `6` - contract_contacts
- `7` - contract_partners
- `8` - contract_additional_charges

**Users (1 etapa):**
- `1` - users

**Quando usar:** Debug, correção de uma etapa específica, validação pontual.

**Exemplo prático:**
```bash
# Corrigir apenas a etapa de customers
python orchestrator_tasks.py customers 2 --destino HML
```

---

### 7. Usar Flag --clear-data

**⚠️ ATENÇÃO:** Limpa completamente as tabelas antes de migrar (TRUNCATE).

```bash
# Limpar tudo e migrar novamente
python orchestrator_tasks.py --clear-data

# Limpar e migrar um orçamento específico
python orchestrator_tasks.py --id-orcamento 6192 --clear-data

# Limpar e migrar com filtros de data
python orchestrator_tasks.py --data-aviso-previo 2025-01-01 --data-inicio-operacao 2026-01-13 --clear-data
```

**O que acontece:**
- ✅ **TRUNCATE** nas tabelas do escopo executado (limpa tudo)
- ✅ Migra apenas os dados novos especificados
- ⚠️ **Importante:** `users` não é afetado por `--clear-data` de outras tasks

**Quando usar:**
- ✅ Reset completo antes de nova migração
- ✅ Testes em ambiente de desenvolvimento
- ✅ Garantir que não há dados antigos misturados

**Quando NÃO usar:**
- ❌ Migração incremental com filtros (use DELETE automático)
- ❌ Em produção sem backup adequado
- ❌ Quando outros steps já foram executados anteriormente

**Exemplo prático:**
```bash
# Reset completo e migrar orçamento 6192
python orchestrator_tasks.py --id-orcamento 6192 --data-aviso-previo 2025-01-01 --data-inicio-operacao 2026-01-13 --clear-data
```

---

### 8. Comandos Compostos

Combine múltiplos parâmetros para casos específicos:

```bash
# Orçamento específico + filtros de data + clear-data + ambiente HML
python orchestrator_tasks.py --id-orcamento 6192 --data-aviso-previo 2025-01-01 --data-inicio-operacao 2026-01-13 --clear-data --destino HML

# Apenas customers, etapa 2, com limite, em HML
python orchestrator_tasks.py customers 2 --limit 50 --destino HML

# Apenas stores, etapa 4, com limite, em PRD
python orchestrator_tasks.py stores 4 --limit 100 --destino PRD

# Contracts com orçamento específico e clear-data
python orchestrator_tasks.py contracts --id-orcamento 6192 --clear-data
```

**Ordem dos parâmetros:**
1. Task (opcional): `customers`, `stores`, `users`, `contracts`
2. Step (opcional): número da etapa
3. Flags: `--limit`, `--destino`, `--id-orcamento`, `--data-aviso-previo`, `--data-inicio-operacao`, `--clear-data`

**Exemplos práticos avançados:**

```bash
# Teste completo de um orçamento em HML
python orchestrator_tasks.py --id-orcamento 6192 --destino HML --limit 10

# Migração completa de um período em PRD
python orchestrator_tasks.py --data-aviso-previo 2025-01-01 --data-inicio-operacao 2026-01-13 --destino PRD

# Reset e migração completa em HML
python orchestrator_tasks.py --clear-data --destino HML

# Atualizar apenas customers de um orçamento específico
python orchestrator_tasks.py customers --id-orcamento 6192
```

---

## 📚 Entendendo as Tasks

### Customers (4 etapas)

Migra dados de clientes e informações relacionadas.

**Ordem de execução:**
1. `customer_segments` - Segmentos de produtos (base)
2. `customers` - Clientes principais ⚠️ **OBRIGATÓRIO antes de contracts**
3. `addresses` - Endereços dos clientes
4. `contacts` - Contatos dos clientes

**Dependências:** Nenhuma (pode ser executado primeiro)

**Filtros:** Sempre filtra pela `ViewOrcamentosLojas` (apenas clientes ativos relacionados a contratos)

---

### Stores (7 etapas)

Migra dados de estabelecimentos (lojas) e informações relacionadas.

**Ordem de execução:**
1. `store_segments` - Segmentos de estabelecimentos (base)
2. `retail_chains` - Redes de varejo
3. `store_brands` - Bandeiras
4. `stores` - Estabelecimentos principais ⚠️ **OBRIGATÓRIO antes de contracts**
5. `store_cnpjs` - CNPJs dos estabelecimentos
6. `addresses` - Endereços das lojas
7. `contacts` - Contatos das lojas

**Dependências:** Nenhuma (pode ser executado primeiro)

**Filtros:** Sempre filtra pela `ViewOrcamentosLojas` (apenas lojas ativas relacionadas a contratos)

---

### Users (1 etapa)

Migra dados de usuários do sistema.

**Ordem de execução:**
1. `users` - Usuários

**Dependências:** Nenhuma

**Comportamento especial:**
- ⚠️ Quando executado automaticamente (como dependência de contracts), **sempre executa FULL** (sem limite)
- ✅ Quando executado diretamente, pode usar `--limit`

---

### Contracts (8 etapas)

Migra dados de contratos e informações relacionadas.

**Ordem de execução:**
1. `contracts` - Contratos principais
2. `contract_scenarios` - Cenários de contratos
3. `contract_scenario_stores` - Lojas por cenário
4. `contract_sellers` - Vendedores
5. `contract_team_members` - Membros da equipe
6. `contract_contacts` - Contatos
7. `contract_partners` - Parceiros
8. `contract_additional_charges` - Cargas adicionais

**Dependências:** 
- ⚠️ **OBRIGATÓRIO:** Customers e Stores devem ser executados antes
- ⚠️ **OBRIGATÓRIO:** Users deve estar disponível (executado automaticamente se necessário)

**Execução automática:**
Quando você executa `contracts`, o sistema automaticamente:
1. Verifica se customers foi executado → Se não, executa customers steps 1 e 2
2. Verifica se stores foi executado → Se não, executa stores steps 1, 2, 3 e 4
3. Verifica se users foi executado → Se não, executa users FULL
4. Executa contracts com os filtros especificados

---

## 🔍 Troubleshooting

### Erro: "column legacy_id does not exist"

**Causa:** Tentativa de inserir `legacy_id` em ambiente PRD onde a coluna não existe.

**Solução:** O código já trata isso automaticamente. Verifique se o destino está correto (`--destino PRD`).

---

### Erro: "Customer não encontrado" ou "Store não encontrado"

**Causa:** Contracts está tentando referenciar customers/stores que não foram migrados.

**Solução:** 
- Execute customers e stores antes de contracts
- Ou deixe o sistema executar automaticamente as dependências

---

### Erro: Tabela não está sendo limpa com --clear-data

**Causa:** O flag `--clear-data` pode não estar sendo passado corretamente.

**Solução:** Verifique se está usando `--clear-data` (com hífen) e não `--clear_data` (com underscore).

---

### Erro de Conexão

**Causa:** Credenciais incorretas ou servidor inacessível.

**Solução:**
1. Verifique as credenciais em `diretrizes_migracao.txt`
2. Teste a conexão: `python utils/test_connections.py`
3. Verifique se a VPN está conectada (se necessário)

---

### Log mostra DELETE mas deveria ser TRUNCATE

**Causa:** O flag `--clear-data` não está sendo passado ou não está sendo aplicado.

**Solução:** 
- Verifique se está usando `--clear-data` no comando
- Verifique se está executando todas as tasks (não apenas uma task específica)
- Verifique os logs para confirmar se o flag foi reconhecido

---

## 📖 Documentação Adicional

### Arquivos de Dicionário

Documentação detalhada sobre mapeamento de campos e transformações:

- **`customers/customers_dictionary.txt`** - Mapeamento de campos de customers
- **`stores/stores_dictionary.txt`** - Mapeamento de campos de stores
- **`contracts/contract_dictionary.txt`** - Mapeamento de campos de contracts
- **`users/users_dictionary.txt`** - Mapeamento de campos de users

**Quando consultar:** Para entender como os dados são transformados durante a migração.

---

### Arquivo de Regras

**`rules.txt`** - Regras completas de negócio e migração

Contém:
- Regras de limpeza de dados (TRUNCATE vs DELETE)
- Regras específicas por etapa
- Lógicas de execução e orquestração
- Comportamentos especiais (filtros, limites, dependências)
- Exemplos de queries SQL

**Quando consultar:** Para entender profundamente como o sistema funciona, regras de negócio, e comportamento em casos especiais.

---

### Outros Documentos

- **`ORDEM_DEPENDENCIAS.md`** - Documentação sobre ordem de execução e dependências entre tasks
- **`ROTEIRO_PADRONIZACAO_MIGRACAO.txt`** - Roteiro de padronização da migração

---

## 📊 Estrutura do Projeto

```
app_migracao_core/
│
├── README.md                          # Este arquivo
├── orchestrator_tasks.py              # Orquestrador principal
├── rules.txt                          # Regras de migração
├── diretrizes_migracao.txt            # Credenciais de conexão
├── log_execution.txt                  # Log de execução (gerado automaticamente)
│
├── utils/                             # Utilitários
│   ├── database_connection.py        # Gerenciamento de conexões
│   └── test_connections.py            # Teste de conexões
│
├── customers/                          # Migração de clientes
│   ├── customers_to_core.py          # Script de migração
│   └── customers_dictionary.txt       # Dicionário de mapeamento
│
├── stores/                            # Migração de estabelecimentos
│   ├── stores_to_core.py             # Script de migração
│   └── stores_dictionary.txt         # Dicionário de mapeamento
│
├── contracts/                         # Migração de contratos
│   ├── contracts_to_core.py          # Script de migração
│   ├── contract_dictionary.txt       # Dicionário de mapeamento
│   └── contracts_filter_main.json    # Arquivo JSON de filtros (gerado automaticamente)
│
└── users/                             # Migração de usuários
    ├── users_to_core.py              # Script de migração
    └── users_dictionary.txt          # Dicionário de mapeamento
```

---

## 🔐 Segurança

⚠️ **IMPORTANTE:**
- Nunca commite credenciais no Git
- Use variáveis de ambiente ou arquivos de configuração locais
- O arquivo `diretrizes_migracao.txt` contém informações sensíveis
- Adicione `diretrizes_migracao.txt` ao `.gitignore` se ainda não estiver

---

## 📞 Suporte

Para dúvidas ou problemas:
1. Verifique os logs em `log_execution.txt`
2. Consulte a seção [Troubleshooting](#troubleshooting)
3. Revise os dicionários de mapeamento em cada módulo
4. Consulte `rules.txt` para regras detalhadas

---

## 📄 Licença

Este projeto é de uso interno da GM GROUP.

---

**Última atualização:** 2026-01-14
