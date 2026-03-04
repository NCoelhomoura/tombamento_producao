# Clone Tables - Clonagem de Schemas e Tabelas

Este módulo contém scripts para clonar estruturas de schemas e tabelas do PostgreSQL PRD para o PostgreSQL HML.

## Scripts Disponíveis

### 1. `clone_all_schemas.py` ⭐ **RECOMENDADO**
**Script genérico que clona TODOS os schemas e tabelas automaticamente.**

- ✅ Lê todos os schemas do database PRD
- ✅ Cria os schemas correspondentes no HML (com prefixo 'gm')
- ✅ Clona todas as tabelas de cada schema
- ✅ Valida a clonagem ao final

**Uso:**

Modo padrão (criação incremental - preserva dados existentes):
```bash
python clone_tables/clone_all_schemas.py
```

Modo DROP e reconstrução completa (remove tudo antes de recriar):
```bash
python clone_tables/clone_all_schemas.py --drop-existing
```

⚠️ **ATENÇÃO**: O modo `--drop-existing` irá:
- Remover TODOS os schemas clonados do HML (com CASCADE)
- Remover TODAS as tabelas desses schemas
- Recriar tudo do zero
- **ISSO APAGARÁ TODOS OS DADOS EXISTENTES!**

**Mapeamento de Schemas:**
- PRD: `core` → HML: `gmcore`
- PRD: `commercial` → HML: `gmcommercial`
- PRD: `pdv` → HML: `gmpdv`
- PRD: `schema_x` → HML: `gmschema_x`

### 2. `clone_tables_from_prd.py`
**Script específico para clonar apenas o schema `core`.**

- Clona `core` (PRD) → `gmcore` (HML)

**Uso:**
```bash
python clone_tables/clone_tables_from_prd.py
```

### 3. `clone_commercial.py`
**Script específico para clonar apenas o schema `commercial`.**

- Clona `commercial` (PRD) → `gmcommercial` (HML)

**Uso:**
```bash
python clone_tables/clone_commercial.py
```

## Funcionalidades

### O que os scripts fazem:
1. **Listagem de Schemas/Tabelas**: Conecta ao PRD e lista todos os schemas e suas tabelas
2. **Extração de DDL**: Obtém a estrutura completa de cada tabela (colunas, tipos, constraints, índices)
3. **Transformação**: Ajusta referências de schemas (PRD → HML)
4. **Criação**: Cria schemas e tabelas no HML
5. **Validação**: Compara PRD e HML para garantir que tudo foi clonado corretamente

### O que os scripts NÃO fazem:
- ❌ Não migram dados (apenas estrutura)
- ❌ Não criam Foreign Keys automaticamente (serão criadas depois quando todas as tabelas existirem)
- ❌ Não migram views, procedures ou functions

## Requisitos

- Python 3.x
- Bibliotecas: `psycopg2`, `subprocess`
- Acesso ao PostgreSQL PRD e HML configurado em `utils/database_connection.py`
- (Opcional) `pg_dump` instalado para método mais confiável de extração de DDL

## Logs

Os scripts geram logs em:
- `clone_all_schemas.log` - Para o script genérico
- `clone_tables.log` - Para o script de core
- `clone_commercial.txt` - Para o script de commercial

## Exemplo de Saída

```
================================================================================
CLONAGEM DE TODOS OS SCHEMAS E TABELAS: PRD -> HML
================================================================================

ORIGEM (PRD):
  Database: gmcoredb
  Host: prd-host.example.com

DESTINO (HML):
  Database: supera_dev_seed
  Host: hml-host.example.com

================================================================================
ETAPA 1: Listando schemas do PostgreSQL PRD
================================================================================
OK - Encontrados 3 schemas no database 'gmcoredb':
  1. core -> gmcore
  2. commercial -> gmcommercial
  3. pdv -> gmpdv

================================================================================
ETAPA 2: Criando schemas no HML
================================================================================
✅ Schema gmcore criado
✅ Schema gmcommercial criado
✅ Schema gmpdv criado

================================================================================
ETAPA 3: Clonando tabelas para o HML
================================================================================
Schema: core -> gmcore
  📋 Encontradas 15 tabelas
  ✅ Tabela gmcore.customers criada com sucesso!
  ✅ Tabela gmcore.stores criada com sucesso!
  ...

RESUMO
================================================================================
Schemas processados: 3
  - Schemas criados: 3
  - Schemas já existentes: 0

Tabelas processadas: 45
  - Tabelas criadas: 45
  - Tabelas já existentes: 0
  - Erros: 0

✅ Todas as tabelas foram criadas corretamente!
```

## Notas Importantes

1. **Schemas do Sistema**: Schemas como `information_schema`, `pg_catalog` são automaticamente ignorados
2. **Tabelas Existentes**: Se uma tabela já existe no HML, ela será pulada (não será sobrescrita)
3. **Erros**: Se ocorrer erro em uma tabela, o processo continua com as próximas
4. **Performance**: Para muitos schemas/tabelas, o processo pode demorar alguns minutos

## Troubleshooting

### Erro: "pg_dump não encontrado"
- **Solução**: O script usará método alternativo automaticamente. Para melhor precisão, instale `pg_dump` do PostgreSQL.

### Erro: "Schema já existe"
- **Normal**: Se o schema já existe, ele será reutilizado. Apenas tabelas novas serão criadas.

### Erro: "Tabela já existe"
- **Normal**: Tabelas existentes são puladas para evitar sobrescrita.

### Erro de conexão
- Verifique as configurações em `utils/database_connection.py`
- Verifique credenciais e acesso de rede aos databases
