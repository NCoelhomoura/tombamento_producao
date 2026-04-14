# Regra de deduplicação: CNPJ e IdCliente (customers)

Documento de referência para implementação e revisões. Alinhado à fase **A** em `PONTOS_A_FECHAR.json` (`ordem_implementacao`).

**Objetivo:** um registro de cliente **canônico** por CNPJ (no escopo da migração), com atributos mesclados e rastreabilidade de **todos** os `IdCliente` do grupo em `customers.legacy_ids` (objeto JSON).

---

## 1. Princípios

1. **Escopo fechado:** só entram na deduplicação os `IdCliente` já filtrados pelo **mesmo critério da migração** (JSON de filtros, `ViewOrcamentosLojas`, ou combinação definida no orquestrador). Evita fundir clientes que não participam do lote atual.
2. **Chave de duplicidade:** CNPJ **normalizado** (apenas dígitos, 14 posições). Tratar explicitamente NULL, branco ou CNPJ inválido (ver secção 5).
3. **Um canônico por grupo:** por `cnpj_normalizado` no escopo, exatamente uma linha “vencedora” que será a base do insert/update no destino.
4. **Rastreabilidade:** todos os `IdCliente` do grupo aparecem em `legacy_ids` (formato de objeto acordado no dicionário de customers).

---

## 2. Processo recomendado (camadas)

Ordem lógica; pode ser **uma query com CTEs**, **poucos statements** ou **staging** (tabela temp) se volume ou legibilidade exigirem.

| Etapa | O que fazer |
|--------|-------------|
| **2.1 Base** | Selecionar clientes do escopo com colunas necessárias à migração + CNPJ. |
| **2.2 Normalização** | Coluna derivada `cnpj_normalizado` (14 dígitos). |
| **2.3 Grupos com mais de um Id** | Identificar CNPJs que aparecem mais de uma vez **no escopo** (`COUNT(*) > 1` por `cnpj_normalizado`). Incluir também CNPJ único (grupo de 1 linha): canônico = o próprio; `legacy_ids` pode ser um único id ou lista com um elemento — **padronizar**. |
| **2.4 Escolha do canônico** | **Fechado (CUST-001):** por `cnpj_normalizado`, o canônico é sempre o **menor `IdCliente`** (inteiro) do grupo — o mais antigo na numeração do ERP. `ORDER BY IdCliente ASC` com `ROW_NUMBER() ... = 1` (ou equivalente). |
| **2.5 Merge de atributos** | **Fechado (CUST-002):** preencher lacunas do canônico com valores não nulos dos demais `IdCliente` do grupo. **Em conflito** (dois valores distintos no mesmo campo): prevalece o valor do registro **canônico** (menor `IdCliente`); depois complementar com não nulos dos outros onde o canônico estiver vazio. |
| **2.6 Construção de `legacy_ids`** | Objeto JSON com **todos** os `IdCliente` do grupo; opcionalmente marcar qual é `canonical` e quais são `duplicate` / ordem, se o modelo JSON permitir. |
| **2.7 Saída** | Uma linha final por CNPJ (no escopo) pronta para gravar no PostgreSQL + mapa auxiliar `IdCliente_origem → IdCliente_canônico` (ou para UUID destino) para uso em contracts, billings, etc. |

---

## 3. Conflitos e exceções

| Situação | Tratamento sugerido |
|----------|---------------------|
| Mesmo campo **preenchido com valores diferentes** em dois IDs | Prioridade: valor do **canônico** (menor `IdCliente`); ver **2.5**. Opcional: log de divergências para revisão. |
| CNPJ inválido / ausente | Não agrupar por CNPJ; tratar como **1:1** sem merge ou regra separada (ex.: deduplicar por outra chave). Documentar no dicionário. |
| Pessoa física (CPF) ou estrangeiro | Fora da deduplicação por CNPJ de 14 dígitos, salvo regra de negócio explícita. |

---

## 4. Integração com o restante da migração

- **Contratos e demais módulos** que ainda referenciam `IdCliente` “antigo” precisam usar o **mapa** `IdCliente_antigo → identidade canônica (destino)** após esta etapa.
- **Billings / legacy_customer_id / Title:** seguir a mesma definição de “principal” e `legacy_ids` já descrita em `PONTOS_A_FECHAR.json` (`escopo_requisitos`).

---

## 5. Performance e alternativas

- **Volume alto ou query ilegível:** materializar resultados intermediários em **tabela temporária** ou staging (ainda poucas passagens pelo banco).
- **Regras muito diferentes por coluna ou auditoria linha a linha:** exportar grupos + ids para **pandas/Polars** no Python, aplicar merge e gerar o mesmo artefato final (uma linha por CNPJ + JSON + mapa). Útil quando a lógica deixar de ser só `COALESCE`.

---

## 6. Validação antes de subir destino

- Contagem: grupos CNPJ × linhas mescladas bate com origem.
- Amostra: para 5–10 CNPJs duplicados, conferir manualmente merge e JSON.
- Nenhum `IdCliente` do escopo fica de fora de `legacy_ids` ou do mapa.

---

**Última atualização:** 2026-03-30  
**Relacionado:** `docs/migracao/PONTOS_A_FECHAR.json` (fase A, itens CUST-001, CUST-002, CUST-003), `customers/customers_dictionary.txt`.
