# Proposta: Estrutura DAG Similar ao Airflow

## 📋 Objetivo

Implementar uma estrutura de definição de dependências similar ao Airflow DAG para facilitar a manutenção e compreensão do orchestrator.

## 🎯 Motivação

Atualmente, as dependências entre tasks são definidas de forma imperativa no código (`orchestrator_tasks.py`), o que torna difícil:
- Visualizar o fluxo de execução
- Manter e modificar dependências
- Entender a ordem de execução
- Reutilizar tasks em diferentes contextos

## 💡 Proposta

Criar um sistema declarativo onde as dependências são definidas usando operadores, similar ao Airflow:

```python
# Exemplo de uso desejado
stores_1 >> [customers_1 >> customers_2]
```

Isso significa:
- `stores_1` executa primeiro
- Depois `customers_1` executa
- Depois `customers_2` executa (depende de `customers_1`)

## 🔧 Como Funciona Tecnicamente

### Sobreposição de Operadores Python

O Airflow usa sobrecarga de operadores (`__rshift__` e `__lshift__`) para criar um DSL:

```python
class Task:
    def __init__(self, name, func, *args, **kwargs):
        self.name = name
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.dependencies = []
        self.dependents = []
    
    def __rshift__(self, other):
        """Define que 'other' depende de 'self'"""
        if isinstance(other, list):
            for task in other:
                task.dependencies.append(self)
                self.dependents.append(task)
        else:
            other.dependencies.append(self)
            self.dependents.append(other)
        return other
    
    def execute(self):
        # Executa dependências primeiro
        for dep in self.dependencies:
            dep.execute()
        # Depois executa a própria task
        return self.func(*self.args, **self.kwargs)
```

## 📝 Exemplo de Implementação Futura

### Estrutura Proposta

```python
# orchestrator_dag.py
from tasks import Task

# Definir todas as tasks
customers_1 = Task("customer_segments", run_customers_task, step='1')
customers_2 = Task("customers", run_customers_task, step='2')
customers_3 = Task("customers_addresses", run_customers_task, step='3')
customers_4 = Task("customers_contacts", run_customers_task, step='4')

stores_1 = Task("store_segments", run_stores_task, step='1')
stores_2 = Task("retail_chains", run_stores_task, step='2')
stores_3 = Task("store_brands", run_stores_task, step='3')
stores_4 = Task("stores", run_stores_task, step='4')

contracts_1 = Task("contracts", run_contracts_task, step='1')
contracts_2 = Task("contract_scenarios", run_contracts_task, step='2')
contracts_3 = Task("contract_scenario_stores", run_contracts_task, step='3')

users_1 = Task("users", run_users_task, step='1')

# Definir DAG de dependências
# Customers
customers_1 >> customers_2 >> [customers_3, customers_4]

# Stores
stores_1 >> stores_2 >> stores_3 >> stores_4

# Contracts depende de Customers, Stores e Users
[customers_2, stores_4, users_1] >> contracts_1
contracts_1 >> contracts_2 >> contracts_3

# Executar (resolve dependências automaticamente)
contracts_3.execute()
```

### Executor com Topological Sort

```python
class DAGExecutor:
    def __init__(self, root_task):
        self.root_task = root_task
        self.executed = set()
    
    def execute(self):
        """Executa tasks em ordem topológica"""
        tasks_to_execute = self._get_execution_order(self.root_task)
        for task in tasks_to_execute:
            if task not in self.executed:
                task.execute()
                self.executed.add(task)
    
    def _get_execution_order(self, task):
        """Retorna ordem topológica de execução"""
        # Implementar topological sort
        pass
```

## ✅ Benefícios

1. **Declarativo**: Dependências ficam claras e fáceis de entender
2. **Manutenível**: Fácil adicionar/remover dependências
3. **Visual**: Similar ao Airflow, facilitando onboarding
4. **Reutilizável**: Tasks podem ser compostas em diferentes DAGs
5. **Testável**: Fácil criar DAGs diferentes para testes
6. **Documentação**: O próprio código serve como documentação visual

## 🔄 Integração com Código Atual

- As funções `run_*_task` continuam funcionando normalmente
- As classes `Task` apenas encapsulam essas funções
- O executor resolve o grafo de dependências automaticamente
- Filtros e parâmetros podem ser passados via `**kwargs`

## 📊 Comparação: Antes vs Depois

### Antes (Imperativo)

```python
# orchestrator_tasks.py
if task == 'contracts':
    # Executar dependências manualmente
    run_customers_task(step='1')
    run_customers_task(step='2')
    run_stores_task(step='1')
    run_stores_task(step='2')
    run_stores_task(step='3')
    run_stores_task(step='4')
    run_users_task()
    run_contracts_task(...)
```

### Depois (Declarativo)

```python
# orchestrator_dag.py
customers_1 >> customers_2
stores_1 >> stores_2 >> stores_3 >> stores_4
[customers_2, stores_4, users_1] >> contracts_1

# Executar
contracts_1.execute()  # Resolve dependências automaticamente
```

## 🚧 Considerações para Implementação

### 1. Ordem de Execução
- Implementar topological sort para resolver ordem correta
- Garantir que dependências sejam executadas antes das tasks dependentes

### 2. Paralelização
- Tasks sem dependências entre si podem executar em paralelo
- Considerar usar `concurrent.futures` para paralelização opcional

### 3. Tratamento de Erros
- Definir comportamento quando uma task falha:
  - Parar todas as tasks dependentes?
  - Continuar com outras tasks independentes?
  - Retry automático?

### 4. Filtros e Parâmetros
- Passar filtros entre tasks (ex: `contracts_filter_main.json`)
- Suportar parâmetros dinâmicos (`--id-orcamento`, `--clear-data`, etc.)

### 5. Validação
- Validar que não há ciclos no grafo de dependências
- Verificar que todas as dependências são válidas

### 6. Logging
- Log claro de qual task está executando
- Log de dependências sendo resolvidas
- Log de ordem de execução final

## 📅 Quando Implementar

**Status**: Proposta documentada, aguardando finalização dos ajustes atuais

**Pré-requisitos**:
- ✅ Finalizar ajustes de validação e filtros
- ✅ Finalizar documentação de dependências (`ORDEM_DEPENDENCIAS.md`)
- ✅ Testar execução automática de dependências atual
- ⏳ Implementar estrutura DAG (futuro)

## 📚 Referências

- [Airflow DAGs Documentation](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html)
- [Python Operator Overloading](https://docs.python.org/3/reference/datamodel.html#emulating-numeric-types)
- [Topological Sort Algorithm](https://en.wikipedia.org/wiki/Topological_sorting)

---

**Criado em**: 2026-01-13  
**Última atualização**: 2026-01-13  
**Status**: Proposta documentada, aguardando implementação
