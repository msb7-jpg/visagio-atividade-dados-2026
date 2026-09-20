# Guia de Deploy e Execução no Databricks
## CineData Analytics Lakehouse

Este documento fornece o passo a passo completo e detalhado para implantar, configurar e executar toda a infraestrutura do projeto **CineData Analytics** dentro do ambiente Databricks (AWS, Azure ou GCP).

---

## 1. Visão Geral da Infraestrutura no Databricks

O projeto foi estruturado com compatibilidade nativa para execução no ecossistema Databricks:
* **Armazenamento / Tabelas**: Tabelas gerenciadas **Delta Lake** nos catálogos/esquemas (`bronze`, `silver`, `gold`).
* **Visualização**: Chamadas nativas via `display()` integradas aos DataFrames.
* **Orquestração**: Manifesto **Databricks Workflows** (`job.yaml`) com dependências lineares e cluster compartilhado efêmero.
* **Parâmetros**: Suporte a widgets interativos (`dbutils.widgets`) para ingestão incremental na API BACEN.

---

## 2. Passo a Passo de Migração

### Passo 1: Upload dos Arquivos Brutos (Landing Zone)

Os 5 arquivos CSV originais da camada Landing devem estar acessíveis no ambiente Databricks.

1. No Databricks Workspace, navegue até **Catalog** ➔ escolha seu catálogo e esquema padrão (ou crie um volume no Unity Catalog: `Catalog > Volumes`).
2. Alternativamente, no DBFS raiz: `/FileStore/tables/cinedata/landing/` ou em um bucket externo (S3/ADLS/GCS).
3. Faça o upload dos 5 arquivos CSV:
   * `movies_info_TMDB_IMDB.csv`
   * `movies_financials_IMDB_TMDB.csv`
   * `movies_metrics_IMDB_TMDB.csv`
   * `credits_and_tags_IMDB_TMDB.csv`
   * `movies_reviews.csv`

> **Dica**: No notebook `Landing_to_Bronze.ipynb`, a variável de diretório `DATA_DIR` detecta automaticamente se está rodando no Databricks. Se você salvar os arquivos em `/dbfs/data/landing` ou em um caminho específico (ex: `/Volumes/main/cinedata/landing`), basta ajustar o caminho base na primeira célula do notebook ou no `00_Setup_Ambiente.ipynb`.

---

### Passo 2: Importar o Repositório ou Notebooks no Workspace

Você tem duas opções para subir o código ao Databricks:

#### Opção A: Databricks Repos / Git Folders (Recomendado)
1. No menu lateral do Databricks, clique em **Workspace** ➔ **Repos** (ou **Git Folders**).
2. Clique em **Add Repo** / **Create Git folder**.
3. Insira a URL do repositório Git do projeto e faça o clone.
4. Toda a estrutura de pastas (`notebooks/`, `job.yaml`, documentações) ficará imediatamente sincronizada e versionada.

#### Opção B: Import Manual dos Notebooks
1. No menu **Workspace**, crie uma pasta chamada `/Workspace/CineData_Analytics/`.
2. Clique com botão direito na pasta ➔ **Import**.
3. Selecione e importe os notebooks da pasta `notebooks/`:
   * `00_Setup_Ambiente.ipynb`
   * `Landing_to_Bronze.ipynb`
   * `Bronze_to_Silver.ipynb`
   * `Silver_to_Gold.ipynb`

---

### Passo 3: Criação dos Schemas / Databases

Ao executar o primeiro notebook (`Landing_to_Bronze.ipynb`), os comandos DDL abaixo são executados automaticamente:

```sql
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;
```

Se estiver utilizando **Unity Catalog**, verifique se o catálogo ativo possui as permissões necessárias para criação dos esquemas (`CREATE SCHEMA`).

---

### Passo 4: Configuração e Execução do Workflow (Databricks Jobs)

A orquestração do pipeline completo é definida pelo arquivo `job.yaml`.

#### Criando o Job via Databricks CLI:
Se você possui a Databricks CLI configurada na sua máquina local:

```bash
# Autenticar na CLI (caso ainda não configurada)
databricks configure

# Criar o job no Databricks
databricks jobs create --json-file job.yaml
```

#### Criando o Job via Interface Gráfica (UI):
1. No Databricks, clique em **Workflows** no menu lateral ➔ **Create Job**.
2. Dê o nome: `cinedata_lakehouse_pipeline`.
3. Configure as 3 Tasks em sequência:

| Parâmetro | Task 1 (Bronze) | Task 2 (Silver) | Task 3 (Gold) |
| :--- | :--- | :--- | :--- |
| **Task name** | `Task_Landing_to_Bronze` | `Task_Bronze_to_Silver` | `Task_Silver_to_Gold` |
| **Type** | Notebook | Notebook | Notebook |
| **Source** | Workspace | Workspace | Workspace |
| **Path** | `/Workspace/CineData_Analytics/Landing_to_Bronze` | `/Workspace/CineData_Analytics/Bronze_to_Silver` | `/Workspace/CineData_Analytics/Silver_to_Gold` |
| **Cluster** | Job Cluster dedicado (14.3 LTS) | Mesmo Job Cluster compartilhado | Mesmo Job Cluster compartilhado |
| **Depends on** | *(Nenhuma)* | `Task_Landing_to_Bronze` | `Task_Bronze_to_Silver` |

4. **Cluster do Job**:
   * **Databricks Runtime**: `14.3.x-scala2.12` (ou versão Spark 3.5 LTS mais recente).
   * **Tipo de Nó**: `Standard_DS3_v2` (Azure) / `m5.xlarge` (AWS).
   * **Autoscale**: Mínimo 1 worker, Máximo 4 workers.
5. **Agendamento (Schedule)**:
   * Cron Quartz: `0 0 6 * * ?` (todos os dias às 06:00 BRT).
   * Timezone: `America/Sao_Paulo`.
6. Clique em **Run now** para disparar o pipeline de validação.

---

### Passo 5: Execução Interativa Individual (Opcional)

Para validar ou rodar cada etapa manualmente direto no editor do Databricks:
1. Abra um cluster interativo existente.
2. Abra `Landing_to_Bronze` ➔ Associe ao cluster ➔ Clique em **Run all**.
3. Abra `Bronze_to_Silver` ➔ Clique em **Run all**.
4. Abra `Silver_to_Gold` ➔ Clique em **Run all**.
5. As visualizações ricas das 6 perguntas de negócio e tabelas serão renderizadas via `display()`.

---

## 3. Matriz de Tabelas Gerenciadas Criadas no Metastore

| Camada | Tabela Delta Gerenciada | Modo de Carga | Particionamento |
| :--- | :--- | :--- | :--- |
| **Bronze** | `bronze.tb_movies_info` | `append` | `ano_particao` |
| **Bronze** | `bronze.tb_movies_financials` | `append` | `ano_particao` |
| **Bronze** | `bronze.tb_movies_metrics` | `append` | `ano_particao` |
| **Bronze** | `bronze.tb_movies_reviews` | `append` | `ano_particao` |
| **Bronze** | `bronze.tb_credits_and_tags` | `append` | `ano_particao` |
| **Bronze** | `bronze.tb_cotacao_dolar` | `append` | `ano_particao` |
| **Silver** | `silver.tb_info_filmes` | `overwrite` | `ano_lancamento` |
| **Silver** | `silver.tb_financeiro_filmes` | `overwrite` | — |
| **Silver** | `silver.tb_metricas_engajamento`| `overwrite` | — |
| **Silver** | `silver.tb_avaliacoes_usuarios` | `overwrite` | — |
| **Silver** | `silver.tb_generos` | `overwrite` | — |
| **Silver** | `silver.tb_pessoas_empresas` | `overwrite` | — |
| **Silver** | `silver.tb_cotacao_dolar` | `overwrite` | — |
| **Gold** | `gold.dim_movies` | `overwrite` | `ano_lancamento` |
| **Gold** | `gold.dim_genres` | `overwrite` | — |
| **Gold** | `gold.dim_people` | `overwrite` | — |
| **Gold** | `gold.dim_companies` | `overwrite` | — |
| **Gold** | `gold.dim_reviews` | `overwrite` | — |
| **Gold** | `gold.bridge_movie_genre` | `overwrite` | — |
| **Gold** | `gold.bridge_movie_person` | `overwrite` | — |
| **Gold** | `gold.bridge_movie_company` | `overwrite` | — |
| **Gold** | `gold.fact_movies_performance` | `overwrite` | — |
| **Gold** | `gold.gold_genai_movies_context`| `overwrite` | — |

---

## 4. Dúvidas Frequentes e Troubleshooting

* **Erro de permissão no catálogo**:
  Certifique-se de que o usuário/service principal possui permissões `USE CATALOG`, `USE SCHEMA` e `CREATE TABLE` no catálogo configurado.
* **Caminho da Landing Zone**:
  Caso os arquivos CSV brutos sejam armazenados em um bucket S3/ADLS/GCS externo, configure o ponto de montagem (Mount Point) ou acesse via credencial do Unity Catalog no formato `abfss://...` ou `s3://...`.
* **Bytecode Limit do Spark (Janino)**:
  Totalmente neutralizado pela materialização prévia de `tb_financeiro_filmes` antes das checagens de Data Quality.
