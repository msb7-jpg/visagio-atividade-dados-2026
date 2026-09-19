# Registro de Decisões de Arquitetura e Engenharia de Dados (ADR)
## Projeto CineData Analytics — Pipeline Lakehouse Databricks

Este documento consolida as decisões arquiteturais, justificativas de modelagem, regras de negócio e contratos de dados aplicados ao longo do pipeline da camada Silver.

---

## 1. silver.tb_info_filmes (Origem: `bronze.tb_movies_info`)

### Decisão 1.1: Descarte da coluna `tconst` (IMDb ID)
* **Contexto**: A tabela bruta de filmes contém o identificador alfanumérico do IMDb (`tconst`) além do identificador numérico `id` (TMDB).
* **Decisão**: A coluna `tconst` foi **descartada** na camada Silver.
* **Justificativa**: 
  - O identificador de relacionamento entre todas as tabelas do Lakehouse (`tb_movies_financials`, `tb_movies_metrics`, `tb_credits_and_tags`, `tb_movies_reviews`) é unicamente o `id` numérico.
  - Como `tconst` não faz parte do contrato de dados da `silver.tb_info_filmes` nem das dimensões da camada Gold (`dim_movies`), seu descarte otimiza o armazenamento e reduz o tráfego de rede e memória nas camadas a jusante (*downstream*).

### Decisão 1.2: Normalização e Tradução Declarativa de Status com Fallback
* **Contexto**: Registros de status contêm variações de caixa, hífens redundantes (`"Post-Production"`, `"in-production"`) e valores desconhecidos.
* **Decisão**: Aplicou-se normalização por regex e mapeamento `create_map` com fallback explícito para `"Não Informado"`.

---

## 2. silver.tb_financeiro_filmes (Origem: `bronze.tb_movies_financials`)

### Decisão 2.1: Chave Primária, Grão da Tabela e Deduplicação
* **Contexto**: A tabela Bronze acumulou 636.990 registros devido a execuções sucessivas de ingestão no modo `append`, enquanto a base original possuía 106.165 linhas e 99.006 filmes distintos.
* **Decisão**: A chave primária (natural) da tabela é unicamente o **`id`** (`id_filme`), definindo o grão como **1 registro único por filme**. A deduplicação é realizada selecionando exclusivamente a versão mais recente com base na coluna `ingestion_datetime` (`Window.partitionBy("id").orderBy(col("ingestion_datetime").desc())`).
* **Justificativa**:
  - Garante a integridade e unicidade do catálogo para a camada analítica (`fact_movies_performance`), impedindo que métricas financeiras (soma de receita/orçamento) sejam multiplicadas ou distorcidas por duplicatas brutas ou re-execuções de ingestão.
  - Alinha-se ao princípio da arquitetura Medalhão: a Bronze armazena histórico bruto imutável (*append-only*), enquanto a Silver consolida o estado atualizado (*SCD Type 1 / Latest record*).

### Decisão 2.2: Higienização de Métricas Monetárias e Notações de Escala
* **Contexto**: As colunas `budget` e `revenue` contêm ruídos heterogêneos: textos sentinela (`"Unknown"`, `"Não Informado"`), símbolos monetários (`$`, `USD`), espaços, notações abreviadas de escala (`10.0M`, `18.0K`), valores zerados e valores negativos (ex: `-800526015`).
* **Decisão**:
  1. **Sentinelas $\rightarrow$ `NULL`**: Qualquer valor textual que indique ausência de dado é convertido para `NULL` antes de cálculos.
  2. **Sanitização de Símbolos**: Remoção de `$`, `USD` e espaços extras via expressões regulares.
  3. **Multiplicadores de Escala**: Reconhecimento de sufixos `K` ($\times 10^3$), `M` ($\times 10^6$) e `B` ($\times 10^9$) multiplicando o valor numérico correspondente.
  4. **Valores $\le 0 \rightarrow$ `NULL`**: Valores zerados ou negativos são invalidados para `NULL` conforme especificado no contrato de dados.
  5. **Tipagem e Precisão**: Conversão final para `DECIMAL(18, 2)` garantindo precisão contábil.
  6. **Cálculos Seguros de Lucro e Margem**: Proteção explícita contra divisão por zero e propagação controlada de nulos no cálculo da `margem_lucro_percentual`.

---

## 3. silver.tb_metricas_engajamento (Origem: `bronze.tb_movies_metrics`)

### Decisão 3.1: Safe Casting contra Column Shift e Validação de Escala
* **Contexto**: A base bruta apresenta desalinhamento de colunas (*column shift*), espalhando trechos de sinopses e nomes em colunas numéricas de notas e votos, além de notas com separador decimal em vírgula (`154,34`) e valores fora da escala de 0 a 10 (ex.: `79.97`).
* **Decisão**:
  1. Substituição prévia de vírgula por ponto.
  2. Uso de `try_cast` para evitar a quebra do job Spark diante de strings literais inválidas.
  3. Aplicação de filtros de domínio de negócio: `nota_media_tmdb` e `nota_media_imdb` restritas ao intervalo $[0.0, 10.0]$; `popularidade`, `qtd_votos_tmdb` e `qtd_votos_imdb` restritas a valores $\ge 0$. Valores anômalos são invalidados para `NULL`.

---

## 4. silver.tb_avaliacoes_usuarios (Origem: `bronze.tb_movies_reviews`)

### Decisão 4.1: Deduplicação Integral e Normalização de Avaliações
* **Contexto**: A tabela de avaliações possui 194.472 registros brutos com elevado volume de duplicatas idênticas decorrentes de execuções de *append*.
* **Decisão**:
  1. **Deduplicação Integral**: Aplicação de `dropDuplicates(["id", "nome", "nota", "comentario"])`, preservando 32.412 avaliações únicas de usuários.
  2. **Validação de Nota**: Notas fora do intervalo $[0.0, 10.0]$ convertidas para `NULL`.
  3. **Comentários Faltantes**: Comentários vazios ou `NULL` recebem o texto padronizado `"Sem comentário"`.

---

## 5. silver.tb_generos (Origem: `bronze.tb_credits_and_tags`)

### Decisão 5.1: Desmembramento Atômico e Filtragem por Domínio Canônico
* **Contexto**: A coluna `genres` armazena listas delimitadas por múltiplos separadores (`,`, `;`, `|`), contendo ainda resíduos de deslocamento de colunas (URLs de imagens `.jpg`, números decimais `0.6`, países e fragmentos de texto).
* **Decisão**:
  1. **Unificação de Separadores e Split**: Substituição de `[,;|]+` por vírgula e aplicação de `explode(split(...))`.
  2. **Filtragem pelo Domínio Canônico de Gêneros TMDB**: Restrição estrita aos 19 gêneros oficiais (`Action`, `Adventure`, `Animation`, `Comedy`, `Crime`, `Documentary`, `Drama`, `Family`, `Fantasy`, `History`, `Horror`, `Music`, `Mystery`, `Romance`, `Science Fiction`, `TV Movie`, `Thriller`, `War`, `Western`).
  3. **Deduplicação de Relacionamento**: Aplicação de `dropDuplicates(["id_filme", "nome_genero"])`.

---

## 6. silver.tb_pessoas_empresas (Origem: `bronze.tb_credits_and_tags`)

### Decisão 6.1: Consolidação em Dimensão Unificada
* **Contexto**: Os participantes técnicos e artísticos da obra estão distribuídos em 4 colunas distintas (`cast`, `directors`, `writers`, `production_companies`) com formatos heterogêneos.
* **Decisão**:
  1. **Mapeamento de Entidades**:
     - `cast` $\rightarrow$ `'Ator'`
     - `directors` $\rightarrow$ `'Diretor'`
     - `writers` $\rightarrow$ `'Roteirista'`
     - `production_companies` $\rightarrow$ `'Produtora'`
  2. **Higienização Textual**: Remoção de aspas, colchetes `[]` e aplicação de `initcap` para capitalização padronizada.
  3. **Expurgo de Resíduos de Column Shift**: Exclusão de extensões de imagem (`.jpg`, `.png`), URLs (`http`, `www`), valores numéricos puros e sentinelas (`N/A`, `Unknown`).
  4. **Unificação e Deduplicação**: União via `unionByName` e deduplicação pelo trio `(id_filme, nome_entidade, tipo_entidade)`.

---

## 7. silver.tb_cotacao_dolar (Origem: `bronze.tb_cotacao_dolar`)

### Decisão 7.1: Série Temporal Contínua e Forward Fill
* **Contexto**: A API do Banco Central (PTAX) fornece dados apenas em dias úteis, gerando descontinuidade em finais de semana e feriados.
* **Decisão**:
  1. **Deduplicação Diária**: Seleção da última cotação registrada por dia útil.
  2. **Geração de Calendário Diário**: Criação de intervalo contínuo com `sequence(min_date, max_date, interval 1 day)`.
  3. **Forward Fill**: Propagação da última cotação válida (`last(cotacao_compra_bruta, ignorenulls=True).over(Window.orderBy("data_cotacao").rowsBetween(Window.unboundedPreceding, Window.currentRow))`) para cobrir todas as datas da série temporal contínua.
  4. **Precisão Numérica**: `DECIMAL(18, 4)` para taxa cambial.

---

## 8. Modelagem Dimensional da Camada Gold (Star Schema)

### Decisão 8.1: Chaves Substitutas (Surrogate Keys) Determinísticas
* **Contexto**: Relacionar dimensões e fatos através de identificadores técnicos independentes das chaves naturais de negócio.
* **Decisão**:
  - Geração de chaves `BIGINT` (`sk_movie_id`, `sk_genre_id`, `sk_person_id`, `sk_company_id`, `sk_review_id`) via `row_number().over(Window.orderBy(...))`.

### Decisão 8.2: Desacoplamento N:N com Tabelas-Ponte (Bridge Tables)
* **Contexto**: Filmes possuem múltiplos gêneros, múltiplos participantes artísticos/técnicos e múltiplas produtoras (e vice-versa).
* **Decisão**:
  - Criação de `gold.bridge_movie_genre`, `gold.bridge_movie_person` e `gold.bridge_movie_company`.
  - Impede a multiplicação de linhas e distorção de agregações métricas na tabela Fato (`fact_movies_performance`), mantendo seu grão estrito em 1 registro por filme.

### Decisão 8.3: Preservação de Catálogo na Tabela Fato
* **Contexto**: Filmes podem não possuir métricas financeiras ou de engajamento preenchidas.
* **Decisão**:
  - Uso de `LEFT JOIN` partindo de `dim_movies` em direção a `silver.tb_financeiro_filmes` e `silver.tb_metricas_engajamento`, garantindo que todo filme do catálogo seja elegível para análise sem perda de registros.

---

## 9. Data Mart de Contexto GenAI / RAG (`gold.gold_genai_movies_context`)

### Decisão 9.1: NULL-Safety e Engenharia de Texto Declarativa
* **Contexto**: Funções de concatenação retornam `NULL` se qualquer operando for nulo, o que causaria descarte silencioso de filmes com dados parciais no banco vetorial.
* **Decisão**:
  - Implementação de fallbacks explícitos com `coalesce` e `when/otherwise` para cada campo (título, ano, receita formatada, orçamento formatado, atores principais agregados, diretores agregados e sinopse).
  - Garantia de 100% de preenchimento textual da coluna `llm_context_document`.
