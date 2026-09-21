# Registro de Decisões de Arquitetura e Engenharia de Dados (ADR)
## Projeto CineData Analytics — Pipeline Lakehouse Databricks

Este documento consolida as decisões arquiteturais, justificativas de modelagem, regras de negócio e contratos de dados aplicados ao longo do pipeline da camada Silver.

---

## 1. silver.tb_info_filmes (Origem: `bronze.tb_movies_info`)

### Decisão 1.1: Deduplicação Canônica por Obra Cinematográfica (`tconst`) vs. Identificador Técnico (`id`)
* **Contexto**: 
  - A base de origem (`movies_info_TMDB_IMDB.csv`) apresenta anomalias graves de duplicidade de catálogo decorrentes de cadastros múltiplos no TMDB. Foram identificados **321 códigos IMDb (`tconst`) associados a múltiplos `id` numéricos do TMDB**, somando **986 IDs distintos** (665 instâncias redundantes).
  - O exemplo mais crítico é o filme *Die Hart 2: Die Harter* (`tt32094375`), que constava em **1.716 linhas duplicadas espalhadas por 61 IDs numéricos distintos** do TMDB (`1300214`, `1362673`, `1611488`, `1476515`, etc.), todos com a mesma data de lançamento (`30/05/2024`).
* **Decisão de Engenharia e Negócio**:
  - **Prioridade da Integridade Real da Obra sobre a Chave Técnica**: Entendeu-se expressamente que a integridade analítica da obra cinematográfica real (representada pelo identificador canônico e universal da indústria IMDb `tconst`) é soberana em relação à chave técnica local do TMDB (`id`).
  - **Mecanismo de Resolução**: A deduplicação na camada Silver foi implementada particionando prioritariamente pela chave universal `coalesce(tconst, id)`, ordenando por `ingestion_datetime DESC NULLS LAST` e desempatando por `id ASC`.
  - **Preservação de Schema e Descarte da Coluna `tconst`**: Uma vez resolvida a unicidade da obra e selecionado o registro canônico mais atual, a coluna `tconst` é descartada e o `id_filme` técnico eleito é mantido para honrar estritamente o contrato de dados oficial especificado para a camada Silver e viabilizar os joins a jusante com as demais tabelas.
* **Impactos Métricos e Analíticos**:
  - **Participações de Elenco**: Neutralizou a distorção artificial de Kevin Hart (que antes apresentava 64 participações nominais decorrentes das réplicas de *Die Hart 2*, e agora apresenta suas 2 obras legítimas), revelando o ranking verídico do biênio liderado por Suhas (4 participações).
  - **Contagem por Gênero**: Expurgou centenas de contagens fantasmas de gêneros em filmes replicados (ex.: Action com 5.935 filmes reais vs. 6.049 inflados).
  - **Integridade Financeira**: Eliminou **US$ 489.329,00** de receita duplicada e **US$ 2.054.749,00** de orçamento fantasma distribuídos entre múltiplos IDs de uma mesma obra.

### Decisão 1.2: Normalização e Tradução Declarativa de Status com Fallback
* **Contexto**: Registros de status contêm variações de caixa, hífens redundantes (`"Post-Production"`, `"in-production"`) e valores desconhecidos.
* **Decisão**: Aplicou-se normalização por regex e mapeamento `create_map` com fallback explícito para `"Não Informado"`.

---


## 2. silver.tb_financeiro_filmes (Origem: `bronze.tb_movies_financials`)

### Decisão 2.1: Deduplicação com Desempate por Maior Valor Preenchido
* **Contexto**: A origem apresenta múltiplas linhas com o mesmo `id` contendo dados conflitantes (ex.: uma linha com faturamento preenchido coexistindo com outra linha zerada ou com texto sentinela `Unknown`).
* **Decisão**: 
  - As colunas de receita e orçamento são sanitizadas e tipadas antes do particionamento.
  - A janela de deduplicação prioriza explicitamente o maior valor: `orderBy(col("receita_usd").desc_nulls_last(), col("orcamento_usd").desc_nulls_last(), col("ingestion_datetime").desc())`.
  - Isso garante que a melhor informação financeira seja preservada para cada filme sem perdas decorrentes de deduplicações determinísticas cegas.

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
