# Relatório de Auditoria e Conformidade Técnica — CineData Analytics

Este documento apresenta uma análise técnica minuciosa da implementação desenvolvida pelo colega (disponível no diretório `example-caminha`), confrontando-a detalhadamente com os requisitos oficiais da especificação técnica (**Atividade de Dados.pdf**) e com a análise preliminar consolidada em `docs/ANALISE_COMPARATIVA_DIVERGENCIAS.md`.

O objetivo deste relatório é avaliar **o que foi retornado** vs. **o que era estritamente esperado**, destacando tanto os pontos em plena conformidade quanto os itens críticos de arquitetura, orquestração, regras de negócio e boas práticas que requerem alinhamento.

---

## 1. Matriz Geral de Conformidade por Camada e Componente

| Componente / Tabela | O que Foi Retornado pelo Colega | O que Era Esperado pelo PDF | Status de Conformidade | Ponto de Atenção / Ação de Alinhamento |
|---|---|---|---|---|
| **Orquestração: Arquivo de Job** | Arquivo `job.yml` com paths de usuário pessoal (`/Workspace/Users/caminha2103@gmail.com/...`) sem cluster configurado. | Arquivo `job.yaml`, exportável e portátil, com dependências explícitas e agendamento. | ⚠️ **Divergência Crítica** | Extensão incorreta (`.yml` vs `.yaml`), quebra de portabilidade em outros ambientes e ausência de spec de cluster. |
| **Orquestração: Casing de Notebooks** | Arquivos nomeados com sufixo minúsculo: `Landing_to_bronze.ipynb`, `Bronze_to_silver.ipynb`, `Silver_to_gold.ipynb`. | Nomenclatura exata do PDF: `Landing_to_Bronze`, `Bronze_to_Silver`, `Silver_to_Gold`. | ⚠️ **Inconsistência de Nomenclatura** | Em sistemas case-sensitive (Linux/Databricks repos), o job falha ao tentar invocar os caminhos. |
| **Bronze: Ingestão de CSVs** | Ingestão permissive com schema string e adição de `ingestion_datetime`. Gravado em Delta Append. | Criação de database `bronze`, ingestão sem alterações estruturais, timestamp e Delta Append. | ✅ **Conforme** | Total aderência às instruções da Seção 1.2. |
| **Bronze: API Cotação PTAX** | Ingestão via requests com widgets `data_inicio` e `data_fim` (MM-DD-AAAA) dos últimos 7 dias. Delta Append. | Consumo da API PTAX Olinda/BCB com parâmetros de data e gravação em `bronze.tb_cotacao_dolar`. | ✅ **Conforme** | Atende rigorosamente aos parâmetros da Seção 1.2. |
| **Silver: `tb_info_filmes`** | Mapeamento de colunas, normalização de status em 6 categorias, conversão multiformato (6 formatos) e `ano_lancamento`. | De-para de colunas em português, tipagem segura, normalização/tradução de status e deduplicação por ingestão. | ✅ **Conforme** | Normalização robusta e filtros adequados. |
| **Silver: `tb_financeiro_filmes`** | Higienização de moedas (K/M/B), conversão BRL usando cotação escalar da Bronze e cálculo de margem como markup `(R-C)/C`. | Higienização monetária, deduplicação por unicidade de filme, conversão BRL via cotação Silver, Lucro e Margem de Lucro Segura (`Lucro / Receita * 100`). | 🔴 **Divergência Grave** | Fórmula incorreta de margem de lucro (markup em vez de margem), sem proteção de divisão por zero; cotação lida direto da Bronze sem usar a Silver. |
| **Silver: `tb_metricas_engajamento`** | Conversão via `try_cast`, substituição simples de `,` por `.`, e filtros de notas (0 a 10) e contagens ($\ge 0$). | Limpeza de formatação numérica de popularidade sem descartar válidos, cast seguro anti-column shift e validação de limites de negócio. | ⚠️ **Atenção Técnica** | Substituição direta de `,` por `.` em popularidade pode invalidar números formatados com separador de milhar. |
| **Silver: `tb_avaliacoes_usuarios`** | Deduplicação por tupla completa `(id, nome, nota, comentario)`, validação de nota 0 a 10 e fallback `"Sem comentário"`. | Deduplicação integral de avaliações, conversão de notas fora da faixa para NULL e preenchimento de vazios com `"Sem comentário"`. | ✅ **Conforme** | Plena conformidade com as regras da Seção 1.3 Tópico 4. |
| **Silver: `tb_generos`** | Split/explode tratando `;` e `\|`, filtro de 19 gêneros canônicos e deduplicação por `(id_filme, nome_genero)`. | Split/explode de múltiplos delimitadores, expurgo de ruídos de column shift e catálogo deduplicado. | ✅ **Conforme** | Plena conformidade com a Seção 1.3 Tópico 5. |
| **Silver: `tb_pessoas_empresas`** | Desmembramento de 4 papéis (`Ator`, `Diretor`, `Roteirista`, `Produtora`), initcap, limpeza de URLs/números e deduplicação. | Modelo unificado com categorização de atuação, padronização de caixa e remoção de duplicatas. | ✅ **Conforme** | Preserva a integridade do elenco para as análises analíticas. |
| **Silver: `tb_cotacao_dolar`** | Geração de calendário diário contínuo e aplicação de Forward Fill com `last(..., ignorenulls=True)`. | Série contínua com Forward Fill para cobrir finais de semana e feriados sem cotação. | ⚠️ **Desconexão de Pipeline** | A tabela é gerada corretamente, mas **não é consumida** por nenhuma etapa subsequente do pipeline. |
| **Gold: Modelagem Dimensional** | Criação de `fact_movies_performance`, 5 dimensões e 3 bridge tables com Surrogate Keys `BIGINT`. | Star Schema com grão único por filme lançado na Fato, dimensões descritivas e bridges N:N. | ✅ **Conforme** | Esquema dimensional completo e normalizado. |
| **Gold: `gold_genai_movies_context`** | Geração de documento textual com template exigido e tratamento de nulos via `coalesce`/`when`. | Documento enriquecido anti-nulos concatenando metadados, métricas e elenco para Vector Search (RAG). | ⚠️ **Ponto de Melhoria** | Concatena todos os atores sem limite de tamanho (risco de estourar tokens no RAG) e inclui filmes não lançados com template afirmativo. |
| **Gold: Desafio de Analytics** | Resolução das 6 consultas via `display()`, com filtros temporais limitados ao lançamento real mais recente. | 6 perguntas de negócio com agregação, janelamento (`RANK`), filtros temporais relativos de 2 e 5 anos. | ⚠️ **Divergência Numérica na Q1** | Pergunta 1 diverge em ~R$ 504 mi por critério de desempate hash; Perguntas 2, 3, 4, 5 e 6 idênticas. |

---

## 2. Análise Aprofundada dos Pontos Críticos para Alinhamento

### Ponto 1: Arquivo e Orquestração do Workflow Databricks (`job.yaml`)

#### O que o documento oficial exige:
* **Seção 3**: Databricks Workflow com no mínimo três tarefas: `to_Bronze`, `to_Silver` e `to_Gold`, com dependências explícitas e agendamento contínuo.
* **Seção 5**: *"Entregar o arquivo .yaml gerado pela exportação do Job no Databricks. Nome do arquivo: `job.yaml`"*.
* **Seção 5**: Nomenclatura dos notebooks: `Landing_to_Bronze`, `Bronze_to_Silver`, `Silver_to_Gold`.

#### O que foi retornado pelo colega:
1. **Nome e extensão do arquivo**: Nomeado como `job.yml` (com extensão de 3 caracteres), em desacordo com o contrato de entrega que exige explicitamente `job.yaml`.
2. **Caminhos absolutos privados/hardcoded**:
   ```yaml
   notebook_path: /Workspace/Users/caminha2103@gmail.com/Landing_to_Bronze
   notebook_path: /Workspace/Users/caminha2103@gmail.com/Bronze_to_Silver
   notebook_path: /Workspace/Users/caminha2103@gmail.com/Silver_to_Gold
   ```
   Esses caminhos apontam para a pasta particular do usuário no Databricks (`/Users/caminha2103@gmail.com`). Ao importar ou rodar esse Job em qualquer outro workspace, usuário ou pipeline de CI/CD, a execução falhará imediatamente com erro de `RESOURCE_DOES_NOT_EXIST`. O correto é utilizar caminhos de repositório ou pasta compartilhada de projeto (ex.: `/Workspace/CineData_Analytics/...`).
3. **Casing dos nomes de arquivos no repositório**:
   No arquivo de job, o colega referencia `Landing_to_Bronze`, mas no repositório físico os arquivos foram salvos como `Landing_to_bronze.ipynb`, `Bronze_to_silver.ipynb` e `Silver_to_gold.ipynb`. Essa assimetria de maiúsculas/minúsculas compromete a automação em ambientes Linux.
4. **Ausência de Configuração de Cluster / Computação**:
   O `job.yml` do colega omite completamente o bloco `job_clusters`, dependendo da existência de cluster interativo ou computação serverless sem limites de tempo de vida (`timeout_seconds`), política de retentativas (`max_retries`) ou limites de paralelismo (`max_concurrent_runs: 1`).

---

### Ponto 2: Cálculo Financeiro e Conceitual de Margem de Lucro (`silver.tb_financeiro_filmes`)

#### O que o documento oficial exige:
* **Seção 1.3 (Tópico 2)**:
  > *"Derive as colunas de Lucro (Dólar/Real) e Margem de Lucro Percentual, garantindo que operações aritméticas com valores ausentes não invalidem o resultado e evitando divisões por zero."*

#### O que foi retornado pelo colega:
No arquivo `Bronze_to_silver.ipynb` (Célula 4):
```python
.withColumn(
    "margem_lucro_percentual",
    (
        (F.col("receita_usd") - F.col("orcamento_usd"))
        / F.col("orcamento_usd") * 100
    ).cast("decimal(18,2)")
)
```

#### Diagnóstico e Risco Técnico:
1. **Erro de Conceito Financeiro**:
   * O colega implementou a fórmula de **Markup / ROI (Retorno sobre Investimento)**: $\frac{\text{Receita} - \text{Orçamento}}{\text{Orçamento}} \times 100$.
   * A **Margem de Lucro (Profit Margin)** é universalmente definida em finanças e contabilidade corporativa como a fração da **Receita** que sobra após deduzidos os custos:
     $$\text{Margem de Lucro (\%)} = \left( \frac{\text{Lucro}}{\text{Receita}} \right) \times 100$$
2. **Vulnerabilidade a Divisão por Zero**:
   * O cálculo não possui encapsulamento protetivo (`when(col.isNotNull() & (col > 0), ...)`). Se `orcamento_usd` for zero, ocorre divisão por zero (que em runtime Spark com modo ANSI ativado lança exceção fatal, abortando o pipeline).

---

### Ponto 3: Desconexão Arquitetural da Cotação Cambial (`silver.tb_cotacao_dolar`)

#### O que o documento oficial exige:
* **Seção 1.2 e 1.3 (Tópicos 2 e 7)**:
  > *"Extraia a cotação do dólar via API do Banco Central e salve na tabela bronze.tb_cotacao_dolar."*  
  > *"Como a API do Banco Central não possui cotações em finais de semana e feriados, estruture o histórico de forma a garantir uma série temporal contínua. Aplique uma técnica de preenchimento (Forward Fill) de modo que dias sem cotação recebam o valor do último dia útil disponível."*  
  > *"Calcule os valores equivalentes em Reais (BRL) aplicando a taxa de cotação obtida."*

#### O que foi retornado pelo colega:
1. Na Célula 4 de `Bronze_to_silver.ipynb`, ao construir `silver.tb_financeiro_filmes`, o colega extrai um único valor escalar diretamente da camada **Bronze**:
   ```python
   # Utiliza a cotação mais recente disponível na Bronze.
   cotacao_atual = (
       spark.table("workspace.bronze.tb_cotacao_dolar")
       .orderBy(F.col("dataHoraCotacao").desc())
       .select(F.col("cotacaoCompra").cast("decimal(10,4)"))
       .first()[0]
   )
   ```
2. Posteriormente, na Célula 14 do mesmo notebook, o colega executa a lógica completa de expansão de calendário contínuo com Forward Fill para gravar `silver.tb_cotacao_dolar`.

#### Diagnóstico e Risco Arquitetural:
* **Quebra de Linhagem (Lineage) e Inversão de Camadas**: A transformação Silver `tb_financeiro_filmes` foi executada antes da `tb_cotacao_dolar` e buscou os dados brutos da Bronze, ignorando a tabela Silver que acabara de ser solicitada no projeto.
* **Tabela Órfã**: A tabela `silver.tb_cotacao_dolar`, construída com sofisticação para tratar finais de semana e feriados via Forward Fill, tornou-se inútil no pipeline do colega, pois não é consumida por nenhuma outra tabela Silver ou Gold.
* **Alinhamento**: A leitura da cotação deve ocorrer a partir da tabela já higienizada `silver.tb_cotacao_dolar`, garantindo rastreabilidade e governança entre as camadas.

---

### Ponto 4: Racional de Deduplicação e Desempate em Métricas Financeiras

#### O que o documento oficial exige:
* **Seção 1.3 e 2**: Unicidade por filme sem duplicar grão através dos joins, garantindo consolidação correta das métricas financeiras.

#### O que foi retornado pelo colega vs. Nosso Ajuste:
* O colega aplicou desempate com `_qualidade` (contagem booleana de campos preenchidos) e, havendo empate, utilizou `F.xxhash64("budget", "revenue").desc()`.
* **Impacto**: Quando um filme possui linhas conflitantes (ex.: uma linha com `revenue = 500` e `budget = NULL` e outra com `budget = 200` e `revenue = NULL`), ambas possuem `_qualidade = 1`. O hash determinou aleatoriamente qual linha venceu. Isso descartou centenas de valores válidos de receita, resultando em:
  * Receita Total do Colega: **R$ 837.771.066.819,07**
  * Receita Total Conforme: **R$ 838.275.201.039,60** (diferença de R$ 504.134.220,53).
* **Alinhamento**: Priorizar explicitamente registros com métricas preenchidas em ordem decrescente de valor monetário (`col("receita_usd").desc_nulls_last()`) antes de recorrer a critérios de timestamp ou identificador.

---

### Ponto 5: Data Mart GenAI / RAG (`gold.gold_genai_movies_context`)

#### O que o documento oficial exige:
* **Seção 2 (Entrega 2)**:
  > *"Template esperado: 'O filme [TÍTULO], lançado no ano de [ANO], faturou [RECEITA] e teve um custo de [ORÇAMENTO]. Estrelado por [ATORES PRINCIPAIS] e dirigido por [DIRETOR], o filme possui a seguinte sinopse: [OVERVIEW].'"*  
  > *"Como há múltiplos atores por filme, será necessário agregá-los em uma única string."*  
  > *"Atenção Técnica — a 'casca de banana' dos nulos: funções de concatenação retornam NULL se qualquer campo for nulo... avalie textos de fallback."*

#### O que foi retornado pelo colega:
1. O colega aplicou `F.collect_set` sem restrição de quantidade para compor `[ATORES PRINCIPAIS]`:
   ```python
   F.concat_ws(", ", F.collect_set(F.when(F.col("tipo_pessoa") == "Ator", F.col("nome_pessoa"))))
   ```
2. Realizou `left join` de `dim_movies` (que contém todos os 99.000 filmes, inclusive cancelados e planejados) com a Fato (que contém apenas lançados).

#### Oportunidade de Alinhamento:
* **Sobrecarga de Tokens no RAG**: Filmes com elencos de 40 a 60 atores geram documentos textuais desproporcionalmente grandes. A especificação orienta expressamente `[ATORES PRINCIPAIS]`. Limitar a agregação aos primeiros 5 a 10 atores (utilizando `slice(collect_set(...), 1, 10)`) preserva o contexto sem onerar o banco vetorial.
* **Filmes Não Lançados**: Filmes com status `Cancelado`, `Em Produção` ou `Planejado` recebem textos do tipo: `"O filme X, lançado no ano de ano não informado, faturou valor não informado..."`. Para um assistente virtual de IA, afirmar que um filme cancelado foi "lançado" gera alucinações no LLM. Recomenda-se filtrar apenas filmes lançados ou adaptar dinamicamente o template.

---

### Ponto 6: Limpeza Numérica de Popularidade (`silver.tb_metricas_engajamento`)

#### O que o documento oficial exige:
* **Seção 1.3 (Tópico 3)**:
  > *"A coluna de popularidade apresenta inconsistências de pontuação e separadores decimais na origem. Limpe a formatação numérica da coluna antes da conversão de tipo, garantindo que os valores não sejam invalidados ou convertidos silenciosamente para nulos."*

#### O que foi retornado pelo colega:
```python
F.expr("try_cast(regexp_replace(trim(popularity), ',', '.') as double)")
```

#### Diagnóstico e Alinhamento:
* A substituição incondicional de vírgula por ponto resolve casos onde a vírgula é o separador decimal (padrão brasileiro/europeu), mas corrompe valores que utilizam vírgula como separador de milhar (padrão americano `1,234.56` $\rightarrow$ `1.234.56`), transformando-os em `NULL` durante o `try_cast`.
* Uma rotina de higienização que analisa se a string contém múltiplos separadores ou remove vírgulas de milhar garante 100% de integridade.

---

### Ponto 7: Padrões de Código e Boas Práticas (Clean Code)

| Critério Clean Code | Situação no Código do Colega | Recomendação de Alinhamento |
|---|---|---|
| **Variáveis de 1 ou 2 letras** | Uso ostensivo de `df`, `p`, `rn`, `k`, `v`, `janela`. | Adotar nomes semânticos descritivos (ex.: `movies_financials_dataframe`, `window_deduplication`). |
| **Magic Numbers** | Presença de literais no código: `* 1_000`, `* 1_000_000`, `* 1_000_000_000`, `* 100`. | Centralizar em constantes em caixa alta: `THOUSAND = 1_000`, `MILLION = 1_000_000`, etc. |
| **Importações e Verbosidade** | Uso repetitivo de `F.col(...)`, `F.when(...)`, `F.lit(...)`. | Importar diretamente as funções de `pyspark.sql.functions` para melhorar a legibilidade matemática. |
| **Modularidade** | Blocos monolíticos com código procedural e sem encapsulamento em funções puras. | Modularizar cada tabela em funções puras de transformação com tipagem estrita de entrada e saída. |

---

## 3. Síntese do Desafio de Analytics (Perguntas de Negócio 1 a 6)

| Pergunta de Negócio | Resultado do Colega | Nosso Resultado Alinhado | Status | Comentário Técnico |
|---|---|---|---|---|
| **1. Receita total (R$)** | `R$ 837.771.066.819,07` | `R$ 838.275.201.039,60` | ⚠️ Divergência (~R$ 504 mi) | Causa: desempate determinístico por hash que descartou receitas legítimas em registros conflitantes. |
| **2. Top 5 Popularidade** | Idêntico | Idêntico | ✅ Conforme | Mesmos filmes e métricas em ambas as abordagens. |
| **3. Volume por Gênero** | Idêntico | Idêntico | ✅ Conforme | Mesma ordenação e contagem exata por gênero. |
| **4. Top 10 Receita com RANK()** | Idêntico | Idêntico | ✅ Conforme | Mesmos títulos e valores de ranking em USD e BRL. |
| **5. Ator mais frequente (2 anos)** | `Kevin Hart (64 participações)` | `Kevin Hart (64 participações)` | ✅ Conforme | Acerto do colega em não aplicar deduplicação cega por filme em `credits_and_tags`. |
| **6. Produtora maior lucro (5 anos)** | `Universal Pictures (US$ 5.772.329.679)` | `Universal Pictures (US$ 5.772.329.679)` | ✅ Conforme | Mesmo resultado e janela temporal (restando 60 meses a partir do último lançamento real). |

---

## 4. Plano de Ação Recomendado para Alinhamento

Para consolidar o projeto em grau máximo de conformidade com a avaliação da Visagio:

1. **Orquestração (`job.yaml`)**:
   - Renomear `job.yml` para `job.yaml`.
   - Substituir os caminhos privados `/Workspace/Users/...` por caminhos relativos ao repositório ou pasta de projeto `/Workspace/CineData_Analytics/...`.
   - Ajustar o casing dos nomes dos notebooks para PascalCase (`Landing_to_Bronze`, `Bronze_to_Silver`, `Silver_to_Gold`).
   - Adicionar a especificação do `job_cluster` com sizing e políticas de retry/timeout.

2. **Cálculo de Margem e Conversão Cambial**:
   - Ajustar a fórmula da margem de lucro para $\frac{\text{Lucro}}{\text{Receita}} \times 100$ com proteção de divisão por zero.
   - Conectar a leitura da cotação diretamente a partir de `silver.tb_cotacao_dolar`.

3. **Data Mart GenAI**:
   - Limitar o número de atores principais concatenados para 5 a 10 nomes, preservando a coerência do RAG.
   - Filtrar a base de contexto para filmes lançados ou adaptar a frase para status não lançados.

4. **Higienização de Código**:
   - Remover variáveis de uma única letra e eliminar números mágicos, elevando o projeto aos mais rigorosos padrões de Clean Code da engenharia de dados moderna.
