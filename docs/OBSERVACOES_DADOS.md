# Observações e Análise de Qualidade dos Dados (Data Profiling)
## Projeto CineData Analytics — Camada Bronze & Silver

Este documento registra as observações técnicas, auditorias de qualidade, volumetria e comportamentos identificados durante o processo de exploração e transformação dos dados em todas as tabelas da camada Silver.

---

## 1. Tabela Financeira (`bronze.tb_movies_financials` $\rightarrow$ `silver.tb_financeiro_filmes`)

### 1.1. Volumetria e Distribuição de Nulos
* **Raio-X da Base Bruta (`bronze.tb_movies_financials` — 636.990 registros)**:
  * `budget` com valor `"0"`: 546.168 ocorrências (**85,7%** da base bruta).
  * `revenue` com valor `"0"`: 558.024 ocorrências (**87,6%** da base bruta).
  * `revenue` com texto sentinela (`"Unknown"`, `"Não Informado"`): 56.424 ocorrências (**8,9%** da base bruta).
  * `revenue` com valores negativos (ex: `-100000`): 1.050 ocorrências (**0,2%** da base bruta).

* **Distribuição na Camada Silver (`silver.tb_financeiro_filmes` — 99.006 filmes únicos)**:
  * Filmes com Orçamento Válido: 8.171 (**8,25%**)
  * Filmes com Orçamento Ausente (`NULL`): 90.835 (**91,75%**)
  * Filmes com Receita Válida: 3.285 (**3,32%**)
  * Filmes com Receita Ausente (`NULL`): 95.721 (**96,68%**)
  * Filmes com Ambos Preenchidos: 1.566 (**1,58%**) — Cálculo de Lucro e Margem %
  * Filmes sem Qualquer Dado Financeiro: 89.116 (**90,01%**)

---

## 2. Tabela de Métricas de Engajamento (`bronze.tb_movies_metrics` $\rightarrow$ `silver.tb_metricas_engajamento`)

* **Column Shift e Tipagem Corrompida**: Fragmentos de texto longo (sinopses, nomes de diretores) foram detectados nas colunas `popularity`, `vote_average` e `vote_count` na base bruta. O uso de `try_cast` com tratamento prévio de vírgula para ponto converte essas inconsistências em `NULL` de forma transparente.
* **Escala de Notas**: Identificou-se ocorrências de notas médias superiores a 10 (ex.: `79.97`, `82.42` decorrentes de erro de escala $\times 10$ ou ruído), as quais foram invalidadas para `NULL` garantindo integridade nos cálculos analíticos.

---

## 3. Tabela de Avaliações de Usuários (`bronze.tb_movies_reviews` $\rightarrow$ `silver.tb_avaliacoes_usuarios`)

* **Volumetria Bruta vs. Deduplicada**: De 194.472 registros brutos na Bronze, a deduplicação integral pelo quarteto `(id, nome, nota, comentario)` resultou em **32.412 avaliações únicas**.
* **Comentários Vazios**: Avaliações com comentário `NULL` ou preenchido apenas com espaços foram padronizadas como `"Sem comentário"`.

---

## 4. Tabela de Gêneros (`bronze.tb_credits_and_tags` $\rightarrow$ `silver.tb_generos`)

* **Separadores Heterogêneos**: A coluna bruta `genres` apresenta valores separados por vírgula (`,`), ponto e vírgula (`;`) e barras verticais (`|`).
* **Ruídos de Column Shift**: A tokenização revelou fragmentos de imagens (ex: `/pz1H1ZHjBGT2X4GPIF69bYzVJpR.jpg`), números (`0.6`) e textos descritivos na coluna. A filtragem pelo domínio canônico dos 19 gêneros oficiais do TMDB expurgou 100% dos resíduos indesejados, gerando 142.160 associações filme-gênero limpas.

---

## 5. Tabela de Pessoas e Empresas (`bronze.tb_credits_and_tags` $\rightarrow$ `silver.tb_pessoas_empresas`)

* **Consolidação Unificada**: Consolidação dos 4 papéis (`cast` $\rightarrow$ `'Ator'`, `directors` $\rightarrow$ `'Diretor'`, `writers` $\rightarrow$ `'Roteirista'`, `production_companies` $\rightarrow$ `'Produtora'`).
* **Padronização Textual**: Aplicação de `initcap`, limpeza de caracteres especiais (`"`, `'`, `[`, `]`, `{`, `}`) e remoção de imagens/URLs/números deslocados.

---

## 6. Tabela de Cotação do Dólar (`bronze.tb_cotacao_dolar` $\rightarrow$ `silver.tb_cotacao_dolar`)

* **Descontinuidade Temporal da API**: A API PTAX do Banco Central só emite cotações em dias úteis. A geração de um calendário contínuo diário somado ao *Forward Fill* (`last(cotacao, ignorenulls=True)`) garantiu a continuidade temporal necessária para os cálculos financeiros em fins de semana e feriados.
