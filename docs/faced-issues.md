# Common Faced Issues — CineData Analytics

Documento de registro e lições aprendidas sobre os principais problemas enfrentados durante o desenvolvimento do pipeline, o que ocorria e o impacto gerado nas métricas e tabelas.

---

## 1. Arredondamento Prematuro da Taxa de Câmbio (USD $\rightarrow$ BRL)

* **Onde era o problema**: Célula 7 de `notebooks/Bronze_to_Silver.ipynb` (`transform_tb_movies_financials`).
* **O que acontecia**: A taxa oficial PTAX de compra lida da API BACEN (`5,1569`) era convertida diretamente para a constante monetária `DECIMAL_PRECISION` (`"decimal(18,2)"`), truncando e arredondando prematuramente o multiplicador cambial para **`5,16`** antes de multiplicar pelas receitas e orçamentos.
* **Impacto**:
  * **Receita Total em BRL (Pergunta 1)**: Inflacionou o resultado em **+R$ 503,61 milhões** (+0,0601%), totalizando R$ 838,27 bilhões em vez dos R$ 837,77 bilhões reais.
  * **Top 10 Receita em BRL (Pergunta 4)**: Valores em Reais ligeiramente inflacionados (a ordem e o ranking em USD permaneceram intactos).
* **Solução aplicada**: Criação de constante dedicada com 4 casas decimais (`EXCHANGE_RATE_PRECISION = "decimal(18,4)"`) para cálculos cambiais intermediários.

---

## 2. Deduplicação Indevida por `id` na Extração de Entidades (Créditos)

* **Onde era o problema**: Célula 15 de `notebooks/Bronze_to_Silver.ipynb` (`transform_tb_pessoas_empresas`).
* **O que acontecia**: Aplicava-se `deduplicate_latest(raw_credits_dataframe, business_key_column="id")` antes de desmembrar atores, diretores e produtoras. Na base de origem (`tb_credits_and_tags`), um mesmo filme possui múltiplos registros complementares para acomodar elencos extensos ou adições de produtoras/roteiristas.
* **Impacto**:
  * **Perda de Entidades**: A deduplicação precipitada descartava ~7.000 registros legítimos de elenco e empresas (a tabela caía de 896.968 para 889.993 entidades).
  * **Métricas de Pessoas e Produtoras (Perguntas 5 e 6)**: Subestimava a contagem de participações de atores nos últimos 2 anos e podia ocultar vínculos com estúdios/produtoras.
* **Solução aplicada**: Remoção da deduplicação por `id` no nível do filme, aplicando a deduplicação apenas após a explosão e normalização atômica na tríade natural `(id_filme, nome_entidade, tipo_entidade)`.

---

## 3. Deslocamento de Colunas (*Column Shift*) em Métricas e Popularidade

* **Onde era o problema**: Célula 9 de `notebooks/Bronze_to_Silver.ipynb` (`transform_tb_movies_metrics`).
* **O que acontecia**: Falhas de delimitação e separadores no arquivo bruto de origem (`movies_metrics_IMDB_TMDB.csv`) empurraram anos redondos de lançamento (`2020`, `2019`, `2018`) para a coluna de popularidade de filmes desconhecidos, enquanto notas/votos recebiam textos residuais.
* **Impacto**:
  * **Top 5 Filmes mais Populares (Pergunta 2)**: Títulos obscuros com ano vazado para a métrica (ex.: *La Fellinette* com valor `2020.0`, *The Fear Footage 2* com `2019.0`) assumiam indevidamente o topo do ranking de popularidade, ocultando os verdadeiros líderes.
* **Solução aplicada**: Criação de filtro de validação estrutural (`corrupted_shift_condition`) que identifica padrões de ano `^(18|19|20)[0-9]{2}$` associados a contagens baixas de votos e resíduos textuais via regex, convertendo a popularidade corrompida para `NULL` e permitindo que filmes legítimos (*Blue Beetle*, *Gran Turismo*, *The Nun II*, etc.) ocupem o topo.
