# Diretrizes e Padrões de Código (Clean Code & PySpark)

Este documento define as regras de desenvolvimento, legibilidade e estilo de código para o projeto **CineData Analytics**. Todas as implementações devem seguir rigorosamente estas diretrizes.

---

## 1. Nomenclatura Semântica e Autoexplicativa (Sem variáveis de 1 letra)

* **Proibido o uso de variáveis de uma única letra** (ex.: `s`, `x`, `i`, `v`, `k`, `df`, `kv`).
* Toda variável, argumento de função e iterador de *list comprehension* deve possuir um nome que descreva exatamente o seu propósito e conteúdo no contexto de negócio ou técnico.

### Exemplos:
* ❌ **Evite**: `[s.upper() for s in sentinels]`
* ✅ **Prefira**: `[sentinel_text.upper() for sentinel_text in sentinel_values]`
* ❌ **Evite**: `[lit(x) for kv in mapping.items() for x in kv]`
* ✅ **Prefira**: `[lit(item) for key_value_pair in translation_map.items() for item in key_value_pair]`
* ❌ **Evite**: `df`
* ✅ **Prefira**: `dataframe` ou `movies_dataframe`

---

## 2. Imutabilidade e Nomes Descritivos para Etapas de Transformação

* **Evitar reutilização/sombreamento da mesma variável** para transformações sequenciais distintas (ex.: `col_cleaned = ...; col_cleaned = ...`).
* Cada estágio intermediário de transformação deve receber um nome que explicite com clareza o que foi alterado naquela etapa, facilitando a depuração e tornando comentários redundantes desnecessários.

### Exemplos:
* ❌ **Evite**:
  ```python
  col_cleaned = when(col.isin(...), lit(None)).otherwise(col)
  col_cleaned = regexp_replace(col_cleaned, r"[$\s]|USD", "")
  col_cleaned = regexp_replace(col_cleaned, r",", "")
  ```
* ✅ **Prefira**:
  ```python
  column_with_null_sentinels = when(column_upper.isin(normalized_sentinels), lit(None)).otherwise(column_upper)
  column_without_currency_symbols = regexp_replace(column_with_null_sentinels, r"[$\s]|USD", "")
  column_sanitized = regexp_replace(column_without_currency_symbols, r",", "")
  ```

---

## 3. Eliminação de Magic Numbers (Uso de Constantes Explícitas)

* Nenhum valor numérico literal com significado específico deve aparecer diretamente no meio de funções sem ser nomeado.
* Constantes devem ser definidas com nomes claros em caixa alta (`UPPER_SNAKE_CASE`) no escopo de configuração ou utilitário.

### Exemplos:
* ❌ **Evite**: `val_k.cast("double") * 1000`, `val_m.cast("double") * 1000000`, `val_b.cast("double") * 1000000000`, `* 100`
* ✅ **Prefira**:
  ```python
  THOUSAND_MULTIPLIER = 1_000
  MILLION_MULTIPLIER = 1_000_000
  BILLION_MULTIPLIER = 1_000_000_000
  PERCENTAGE_FACTOR = 100
  DECIMAL_PRECISION = "decimal(18,2)"
  ```

---

## 4. Importação Limpa e Redução de Verbosidade (`F.` vs Funções Diretas)

* Em vez de prefixar cada chamada com `F.when`, `F.col`, `F.lit`, `F.regexp_replace`, importe diretamente as funções necessárias de `pyspark.sql.functions` ou utilize aliases expressivos.
* Isso melhora a fluidez e a legibilidade matemática das expressões de DataFrame.

### Exemplos:
* ❌ **Evite**:
  ```python
  F.when(F.col("receita_usd").isNotNull() & (F.col("receita_usd") > 0), ...)
  ```
* ✅ **Prefira**:
  ```python
  from pyspark.sql.functions import col, when, lit, coalesce, upper, trim, regexp_replace, regexp_extract, year, current_timestamp, create_map, row_number, try_to_date, expr
  
  when(col_revenue_usd.isNotNull() & (col_revenue_usd > 0), ...)
  ```

---

## 5. Princípio DRY em Expressões de Coluna (Evitar Repetição de `col(...)`)

* Se uma coluna calculada ou existente for referenciada mais de uma vez dentro de uma função ou expressão encadeada, armazene a referência em uma variável semântica local.
* Isso evita chamadas repetidas a `col("mesmo_nome")` e torna expressões complexas (como fórmulas de margem de lucro e diferenças) mais concisas e legíveis.

### Exemplos:
* ❌ **Evite**:
  ```python
  .withColumn("lucro_usd", (col("receita_usd") - col("orcamento_usd")).cast("decimal(18,2)"))
  .withColumn("margem_lucro_percentual", when((col("receita_usd").isNotNull()) & (col("receita_usd") > 0), ((col("lucro_usd") / col("receita_usd")) * 100)))
  ```
* ✅ **Prefira**:
  ```python
  col_revenue_usd = col("receita_usd")
  col_budget_usd = col("orcamento_usd")
  col_profit_usd = col("lucro_usd")
  
  # Uso direto e limpo das variáveis de coluna:
  profit_usd_expression = (col_revenue_usd - col_budget_usd).cast(DECIMAL_PRECISION)
  profit_margin_expression = when(
      col_revenue_usd.isNotNull() & (col_revenue_usd > 0) & col_profit_usd.isNotNull(),
      ((col_profit_usd / col_revenue_usd) * PERCENTAGE_FACTOR).cast(DECIMAL_PRECISION)
  ).otherwise(lit(None))
  ```

---

## 6. Funções Utilitárias Puras e Semânticas (Intencionalidade de Código)

* Operações matemáticas e lógicas recorrentes com regras de proteção (como cálculo de percentual, proporção e conversões cambiais) devem ser encapsuladas em funções puras autoexplicativas que retornam `Column`.
* Isso torna as chamadas nas transformações diretas, legíveis e intencionais.

### Exemplos:
* ✅ **Prefira**:
  ```python
  def calculate_safe_percentage(
      numerator_column: Column,
      denominator_column: Column,
      scale_factor: int = PERCENTAGE_FACTOR,
      target_precision: str = DECIMAL_PRECISION
  ) -> Column:
      """
      Calcula percentual de forma segura ((numerador / denominador) * scale_factor),
      protegendo contra divisões por zero e propagando NULL para valores ausentes.
      """
      valid_division_condition = (
          numerator_column.isNotNull()
          & denominator_column.isNotNull()
          & (denominator_column > 0)
      )
      percentage_calculation = ((numerator_column / denominator_column) * scale_factor).cast(target_precision)
      return when(valid_division_condition, percentage_calculation).otherwise(lit(None))
  ```

---

## 7. Proibição Estrita de Auto-Menção em Documentações e Códigos

* **Nunca fazer auto-referência ou citar ferramentas/assistentes** em nenhum arquivo de documentação Markdown (`.md`), células de documentação em notebooks Jupyter (`.ipynb`) ou comentários no código.
* Evite termos como "GEMINI.md", "Gemini", "assistente de IA", "gerado pela IA" ou similares na documentação técnica e no código.
* Toda a documentação, comentários e mensagens devem ser estritamente impessoais, profissionais e orientados exclusivamente ao domínio de dados e engenharia da CineData Analytics.

### Exemplos:
* ❌ **Evite**: `Este notebook segue as diretrizes do GEMINI.md criadas pelo assistente.`
* ✅ **Prefira**: `Este notebook realiza o pipeline de transformação da camada Bronze para a Silver segundo as especificações técnicas e regras de negócio do projeto.`

---

## 8. Uso Direto de Constantes Globais vs. Parâmetros Inflados

* Se um parâmetro de uma função existe unicamente para receber um valor constante global e imutável pelo chamador, **não infle a assinatura da função** com esse parâmetro.
* Utilize a constante diretamente no corpo da função ou atribua uma variável local a essa constante.
* Mantenha parâmetros de função apenas para valores que realmente variam dinamicamente por chamada (como o próprio DataFrame ou colunas configuráveis).

### Exemplos:
* ❌ **Evite**:
  ```python
  def transform_tb_generos(raw_credits_dataframe: DataFrame, canonical_genres_list: list[str]) -> DataFrame:
      ...
  # Chamada inflada:
  transform_tb_generos(dataframe_credits_bronze, canonical_genres_list=KNOWN_GENRES_LIST)
  ```
* ✅ **Prefira**:
  ```python
  def transform_tb_generos(raw_credits_dataframe: DataFrame) -> DataFrame:
      # Uso direto da constante do domínio de gêneros:
      valid_genres_domain = KNOWN_GENRES_LIST
      ...
  # Chamada direta e limpa:
  transform_tb_generos(dataframe_credits_bronze)
  ```

---

## 9. Comentários Intencionais Focados Exclusivamente nas Regras de Negócio

* Cada tópico e tabela do pipeline possui instruções, contratos e regras de negócio específicas (ex.: *Forward Fill* de cotações em fins de semana, remoção de registros fora da escala 0 a 10, preenchimento de comentários vazios com `"Sem comentário"`).
* **Proibidos comentários mecânicos, óbvios ou de fluxo procedural** que apenas narram ações triviais de execução, chamadas de I/O ou sintaxe do Spark (ex.: `# Leitura da Bronze`, `# Execução da Transformação`, `# Gravação na Silver`, `# Validação do Schema e Amostra`, `# Faz o select`, `# aplicando filter`).
* O código limpo com nomenclatura semântica deve ser autoexplicativo em sua estrutura.
* **Os únicos comentários no código devem ficar posicionados diretamente acima das linhas/blocos exatos onde cada regra de negócio ou decisão não-trivial é aplicada**, explicitando a intenção e conformidade com o requisito.

### Exemplos:
* ❌ **Evite**:
  ```python
  # Leitura da Bronze
  dataframe_reviews_bronze = spark.read.parquet(...)

  # Execução do Pipeline de Transformação
  dataframe_reviews_silver = transform_tb_movies_reviews(dataframe_reviews_bronze)

  # Gravação na Silver
  write_dataframe_with_timestamp(dataframe_reviews_silver, ...)

  # Validação do Schema e Amostra
  dataframe_reviews_silver.printSchema()
  dataframe_reviews_silver.show(5)
  ```
* ✅ **Prefira**:
  ```python
  # Chamada direta e limpa sem comentários procedurais óbvios:
  dataframe_reviews_bronze = spark.read.parquet(...)
  dataframe_reviews_silver = transform_tb_movies_reviews(dataframe_reviews_bronze)
  write_dataframe_with_timestamp(
      dataframe=dataframe_reviews_silver,
      target_path=SILVER_DIRECTORY / "silver.tb_avaliacoes_usuarios"
  )
  dataframe_reviews_silver.printSchema()
  dataframe_reviews_silver.show(5, truncate=False)
  ```
* ❌ **Evite**:
  ```python
  # Faz o drop de duplicados
  df.dropDuplicates(...)
  # Converte coluna para int
  col.cast("int")
  ```
* ✅ **Prefira**:
  ```python
  # Como a API do Banco Central não possui cotações em fins de semana e feriados,
  # aplica-se Forward Fill propagando a última cotação útil disponível na série contínua.
  .withColumn("cotacao_compra", last(col_raw_quote, ignorenulls=True).over(forward_fill_window))
  
  # A nota de usuário deve respeitar a escala permitida (0 a 10). Valores fora da faixa são convertidos para NULL.
  valid_rating_condition = col_user_rating.between(MINIMUM_RATING_VALUE, MAXIMUM_RATING_VALUE)
  ```

---

## 10. Coesão, Localidade e Modularidade de Funções e Constantes Específicas

* Funções auxiliares, constantes de domínio e dicionários de mapeamento que são utilizados exclusivamente por uma única tabela ou etapa de processamento devem ser definidos **próximos ao local de sua utilização** (na respectiva seção ou célula da tabela), e não centralizados em uma célula genérica no início do arquivo.
* Na célula inicial de configuração, devem permanecer exclusivamente:
  * Importações de bibliotecas e funções do Spark;
  * Inicialização da sessão Spark;
  * Definição de diretórios base e caminhos do *data lake*;
  * Utilitários e constantes verdadeiramente globais compartilhados por múltiplos pipelines (ex.: função de persistência com *timestamp*, deduplicação por chave de negócio, *safe casting* genérico e sentinelas de valores nulos).
* Essa prática aumenta a alta coesão e o encapsulamento, tornando cada estágio de transformação autocontido, legível e mais fácil de manter e depurar.

### Exemplos:
* ❌ **Evite**: Declarar `STATUS_TRANSLATION_MAP`, `SUPPORTED_DATE_FORMATS`, `KNOWN_GENRES_LIST` e `ENTITY_TYPE_MAPPINGS` todos misturados em uma única célula de setup no topo do notebook.
* ✅ **Prefira**: Definir `KNOWN_GENRES_LIST` diretamente no bloco de transformação de `silver.tb_generos`, `STATUS_TRANSLATION_MAP` no bloco de `silver.tb_info_filmes`, e assim por diante.

---

## 11. Constantes de Colunas com Acesso Estático em Dataclasses (Sem Instanciação Redundante)

* Para mapear nomes de colunas de forma fortemente tipada e imutável, utilize classes ou `@dataclass(frozen=True)`.
* **Proibida a instanciação redundante da classe para variáveis em caixa alta** (ex.: `BACEN_COTACAO_COLUMNS = BacenCotacaoColumns()`), pois isso introduz verbosidade desnecessária e infla o escopo global.
* Acesse os atributos de coluna diretamente na própria classe como propriedades estáticas.

### Exemplos:
* ❌ **Evite**:
  ```python
  @dataclass(frozen=True)
  class BacenCotacaoColumns:
      COTACAO_COMPRA: str = "cotacaoCompra"
      DATA_HORA_COTACAO: str = "dataHoraCotacao"

  BACEN_COTACAO_COLUMNS = BacenCotacaoColumns()
  # Uso excessivamente verboso:
  col(BACEN_COTACAO_COLUMNS.COTACAO_COMPRA)
  ```
* ✅ **Prefira**:
  ```python
  @dataclass(frozen=True)
  class BacenCotacaoColumns:
      COTACAO_COMPRA: str = "cotacaoCompra"
      DATA_HORA_COTACAO: str = "dataHoraCotacao"

  # Acesso direto, limpo e estático:
  col(BacenCotacaoColumns.COTACAO_COMPRA)
  ```

---

## 12. Nomenclatura PascalCase para Contratos de Schema (StructType)

* Instâncias estruturadas de `StructType` que definem os contratos e esquemas canônicos de tabelas devem seguir nomenclatura em **`PascalCase`** (ex.: `TbCotacaoDolarSilverSchema`, `TbInfoFilmesSilverSchema`, `DimFilmesGoldSchema`, `RawMoviesInfoBronzeSchema`).
* Isso confere aos contratos de esquema a semântica visual e arquitetural de tipos/classes de dados estruturados, diferenciando-os de instâncias dinâmicas de DataFrame e variáveis de fluxo.

### Exemplos:
* ❌ **Evite**: `schema_tb_cotacao_dolar_silver = StructType(...)`, `SCHEMA_TB_COTACAO_DOLAR_SILVER = StructType(...)`
* ✅ **Prefira**: `TbCotacaoDolarSilverSchema = StructType(...)`, `DimFilmesGoldSchema = StructType(...)`

---

## 13. Contratos Tipados de Entrada e Saída (Zero Magic Strings)

* Nenhuma string literal de coluna deve ser passada diretamente solta em `col("...")`, `.select(...)` ou filtros.
* Para cada tabela ou etapa de transformação, declare ou reutilize dataclasses imutáveis `@dataclass(frozen=True)` separando explicitamente as colunas de entrada (*Input/Bronze*) e as colunas de saída (*Output/Silver/Gold*).
* Isso garante rastreabilidade estrita (*lineage* de dados) e proteção contra erros tipográficos em tempo de desenvolvimento.

### Exemplos:
* ❌ **Evite**:
  ```python
  transformed_dataframe = raw_dataframe.select(
      try_cast("id", "integer").alias("id_filme"),
      sanitize_numeric_metric("popularity", "double").alias("popularidade")
  )
  ```
* ✅ **Prefira**:
  ```python
  @dataclass(frozen=True)
  class RawMoviesMetricsColumns:
      ID: str = "id"
      POPULARITY: str = "popularity"

  @dataclass(frozen=True)
  class TbMetricasEngajamentoSilverColumns:
      MOVIE_ID: str = "id_filme"
      POPULARITY: str = "popularidade"
  ```

---

## 14. Ciclo de Vida do Timestamp de Ingestão (`ingestion_datetime`)

* A coluna `ingestion_datetime` é gerada **uma única vez** no momento da ingestão na camada **Bronze** (via `write_dataframe_with_timestamp` na ingestão Landing ➔ Bronze).
* Nas camadas subsequentes (**Silver** e **Gold**), o `ingestion_datetime` gerado na Bronze **deve ser preservado e repassado** através das projeções (`.select(...)`).
* A função de persistência nas camadas Silver e Gold deve ser exclusivamente `write_dataframe(dataframe, target_path)` (salvando em Parquet sem injetar novos timestamps artificiais).

---

## 15. Formatação e Legibilidade de Transformações (Expression Preparation First)

* Para manter alta legibilidade e evitar chamadas aninhadas profundas e confusas dentro de `.select(...)` ou `.withColumn(...)`, estruture as transformações em 3 etapas sequenciais e claras:
  1. **Atribuição das variáveis de coluna de entrada** (`col_input_... = col(...)`);
  2. **Preparação das expressões semânticas de transformação** (`expr_... = ...`);
  3. **Projeção limpa no `.select(...)`**.

### Exemplos:
* ❌ **Evite**:
  ```python
  transformed_dataframe = (
      deduplicated_dataframe.select(
          try_cast("id", "integer").alias(Columns.MOVIE_ID),
          sanitize_numeric_metric("popularity", "double", minimum_value=0.0).alias(Columns.POPULARITY),
          sanitize_numeric_metric("vote_average", "double", minimum_value=0.0, maximum_value=10.0).alias(Columns.VOTE_AVERAGE)
      )
  )
  ```
* ✅ **Prefira**:
  ```python
  col_input_id = col(RawColumns.ID)
  col_input_popularity = col(RawColumns.POPULARITY)
  col_input_vote_average = col(RawColumns.VOTE_AVERAGE)

  expr_movie_id = try_cast(col_input_id, "integer").alias(SilverColumns.MOVIE_ID)
  expr_popularity = sanitize_numeric_metric(col_input_popularity, "double", minimum_value=0.0).alias(SilverColumns.POPULARITY)
  expr_vote_average = sanitize_numeric_metric(col_input_vote_average, "double", minimum_value=MIN_RATING, maximum_value=MAX_RATING).alias(SilverColumns.VOTE_AVERAGE)

  transformed_dataframe = deduplicated_dataframe.select(
      expr_movie_id,
      expr_popularity,
      expr_vote_average
  )
  ```

---

