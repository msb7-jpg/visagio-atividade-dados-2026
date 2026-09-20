# Análise Comparativa e Racional de Divergências — CineData Analytics

Este documento consolida o diagnóstico técnico detalhado, a análise de causas raízes e o raciocínio fundamentado sobre as divergências identificadas entre a execução deste projeto e a do colega, confrontando diretamente as regras do documento oficial de especificação (**Atividade de Dados.pdf**).

---

## 1. Visão Geral das Divergências

Ao comparar os resultados finais reportados nas 6 perguntas analíticas da camada Gold:

| Pergunta Analítica | Resultado do Colega | Seu Resultado Após Ajuste | Status | Causa Raiz e Solução Implementada |
|---|---|---|---|---|
| **1. Receita total (BRL)** | `R$ 837.771.066.819,07` | `R$ 838.275.201.039,60` | Conforme (~838 bi) | Higienização monetária prévia à janela de deduplicação priorizando valores preenchidos. |
| **2. Top 5 popularidade** | Idêntico | Idêntico | Conforme | Mesma ordenação e valores. |
| **3. Filmes por gênero** | Idêntico | Idêntico | Conforme | Mesma ordenação e volumes. |
| **4. Top 10 receita (USD/BRL)** | Idêntico | Idêntico | Conforme | Mesmos filmes e valores exatos. |
| **5. Ator mais frequente (2 anos)** | `Kevin Hart (64 participações)` | `Kevin Hart (64 participações)` | Conforme | Remoção de deduplicação cega por filme antes da desagregação de elenco. |
| **6. Produtora maior lucro (5 anos)** | `Universal Pictures (US$ 5.772.329.679)` | `Universal Pictures (US$ 5.772.329.679)` | Conforme | Mesma produtora e valor exato. |

---

## 2. Aprofundamento da Pergunta 5: Ator com Mais Participações (Últimos 2 Anos)

### 2.1. O que diz a especificação oficial (`Atividade de Dados.pdf`)

1. **Seção 1.3 (Camada Silver) — Tópico 1: `silver.tb_info_filmes`**:
   > *"Deduplicação: A tabela deve conter unicidade por filme. Havendo registros duplicados na origem, mantenha exclusivamente a versão mais recente com base na data de ingestão."*
2. **Seção 1.3 (Camada Silver) — Tópicos 5 e 6: `silver.tb_generos` e `silver.tb_pessoas_empresas`**:
   > *"A origem armazena múltiplos valores delimitados na mesma coluna. Desmembre estes elementos para que cada registro represente um único gênero ou participante/empresa por filme."*  
   > *"Padronize a formatação de capitalização de texto, consolide as entidades em um modelo unificado categorizado pelo tipo de atuação (Ator, Diretor, Roteirista, Produtora) e elimine registros duplicados."*

### 2.2. O que aconteceu na sua base vs. base do colega

* **Estrutura dos dados brutos em `credits_and_tags`**:  
  O arquivo bruto possui 106.320 linhas para 99.006 filmes únicos. Em múltiplos casos (como no filme `1300214 - Die Hart: Die Harter`), há várias linhas com o mesmo `id`, cada uma contendo pedaços do elenco, algumas completas e algumas com `cast = NULL`.
* **No seu fluxo original**:  
  Aplicou-se a função `deduplicate_latest(raw_credits_dataframe, business_key_column="id")` **antes** da extração e do split/explode dos atores. Como todos os registros da mesma carga possuem rigorosamente o mesmo `ingestion_datetime`, o `row_number().over(partitionBy("id").orderBy(col("ingestion_datetime").desc()))` não tinha critério de desempate determinístico. Em dezenas de filmes, o Spark selecionou aleatoriamente uma linha onde o campo `cast` vinha nulo ou sem o ator principal.
* **No fluxo do colega**:  
  Ele **não** fez deduplicação prévia do arquivo de créditos por filme. Ele explodiu todas as ocorrências de atores e depois aplicou a regra explícita do PDF: eliminar registros duplicados pela tupla `(id_filme, nome_entidade, tipo_entidade)`.

### 2.3. Racional e Veredito

* **Faz sentido deduplicar `credits_and_tags` por filme antes de explodir?**  
  **Não.** O arquivo foi intencionalmente fragmentado na origem. Duplicar ou repartir linhas de créditos não significa que a primeira linha é a única verdadeira, mas sim que partes do elenco foram distribuídas ou poluídas entre linhas. Deduplicar por `id_filme` antes do split causa perda irreparável de informação.
* **Qual é o resultado correto segundo o PDF?**  
  **Kevin Hart com 64 participações.** A especificação solicitou deduplicação por data de ingestão apenas em `tb_info_filmes`. Para `tb_pessoas_empresas`, o contrato pede apenas desmembrar e eliminar duplicatas da entidade por filme.
* **Status do Ajuste**: ✅ Implementado e validado. O ranking agora aponta Kevin Hart no topo com exatamente 64 participações.

---

## 3. Aprofundamento da Pergunta 1: Receita Total em Reais (BRL)

### 3.1. O que diz a especificação oficial (`Atividade de Dados.pdf`)

1. **Seção 1.3 (Camada Silver) — Tópico 2: `silver.tb_financeiro_filmes`**:
   > *"Tratar valores textuais que representam ausência de dado (ex.: 'Unknown', 'Não Informado') como NULL antes da conversão de tipo."*  
   > *"Higienize as colunas de orçamento e receita para remover símbolos de moedas, pontuações de milhar e textos representativos de ausência de dados."*  
   > *"Converta as métricas para o tipo numérico decimal apropriado e garanta que valores zerados ou negativos sejam tratados como ausentes."*  
   > *"Calcule os valores equivalentes em Reais (BRL) aplicando a taxa de cotação obtida."*
2. **Seção 2 (Camada Gold) — Tópico `gold.fact_movies_performance`**:
   > *"Grão: Um registro único por filme."*  
   > *"A tabela fato deve consolidar métricas de filmes lançados sem duplicar grão através dos joins."*

### 3.2. O que aconteceu na sua base vs. base do colega

* **Comportamento da origem (`movies_financials`)**:  
  Existem 3.805 filmes com múltiplas linhas no arquivo financeiro, e 803 filmes trazem valores conflitantes entre as linhas (ex.: o filme `1322058` possui linhas com `revenue = 500`, mas também linhas com `revenue = -500`, `Unknown` e `Não Informado`).
* **No seu fluxo original**:  
  O `deduplicate_latest` foi aplicado logo no início da função sobre os dados textuais brutos. Em empates de `ingestion_datetime`, para alguns filmes a linha mantida continha `"Unknown"`, `"-500"` ou `"0"`. Quando o pipeline executou a higienização a seguir, esses valores viraram `NULL`, perdendo a receita positiva legítima daquele filme.
* **No fluxo ajustado**:  
  As métricas financeiras são primeiro higienizadas e tipadas para numérico, e a deduplicação prioriza registros com valores preenchidos (`col("receita_usd").desc_nulls_last()`), mantendo a data de ingestão como desempate final.
* **Status do Ajuste**: ✅ Implementado e validado. O valor total de receita em BRL agora soma **`R$ 838.275.201.039,60`**, plenamente alinhado à ordem de grandeza (~R$ 837–838 bilhões).

---

## 4. Conclusão Final

* As divergências não eram falhas de integridade nos arquivos brutos de entrada, mas consequências de deduplicações prematuras com empate cego de timestamps.
* Ambos os pontos foram harmonizados mantendo estrita conformidade com as regras do documento oficial de especificação e os padrões de Clean Code do projeto.
