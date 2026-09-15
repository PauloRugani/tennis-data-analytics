# Auditoria Completa — Pontos de Melhoria

> Este documento complementa [analise_arquitetura.md](analise_arquitetura.md) (visão geral positiva) e [arquitetura_critica.md](arquitetura_critica.md) (10 anti-patterns já mapeados). Aqui vai uma varredura linha a linha de **todo** o repositório (`bot/`, `dags/`, `src/`, `.github/workflows/`, configs e repositório em si), listando **todos** os pontos encontrados — bugs confirmados, riscos silenciosos, desvios de padrão e melhorias de qualidade — sem filtrar por "relevância". Itens estão numerados sequencialmente e marcados por severidade:
>
> - 🔴 **Crítico** — vai quebrar em produção ou já está quebrado / risco de perda de dados.
> - 🟠 **Alto** — falha silenciosa, risco de segurança ou inconsistência de dados.
> - 🟡 **Médio** — anti-pattern, dívida técnica, ou algo fora do padrão de mercado.
> - 🔵 **Baixo** — sugestão de melhoria, boas práticas, organização.

---

## Sumário Executivo — Os 8 mais urgentes

| # | Item | Severidade |
|---|------|------------|
| 5 | `except:` genérico usado para decidir `init_run` em `src/bronze/tb_atp_matches.py` e `tb_atp_ranking.py` — qualquer erro (rede, credencial) é interpretado como "primeira carga" e pode **sobrescrever histórico inteiro** | 🔴 |
| 8 | SK's (surrogate keys) das dimensões são gerados com `monotonically_increasing_id()` sobre uma tabela **recriada do zero (`mode=overwrite`) a cada execução** — chave não é estável e tipos de `SK_DATE` divergem entre fato (string) e dimensão (int) | 🟠 |

---

## 1. 🔴 Bugs confirmados (vão quebrar em produção)

2. **`get_object` sem tratamento de `NoSuchKey`.** Em [bot/ranking_extractor.py](bot/ranking_extractor.py), depois de decidir que a data ainda não foi processada, o código faz `resp = s3_client.get_object(...)` **sem try/except**, assumindo que o arquivo incremental do ano já existe. Na primeira execução do ano (logo após `previous_year_process` arquivar o CSV do ano anterior), o objeto ainda não existe no bucket → `ClientError: NoSuchKey` não tratado, mesmo já tendo feito o scraping (dado raspado é perdido).
4. **`except:` genérico decidindo carga inicial.** Em [src/bronze/tb_atp_matches.py](src/bronze/tb_atp_matches.py) e [src/bronze/tb_atp_ranking.py](src/bronze/tb_atp_ranking.py), a função `run()` faz:
   ```python
   try:
       handler.load_data(...)  # tenta ler o bronze existente
       init_run = False
   except:
       init_run = True
   ```
   Qualquer exceção — timeout de rede, credencial errada, bucket temporariamente indisponível, permissão — é interpretada como "esta é a primeira carga", e o pipeline reprocessa `historical_matches`/`historical_ranking` com `mode="overwrite"`. Um problema transitório de infraestrutura pode **apagar o incremento acumulado até então**.
5. **Falha silenciosa de scraping tratada como "sem novidade".** Em [bot/ranking_extractor.py](bot/ranking_extractor.py): `if not ranking: print("No data"); return None`. Se o site mudar o HTML (os seletores `.lower_row, .lower-row, tr.lower-row, tr.lower_row` são muito específicos e frágeis) ou bloquear o scraper (403/429), a lista fica vazia e a função simplesmente retorna `None` **sem lançar exceção** — a task do Airflow/GitHub Actions é marcada como sucesso mesmo sem nenhum dado novo ter sido coletado. Não há alerta, nem contagem mínima esperada de linhas.
7. **`f.last(..., ignorenulls=True)` esquecido.** Em [src/silver/tb_atp_player_match.py](src/silver/tb_atp_player_match.py), o preenchimento de IDs ausentes usa:
   ```python
   f.last("winner_id").over(Window.partitionBy("winner_name").orderBy(...).rowsBetween(unboundedPreceding, unboundedFollowing))
   ```
   sem `ignorenulls=True`. Se a última linha da partição (por ordenação de `tourney_date`) tiver `winner_id` nulo, o `last()` retorna `NULL` em vez de buscar o último valor não nulo — o objetivo de "recuperar o ID em outro registro do mesmo jogador" falha silenciosamente. Em outros arquivos do próprio projeto (ex. [src/gold/dimension/dim_players.py](src/gold/dimension/dim_players.py)) o padrão correto `ignorenulls=True` é usado — inconsistência entre arquivos.
9. **Geração não determinística de `match_num`.** Quando `match_num` é nulo, o código usa `f.row_number().over(Window.partitionBy("tourney_id").orderBy(tourney_date, match_num.asc_nulls_last()))`. Quando há múltiplas linhas com o mesmo `tourney_date` e `match_num` nulo (empate no critério de ordenação), o Spark pode atribuir `row_number()` em ordens diferentes entre execuções distribuídas, mudando o `COD_MATCH_ID` (`tourney_id-match_num`) de um dia para o outro para essas partidas — quebrando a rastreabilidade histórica dessas linhas nas fatos.
10. **Contagem de validação pode falhar por motivo errado.** Em [src/gold/fact/fact_player_match_stats.py](src/gold/fact/fact_player_match_stats.py): `if tb_player_match.count() != df.count(): raise Exception(...)`. Depois de dois `LEFT JOIN` (que podem gerar fan-out) seguido de `.distinct()`, uma divergência de contagem é esperada em cenários legítimos (múltiplos jogadores com mesmo ID antigo mapeando para o mesmo `SK_PLAYER`, por exemplo) — o teste é rígido demais e pode derrubar o pipeline por um motivo diferente do que a mensagem de erro sugere (mensagem genérica, sem diagnóstico de quais linhas divergem).
11. **Mistura de tipos entre fato e dimensão em `SK_DATE`.** Em [src/gold/fact/fact_player_season.py](src/gold/fact/fact_player_season.py) e [src/gold/fact/fact_player_tournament_stats.py](src/gold/fact/fact_player_tournament_stats.py), `SK_DATE` é calculado via `f.concat(f.substring(..., 1, 4), f.lit('0101'))`, que resulta em **string**. Em [src/gold/dimension/dim_date.py](src/gold/dimension/dim_date.py), `SK_DATE` é `f.date_format(...).cast("int")`. Ao gravar via JDBC (`mode=overwrite`, sem DDL controlado), o Postgres cria colunas com tipos diferentes (`text` vs `integer`) para a "mesma chave" em tabelas diferentes — joins no Power BI/SQL entre fato e `dim_date` por `SK_DATE` podem falhar ou exigir cast manual.
12. **Chaves naturais frágeis para idempotência (matches).** Em [src/bronze/tb_atp_matches.py](src/bronze/tb_atp_matches.py), a detecção de "linhas novas" é feita com join por `(tourney_date, winner_name, loser_name)`. Nomes de jogadores raspados podem variar (acentuação, abreviação, grafias diferentes — o próprio código já tem *patches* manuais para isso, ver item 27). Se o nome vier ligeiramente diferente entre execuções, a mesma partida pode ser inserida **duas vezes** no bronze (quebra de idempotência que o append-only deveria garantir).
13. Mesmo problema em [src/bronze/tb_atp_ranking.py](src/bronze/tb_atp_ranking.py): a chave de deduplicação incremental é `(date, name)` — nome como texto livre, sujeito a variações de grafia.

## 2. 🟠 Falhas silenciosas e tratamento de erro inadequado

21. Uso de `except:` (sem tipo) em pelo menos 3 lugares: `bot/match_extractor.py` (`head_bucket`/`create_bucket`), `bot/ranking_extractor.py` (idem) e `src/bronze/*.py` (decisão de `init_run`). Bare `except` captura até `KeyboardInterrupt`/`SystemExit`, mascarando qualquer causa real do erro.
22. Em quase todos os módulos de `src/bronze`, `src/silver`, `src/gold` o padrão é `except Exception as e: print(e); raise`. Isso funciona, mas usa `print()` em vez de um logger estruturado — não há como filtrar por nível (INFO/WARNING/ERROR) nem correlacionar logs entre execuções no ambiente do Spark/Airflow.
23. Nenhum script valida `df_final.count() > 0` antes de sobrescrever a camada gold (dimensões e fatos) — só o bronze faz essa checagem (`if df_final.count() > 0: handler.save_data(...)`). Se uma dimensão ou fato resultar em DataFrame vazio (por exemplo, por uma falha upstream que retorna 0 linhas), o `mode="overwrite"` vai **truncar a tabela de produção com um resultado vazio**, tanto no Parquet quanto no Postgres via JDBC.
24. `previous_year_process` em [bot/ranking_extractor.py](bot/ranking_extractor.py) faz `copy_object` seguido de `delete_object` sem transação: se o processo cair entre as duas chamadas, o arquivo antigo é perdido (existe cópia, mas o original não foi removido — não é destrutivo nesse caso), mas se `copy_object` falhar silenciosamente e o código não verificar o resultado antes do `delete_object`... na implementação atual o `try/except` cobre ambas chamadas e propaga erro, então correto; mas não há verificação de que a cópia foi bem-sucedida antes do delete (apenas confia na ausência de exceção).

---

## 3. 🟠 Idempotência, chaves frágeis e "patches" hardcoded

27. Múltiplos *patches* de dados codificados diretamente no Spark, espalhados em pelo menos 3 arquivos diferentes:
    - [src/silver/tb_atp_matches.py](src/silver/tb_atp_matches.py) e [src/silver/tb_atp_player_match.py](src/silver/tb_atp_player_match.py): `loser_id` fixo para "Alejandro Davidovich Fokina" (`200221`), "Shevchenko" (`207686`), "Jesper De Jong" (`207411`); `tourney_id` fixo `2026-416` → `2026-308` quando `tourney_name == 'Munich'`.
    - [src/silver/tb_atp_players.py](src/silver/tb_atp_players.py): substituição hardcoded de nome para "Shevchenko" → "Aleksandr Shevchenko".
    - [src/silver/tb_atp_tournaments.py](src/silver/tb_atp_tournaments.py) e [src/gold/dimension/dim_tournaments.py](src/gold/dimension/dim_tournaments.py): `tourney_id == '2026-416'` mapeado para nome/nível fixos ("Rome Masters", nível "M").
    Isso é duplicação da mesma correção em lugares diferentes (viola DRY), acopla regra de negócio à camada de transformação, e cada nova inconsistência do site-fonte exige um novo deploy de código Spark. O correto seria uma tabela de "de-para"/reconciliação (arquivo de referência versionado ou tabela de staging) consultada dinamicamente.
28. Anos e IDs de torneio ficam **hardcoded com o ano atual** (`2026-416`, `2026-308`) — no próximo ano os IDs do site mudam e o patch se torna código morto (ou pior, aplica-se erroneamente a um torneio diferente que reuse o mesmo ID sequencial).
29. `previous_year_process` (arquivamento do CSV incremental de ranking do ano anterior) só roda quando `current_monday.month == 1 and current_monday.day <= 7`: se a DAG/workflow não rodar exatamente nessa janela (falha, pausa, backfill fora do prazo), o arquivo do ano anterior nunca é arquivado — não há mecanismo de recuperação/retry para esse caso específico além da próxima segunda-feira de janeiro do ano seguinte.

---

## 4. 🟠 Segurança

32. Credenciais (`DB_PASSWORD`, `MINIO_SECRET_KEY`, `GITHUB_TOKEN`) ficam em arquivos `.env` (carregados via `load_dotenv()`), sem uso do gerenciador nativo de segredos do Airflow (Connections/Variables) nem de um secrets manager — como já apontado em [arquitetura_critica.md](arquitetura_critica.md#7-dependência-global-do-loaddotenv-env-variables), isso não escala para múltiplos workers/ambientes e fica mais fácil de vazar (ex.: `.env` acidentalmente commitado).
33. `.gitignore` ignora `.env`, o que é correto, mas **não há `.env.example`** no repositório documentando quais variáveis são necessárias — dificulta onboarding e aumenta a chance de alguém commitar um `.env` real por engano ao tentar documentar/testar.
37. Scraping de sites de terceiros (ATP Tour, tennismylife.org) sem `robots.txt`/termos de uso verificados no código — risco legal/de bloqueio de IP não tratado (fora do escopo técnico, mas vale documentar como risco de produto).
38. Nenhuma sanitização/parametrização nas queries executadas via `db_handler.execute_query(f"CREATE OR REPLACE VIEW vw_{table} AS SELECT * FROM gold.{table};")` — hoje `table` vem de uma lista fixa no código (baixo risco real), mas o padrão de montar SQL com f-string em vez de identificar objetos de forma segura (`psycopg2.sql.Identifier`) é uma prática arriscada que pode virar injeção de SQL se a lista um dia vier de configuração externa.

---

## 5. 🟡 Anti-patterns de Engenharia de Dados (complementares aos já listados em arquitetura_critica.md)

39. `sys.path.insert` dinâmico em todos os DAGs (já apontado em [arquitetura_critica.md](arquitetura_critica.md#1-airflow-modificação-global-do-syspath-a-famosa-gambiarra)) — reforço: isso também dificulta rodar testes unitários dos módulos `src/*` fora do container do Airflow, pois os imports (`from src.bronze import ...`) dependem desse ajuste de path só presente dentro dos arquivos DAG.
41. `os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'` hardcoded em **todos** os scripts de `src/bronze`, `src/silver` e `src/gold` (10+ arquivos) — já criticado em [arquitetura_critica.md](arquitetura_critica.md#5-spark-acoplamento-e-hardcode-de-infraestrutura); reforço: está duplicado em cada arquivo individualmente (10+ cópias do mesmo hack) em vez de centralizado uma vez no `PySparkHandler`.
42. `mode="overwrite"` no conector JDBC para **todas** as tabelas gold (dimensões e fatos) — o `DROP TABLE`/`CREATE TABLE` destrutivo já é citado em [arquitetura_critica.md](arquitetura_critica.md#4-pyspark---postgresql-destruição-silenciosa-de-metadados-modeoverwrite); reforço: isso também significa que a **camada gold inteira é recalculada do zero a cada execução diária**, mesmo para dados históricos que nunca mudam (ex.: partidas de 1990) — sem particionamento incremental, sem `MERGE`/upsert, o custo computacional cresce indefinidamente à medida que o histórico aumenta.
43. `spark.jars.packages` (`hadoop-aws`, `aws-java-sdk-bundle`, `postgresql`) sendo baixado via Maven a cada início de sessão Spark (`PySparkHandler.init_spark_session`), sem cache de imagem/container com os jars pré-instalados — adiciona latência e dependência de rede (Maven Central) a cada execução de cada script.
44. Cada script (`tb_atp_matches.py`, `tb_atp_player_match.py`, etc.) cria sua **própria SparkSession** (`PySparkHandler(app_name=...)`) e a derruba ao final (`handler.spark.stop()`). Isso significa 10 sessões Spark distintas sendo criadas e destruídas por execução completa do pipeline diário, cada uma pagando o custo de start-up (download de jars, alocação de memória) — não há reaproveisamento de sessão entre as etapas do mesmo `task_group`.
45. Nenhum uso de particionamento físico (`partitionBy`) ao gravar Parquet no bronze/silver/gold — todas as tabelas são gravadas como uma pasta plana, o que combinado com `mode=overwrite` do histórico completo gera arquivos pequenos e sem poda de partição (partition pruning) possível nas leituras.
46. 60 arquivos `part-*.snappy.parquet` para uma tabela de bronze de partidas (`data/bronze/tb_atp_matches/`) sugerem ausência de `coalesce()`/`repartition()` antes da escrita — problema clássico de "small files" que piora a performance de leitura no S3A/MinIO.
47. Nenhuma tag/label de ambiente (`dev`/`staging`/`prod`) nos buckets do MinIO nem nos schemas do Postgres — um único `MINIO_BUCKET` e um único banco aparentam ser usados tanto localmente quanto em produção (reforçado pelas pastas `data/test_historical/` e `data/test_incremental/` misturadas com `data/raw/historical/` na mesma árvore de diretórios).
49. Não há uso de `Airflow Variables`/`Connections` nem de `SLA`, `on_failure_callback` ou alertas (Slack/e-mail) em nenhuma DAG — falha de pipeline só é percebida ao checar a UI manualmente.
50. `TriggerDagRunOperator` com `wait_for_completion=False` — a DAG "bronze" é marcada como concluída com sucesso mesmo que o `silver_gold_*_dag` disparado falhe depois; não há correlação de status entre os dois DAGs pais/filhos.
51. `reset_dag_run=True` no `TriggerDagRunOperator` (item já citado em [arquitetura_critica.md](arquitetura_critica.md#10-triggerdagrunoperator-com-reset_dag_runtrue)) — reforço: combinado com o item 15/16 (duplicidade de scheduler), se o mesmo `logical_date` for disparado duas vezes (uma pelo Airflow, outra manualmente via GitHub Actions/backfill), o histórico de uma das execuções é apagado.

---

## 6. 🟡 Qualidade de código / fora do padrão de desenvolvimento

52. `import pandas as pd` presente em **todos** os módulos de `src/bronze`, `src/silver` e `src/gold` (15+ arquivos), mas `pandas` **nunca é usado** neles (tudo é feito com a API do PySpark) — import morto repetido em todo o projeto.
53. Zero *type hints* nas funções de `src/bronze`, `src/silver`, `src/gold` (contrastando com `src/utils/db_handler.py` e `pyspark_handler.py`, que usam tipagem parcial) — inconsistência de padrão dentro do próprio repositório.
55. Nomes de função **repetidos e colidentes** entre módulos: `run_ingestion()` é definido tanto em [bot/match_extractor.py](bot/match_extractor.py) quanto reimportado dentro do DAG como `run_ingestion` (task_group) e task (`task_get_file`), criando *shadowing* de nomes que dificulta leitura (`run_ingestion` significa coisas diferentes em escopos próximos dentro do mesmo arquivo DAG).
56. `run()` é o nome de função usado em **todo** módulo de bronze/silver/gold (16 arquivos) como ponto de entrada — funcional, mas sem um contrato/assinatura comum (não há classe base ou protocolo `Job`/`Step`), o que impede tratar os módulos de forma genérica (ex.: um orquestrador simples que itere sobre uma lista de jobs) sem repetir os `from src.X import Y; Y.run()` manualmente em cada DAG.
57. Duplicação quase idêntica de `load_tables`/`run_transformation`/`save_table`/`run()` entre os 5 arquivos de silver e os 8 arquivos de gold — mesma estrutura de try/except/print repetida dezenas de vezes; um decorator ou função utilitária genérica (`with_spark_job(app_name, load_fn, transform_fn, save_fn)`) reduziria drasticamente a duplicação.
58. Bloco JDBC de escrita (14 linhas: url, dbtable, user, password, driver, mode) **copiado e colado** em 8 arquivos gold diferentes, sem nenhuma função utilitária (`PySparkHandler` já existe e seria o lugar natural para um método `save_jdbc(df, table)`).
64. Loop `for table in tables: db_handler.execute_query(f"CREATE OR REPLACE VIEW ...")` reexecuta a mesma tarefa em série dentro de uma única task Airflow, sem paralelismo nem tratamento por tabela — se a view 2 de 4 falhar, as views 3 e 4 nunca são criadas naquela execução, mas a 1 já foi (execução parcial sem rollback).

---

## 7. 🟡 Performance e escalabilidade

69. Múltiplas chamadas a `.count()` (validação em `fact_player_match_stats.py`, `df_final.count() > 0` no bronze) disparam jobs Spark completos adicionais só para contar linhas — cada `.count()` é uma ação materializada independente, dobrando o tempo de execução em alguns pontos do pipeline.
72. Cada execução baixa os pacotes Maven do Spark (`hadoop-aws`, `aws-java-sdk-bundle`, `postgresql` JDBC) via `spark.jars.packages` em vez de usar uma imagem/ambiente com esses jars pré-empacotados — soma minutos de latência por execução em CI (GitHub Actions) e possivelmente também no Airflow.

## 8. 🔵 Observabilidade, testes e monitoramento

85. Logging feito 100% via `print()` — sem correlação de execução (nenhum `run_id`/`execution_date` incluído nas mensagens), sem níveis de severidade, e sem envio para uma ferramenta central de observabilidade (não há integração com CloudWatch, Grafana Loki, ELK, ou mesmo os logs nativos estruturados do Airflow).
87. Nenhum alerta configurado (Slack, e-mail, PagerDuty) em nenhuma DAG do Airflow nem nos workflows do GitHub Actions além do status nativo (verde/vermelho) da própria plataforma — falhas só são percebidas se alguém checar manualmente.
90. Sem testes de regressão para as transformações de silver/gold (ex.: garantir que `dim_tournaments` sempre produza exatamente 1 linha por `COD_TOURNEY_ID`, ou que `fact_player_match_stats` nunca tenha `SK_PLAYER` nulo) — essas invariantes são implícitas no código, não verificadas automaticamente.

---

## 10. Tabela-resumo priorizada (por onde começar)

| Prioridade | Itens | Ação recomendada |
|---|---|---|
| **Fazer agora (quebra produção)** | 1, 2, 3, 4, 5 | Corrigir bug de coluna `id`, decidir orquestrador único, corrigir nomes de view, adicionar env vars faltantes nos workflows |
| **Esta semana (risco de dado errado/perdido)** | 8–13, 21–26 | Trocar `except:` genérico por exceções específicas, revisar chaves de idempotência, adicionar validação de contagem mínima |
| **Este mês (segurança e governança)** | 31–38, 79–84 | Mover segredos para secrets manager, remover dados binários do Git (considerar `git filter-repo` + DVC/LakeFS), adicionar `.env.example` |
| **Backlog técnico (qualidade/performance)** | 39–78 | Extrair utilitários comuns (JDBC writer, Spark job runner), adicionar testes, configurar lint/format, avaliar incrementalidade da gold |
| **Contínuo (observabilidade/documentação)** | 85–96 | Logger estruturado, alertas, diagramas, changelog |

---

### Observação final

Este documento foi gerado por leitura completa de `bot/`, `dags/`, `src/` (bronze, silver, gold, utils), `.github/workflows/`, `requirements.txt`, `.gitignore` e `README.md`. A pasta `dashboard/` (Power BI `.pbip`/TMDL) e os `notebooks/*.ipynb` **não foram auditados em profundidade** neste documento — vale uma revisão específica futura para checar se há credenciais embutidas nos arquivos `.tmdl`/`.json` do modelo semântico e se os notebooks estão sincronizados com o código de produção em `src/`.
