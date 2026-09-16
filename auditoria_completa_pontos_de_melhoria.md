## Sumário Executivo — Os 8 mais urgentes

| 8 | SK's (surrogate keys) das dimensões são gerados com `monotonically_increasing_id()` sobre uma tabela **recriada do zero (`mode=overwrite`) a cada execução** — chave não é estável e tipos de `SK_DATE` divergem entre fato (string) e dimensão (int) | 🟠 |

---

## 1. 🔴 Bugs confirmados (vão quebrar em produção)

2. **`get_object` sem tratamento de `NoSuchKey`.** Em [bot/ranking_extractor.py](bot/ranking_extractor.py), depois de decidir que a data ainda não foi processada, o código faz `resp = s3_client.get_object(...)` **sem try/except**, assumindo que o arquivo incremental do ano já existe. Na primeira execução do ano (logo após `previous_year_process` arquivar o CSV do ano anterior), o objeto ainda não existe no bucket → `ClientError: NoSuchKey` não tratado, mesmo já tendo feito o scraping (dado raspado é perdido).
11. **Mistura de tipos entre fato e dimensão em `SK_DATE`.** Em [src/gold/fact/fact_player_season.py](src/gold/fact/fact_player_season.py) e [src/gold/fact/fact_player_tournament_stats.py](src/gold/fact/fact_player_tournament_stats.py), `SK_DATE` é calculado via `f.concat(f.substring(..., 1, 4), f.lit('0101'))`, que resulta em **string**. Em [src/gold/dimension/dim_date.py](src/gold/dimension/dim_date.py), `SK_DATE` é `f.date_format(...).cast("int")`. Ao gravar via JDBC (`mode=overwrite`, sem DDL controlado), o Postgres cria colunas com tipos diferentes (`text` vs `integer`) para a "mesma chave" em tabelas diferentes — joins no Power BI/SQL entre fato e `dim_date` por `SK_DATE` podem falhar ou exigir cast manual.

## 2. 🟠 Falhas silenciosas e tratamento de erro inadequado

21. Uso de `except:` (sem tipo) em pelo menos 3 lugares: `bot/match_extractor.py` (`head_bucket`/`create_bucket`), `bot/ranking_extractor.py` (idem) e `src/bronze/*.py` (decisão de `init_run`). Bare `except` captura até `KeyboardInterrupt`/`SystemExit`, mascarando qualquer causa real do erro.

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

## 6. 🟡 Qualidade de código / fora do padrão de desenvolvimento

58. Bloco JDBC de escrita (14 linhas: url, dbtable, user, password, driver, mode) **copiado e colado** em 8 arquivos gold diferentes, sem nenhuma função utilitária (`PySparkHandler` já existe e seria o lugar natural para um método `save_jdbc(df, table)`).

---

## 7. 🟡 Performance e escalabilidade

72. Cada execução baixa os pacotes Maven do Spark (`hadoop-aws`, `aws-java-sdk-bundle`, `postgresql` JDBC) via `spark.jars.packages` em vez de usar uma imagem/ambiente com esses jars pré-empacotados — soma minutos de latência por execução em CI (GitHub Actions) e possivelmente também no Airflow.

## 8. 🔵 Observabilidade, testes e monitoramento

87. Nenhum alerta configurado (Slack, e-mail, PagerDuty) em nenhuma DAG do Airflow nem nos workflows do GitHub Actions além do status nativo (verde/vermelho) da própria plataforma — falhas só são percebidas se alguém checar manualmente.
90. Sem testes de regressão para as transformações de silver/gold (ex.: garantir que `dim_tournaments` sempre produza exatamente 1 linha por `COD_TOURNEY_ID`, ou que `fact_player_match_stats` nunca tenha `SK_PLAYER` nulo) — essas invariantes são implícitas no código, não verificadas automaticamente.
