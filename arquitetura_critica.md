# Revisão Arquitetural Crítica (Anti-Patterns e Gafes)

Você pediu para ser "chato" e minucioso. Analisando o seu código friamente sob a ótica de padrões de Engenharia de Dados em empresas *Big Tech* ou projetos *Enterprise*, existem vários **anti-patterns** graves que precisam ser corrigidos.

Aqui estão os principais problemas de arquitetura que ferem as boas práticas:

---

## 1. Airflow: Modificação Global do `sys.path` (A Famosa "Gambiarra")
Em todos os seus arquivos DAG (ex: `matches_ingestion_bronze_dag.py`), logo no topo, você tem:
```python
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)
```
> [!WARNING]
> **O Erro:** Mudar o `sys.path` dinamicamente dentro de um arquivo DAG é um anti-pattern fortíssimo. O *Scheduler* do Airflow avalia os arquivos DAG a cada 30 segundos. Fazer manipulação do path do sistema em tempo de execução pode causar conflitos bizarros de módulos entre DAGs diferentes, além de vazamento de memória.
> **A Solução:** O código deve ser empacotado como um pacote Python (`setup.py` ou `pyproject.toml`) e instalado no ambiente do Airflow, ou a variável `PYTHONPATH` deve ser configurada globalmente no `.env` do Docker, apontando para a raiz do repositório. O arquivo DAG nunca deve saber sobre diretórios do SO.

---

## 2. Airflow: Uso de Classes Python para Queries de Banco de Dados
No seu `silver_gold_match_dag.py`, você faz isso para criar views e schemas:
```python
def setup_database():
    from src.utils.db_handler import DBHandler
    db_handler = DBHandler()
    db_handler.execute_query("CREATE SCHEMA IF NOT EXISTS gold;")
```
> [!WARNING]
> **O Erro:** Usar o `PythonOperator` (ou o decorator `@task`) para abrir uma conexão de banco com uma classe customizada (como seu `DBHandler`) ignora completamente o gerenciamento nativo de conexões do Airflow. Você perde connection pooling, tracking de credenciais seguras na UI do Airflow (Admin > Connections), e os logs não mostram a query formatada.
> **A Solução:** Use o padrão da ferramenta. Substitua isso pelo `SQLExecuteQueryOperator` (ou `PostgresOperator`), passando a `conn_id`. Deixe o Airflow gerenciar a conexão, não uma classe utilitária do seu código.

---

## 3. Data Lake (MinIO): Violação de Imutabilidade e Idempotência
No `bot/ranking_extractor.py`, sua lógica baixa o CSV atual do MinIO, lê pra memória, dá um "append" (`csv.DictWriter`) na memória e sobrescreve o arquivo no MinIO.
> [!CAUTION]
> **O Erro (Grave):** Em Data Lakes, arquivos em Object Storage (S3/MinIO) são considerados **imutáveis**. Fazer *append* num arquivo existente baixando-o e subindo de novo gera **Race Conditions** severas. Se o Airflow rodar essa task duas vezes ao mesmo tempo (por um clear ou backfill), um arquivo vai sobrescrever o outro de forma corrompida.
> **A Solução:** Extrações incrementais devem gravar arquivos **novos**. Exemplo: `raw/ranking/dt=2026-09-15/data.csv`. Nunca se faz update em arquivo cru. Quem cuida de deduplicar e juntar os dados novos com os antigos é a camada Bronze no Spark, e não o bot na extração.

---

## 4. PySpark -> PostgreSQL: Destruição Silenciosa de Metadados (`mode="overwrite"`)
Nas camadas Gold (ex: `dim_date.py`), ao salvar no Postgres, você usa:
```python
df.write.format("jdbc").mode("overwrite").save()
```
> [!CAUTION]
> **O Erro (Grave):** O comportamento padrão do `mode("overwrite")` no conector JDBC do Spark não é dar um "TRUNCATE". Ele faz um **`DROP TABLE` e um `CREATE TABLE`**. Isso significa que se um DBA criou índices, chaves primárias (PK), chaves estrangeiras (FK), ou deu permissões de leitura (GRANT) na sua tabela `dim_date`, **o Spark vai apagar e destruir tudo isso toda vez que a DAG rodar**.
> **A Solução:** Bancos relacionais não devem ter seu DDL (schema) gerenciado pelo Spark. Crie as tabelas com uma ferramenta de migração (Flyway, Alembic, dbt). No Spark, você deve usar `mode="append"` aliado à opção `.option("truncate", "true")` (se quiser limpar a tabela) ou fazer um *UPSERT* jogando os dados numa tabela temporária (`staging_dim`) e chamando uma procedure de `MERGE` no Postgres.

---

## 5. Spark: Acoplamento e Hardcode de Infraestrutura
Nos scripts PySpark (`tb_atp_matches.py`):
```python
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
```
> [!WARNING]
> **O Erro:** Scripts de transformação de dados (sua lógica de negócio) estão sabendo *onde* e *como* estão rodando. Se amanhã você subir isso pra um cluster Databricks ou EMR na AWS, esse hardcode vai quebrar o cluster. 
> **A Solução:** O código Spark não deve ter `.environ` de IPs nem criação manual de `PySparkHandler` chumados. O `SparkSession` deve vir de fora, e as configurações de cluster devem ser injetadas via `spark-submit` ou pelo arquivo `spark-defaults.conf`. 

---

## 6. Arquitetura do Bot: O Extractor faz coisa demais (SoC)
Ainda no `ranking_extractor.py`, a função não apenas raspa os dados com `BeautifulSoup`, mas também chama `s3_client.copy_object` e `delete_object` para renomear e arquivar planilhas de anos anteriores (`previous_year_process`).
> [!WARNING]
> **O Erro:** Fere o princípio do *Separation of Concerns* (Separação de Conceitos). O bot extrator agora também é gerente de Data Lake. Isso torna a task da DAG uma caixa-preta que faz movimentação de dados ocultos.
> **A Solução:** O bot só deve ter UMA função: extrair do HTML e salvar o novo CSV num canto. A lógica de pegar arquivos velhos, arquivar (historical) ou copiar no MinIO deveria ser um fluxo visível (Tasks dedicadas) desenhado no Airflow, ou o Spark deveria ler tudo e particionar corretamente por ano dentro da camada Bronze, sem precisar mover fisicamente arquivos brutos.

---

## 7. Dependência Global do `load_dotenv()` (Env Variables)
Você chama `load_dotenv()` em praticamente todos os arquivos (`tb_atp_matches.py`, `ranking_extractor.py`, etc).
> [!WARNING]
> **O Erro:** Num ambiente de produção distribuído (onde o Airflow roda num servidor e o Spark roda em várias máquinas diferentes num cluster), não existe um arquivo `.env` físico nos computadores "workers".
> **A Solução:** O Airflow e o Spark têm sistemas próprios de gerenciar variáveis. O Airflow usa as *Airflow Variables* ou *Connections*, e o Spark aceita parâmetros na hora de submeter o job (`spark-submit --conf`). As credenciais deveriam vir orquestradas de cima para baixo.

---

## 8. Airflow: Acionando Views via String SQL (`f"CREATE OR REPLACE..."`)
No `silver_gold_match_dag.py`, você faz um loop num array `tables = ["dim_date", "dim_entry", ...]` e executa uma string formatada no banco.
> [!WARNING]
> **O Erro:** Gerenciar DDL (esquema do banco) via laço `for` e strings mágicas dentro de um DAG de Airflow torna o versionamento do banco de dados nulo. Se a view der erro, é mais chato fazer o debug de qual query exata falhou.
> **A Solução:** Consultas de estruturação do banco de dados (DDL) deveriam estar em arquivos `.sql` reais e versionados, controlados por ferramentas como o **dbt** (Data Build Tool), **Flyway**, ou pelo menos serem chamadas a partir de arquivos físicos `.sql` através do `SQLExecuteQueryOperator`.

---

## 9. Inversão de Controle: Identificando o `initial_run` na Camada Bronze
Em `tb_atp_matches.py`, você instanciou o cliente S3 e checou os objetos para decidir se é `initial_run`.
> [!WARNING]
> **O Erro:** Um script de transformação (ETL) no Spark não deve ter que "investigar" a infraestrutura via `boto3` para saber a operação lógica que ele deve realizar. Isso gera acoplamento e tira do orquestrador (Airflow) a visibilidade do processo.
> **A Solução:** O Airflow, como cérebro da operação, é quem sabe se o sistema está rodando pela primeira vez. O DAG deveria repassar um argumento/parâmetro (ex: `--is_initial_run=True`) para o script PySpark através da chamada. O Spark tem que ser puramente reativo.

---

## 10. `TriggerDagRunOperator` com `reset_dag_run=True`
Nos DAGs da camada bronze, ao chamar o `TriggerDagRunOperator` para engatilhar os DAGs Silver/Gold, a flag `reset_dag_run=True` foi habilitada.
> [!WARNING]
> **O Erro:** Habilitar essa flag faz com que toda vez que o DAG de ingestão acionar o processamento para uma mesma data, o Airflow apague violentamente o histórico da execução anterior (State = CLEAR). Isso destrói a trilha de auditoria e métricas de retentativas.
> **A Solução:** Remova o `reset_dag_run` e permita que instâncias subsequentes passem a ter um `trigger_run_id` customizado (com timestamp) para não conflitar com execuções passadas da mesma logical_date.
