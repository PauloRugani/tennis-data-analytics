
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

## 3. Data Lake (MinIO): Violação de Imutabilidade e Idempotência
No `bot/ranking_extractor.py`, sua lógica baixa o CSV atual do MinIO, lê pra memória, dá um "append" (`csv.DictWriter`) na memória e sobrescreve o arquivo no MinIO.
> [!CAUTION]
> **O Erro (Grave):** Em Data Lakes, arquivos em Object Storage (S3/MinIO) são considerados **imutáveis**. Fazer *append* num arquivo existente baixando-o e subindo de novo gera **Race Conditions** severas. Se o Airflow rodar essa task duas vezes ao mesmo tempo (por um clear ou backfill), um arquivo vai sobrescrever o outro de forma corrompida.
> **A Solução:** Extrações incrementais devem gravar arquivos **novos**. Exemplo: `raw/ranking/dt=2026-09-15/data.csv`. Nunca se faz update em arquivo cru. Quem cuida de deduplicar e juntar os dados novos com os antigos é a camada Bronze no Spark, e não o bot na extração.

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