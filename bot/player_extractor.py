import io
import logging
import boto3
from botocore.client import Config
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

URL = "https://raw.githubusercontent.com/Tennismylife/TML-Database/master/ATP_Database.csv"
DESTINATION = "raw/incremental/tb_players.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)

def extract_bot(conn_vars):
    s3_client = boto3.client(
        's3',
        endpoint_url=conn_vars["bucket_endpoint"],
        aws_access_key_id=conn_vars["bucket_access_key"],
        aws_secret_access_key=conn_vars["bucket_secret_key"],
        config=Config(signature_version='s3v4')
    )

    bucket = conn_vars["bucket_name"]
    try:
        s3_client.head_bucket(Bucket=bucket)
    except:
        s3_client.create_bucket(Bucket=bucket)

    try:
        request = Request(URL, headers={"User-Agent": "tennis-data-analytics"})
        with urlopen(request, timeout=120) as response:
            if response.status != 200:
                raise RuntimeError(f"Download falhou com HTTP {response.status}")
            
            file_bytes = response.read()

        if len(file_bytes) == 0:
            raise RuntimeError("O arquivo baixado está vazio")

        s3_client.upload_fileobj(io.BytesIO(file_bytes), bucket, DESTINATION)
        logger.info("Arquivo ATP_Database.csv salvo em s3://%s/%s (%s bytes)", bucket, DESTINATION, len(file_bytes))
        
        return DESTINATION

    except (HTTPError, URLError, RuntimeError) as error:
        logger.error("Não foi possível baixar o banco ATP: %s", error)
        raise

def run_ingestion(conn_vars):
    logger.info("[Airflow] Download ATP database starts...")
    extract_bot(conn_vars)

if __name__ == "__main__":
    extract_bot()
