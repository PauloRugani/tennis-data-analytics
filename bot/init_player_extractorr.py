import logging
import os
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

URL = "https://raw.githubusercontent.com/Tennismylife/TML-Database/master/ATP_Database.csv"
DESTINATION = "raw/incremental/tb_players.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)

def extract_bot():
    try:
        request = Request(URL, headers={"User-Agent": "tennis-data-analytics"})
        with urlopen(request, timeout=120) as response:
            if response.status != 200:
                raise RuntimeError(f"Download falhou com HTTP {response.status}")

            file_bytes = response.read()

        if len(file_bytes) == 0:
            raise RuntimeError("O arquivo baixado está vazio")

        os.makedirs(os.path.dirname(DESTINATION), exist_ok=True)
        with open(DESTINATION, "wb") as f:
            f.write(file_bytes)

        logger.info("Arquivo ATP_Database.csv salvo em %s (%s bytes)", DESTINATION, len(file_bytes))

        return DESTINATION

    except (HTTPError, URLError, RuntimeError) as error:
        logger.error("Não foi possível baixar o banco ATP: %s", error)
        raise

if __name__ == "__main__":
    extract_bot()