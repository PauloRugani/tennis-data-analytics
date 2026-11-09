from pyspark.sql import functions as f

_ACCENTED = "ÀÁÂÃÄÅàáâãäåÈÉÊËèéêëÌÍÎÏìíîïÒÓÔÕÖØòóôõöøÙÚÛÜùúûüÝýÿÑñÇçŠšŽžĆćČčĐđŁłŃńŚśŹźŻż"
_UNACCENTED = "AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOOooooooUUUUuuuuYyyNnCcSsZzCcCcDdLlNnSsZzZz"


def normalize_player_name(col):
    col = f.col(col) if isinstance(col, str) else col
    sem_hifen = f.regexp_replace(col, r"-", " ")
    sem_pontuacao = f.regexp_replace(sem_hifen, r"[^a-zA-ZÀ-ÿ\s]", "")
    return f.trim(f.regexp_replace(sem_pontuacao, r"\s+", " "))


def player_name_key(col):
    col = f.col(col) if isinstance(col, str) else col
    return f.regexp_replace(f.lower(f.translate(col, _ACCENTED, _UNACCENTED)), r"[^a-z]", "")
