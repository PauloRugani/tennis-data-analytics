from utils import DBHandler

def create_schemas():
    try: 
        db_handler = DBHandler()
        db_handler.execute_query("CREATE SCHEMA IF NOT EXISTS raw;")
        db_handler.execute_query("CREATE SCHEMA IF NOT EXISTS bronze;")
        db_handler.execute_query("CREATE SCHEMA IF NOT EXISTS silver;")
        db_handler.execute_query("CREATE SCHEMA IF NOT EXISTS gold;")
        return True, "Success"
    except Exception as e:
        return False, e