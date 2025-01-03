import sqlalchemy as sal
import pyodbc
from utils.get_config_file import get_sql_server_config


def sql_server_db_conn():
    try:
        sql_cred = get_sql_server_config()
        #logging.info('getting sql server credential config ')
        sql_server_engine = sal.create_engine(f"mssql+pyodbc://{sql_cred['host']}/{sql_cred['database']}?driver=ODBC+Driver+17+for+SQL+Server")
        sql_server_conn= sql_server_engine.connect()
        #logging.info('sucessfully connect to the sql_server_db')
        return sql_server_conn
    
    except Exception as e:
        print(e)
        #logging.info(f"Error connecting to SQL Server: {e}")