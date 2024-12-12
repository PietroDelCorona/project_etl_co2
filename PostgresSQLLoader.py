import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

class PostgresSQLLoader:
    def __init__(self):
        self.host = os.getenv('POSTGRES_HOST')
        self.port = os.getenv('POSTGRES_PORT')
        self.dbname = os.getenv('POSTGRES_DB')
        self.user = os.getenv('POSTGRES_USER')
        self.password = os.getenv('POSTGRES_PASSWORD')
    
    def load(self, data, table_name):
        connection = None
        try:
            connection = psycopg2.connect(
                host = self.host,
                port = self.port,
                dbname = self.dbname,
                user = self.user,
                password = self.password
            )
            cursor = connection.cursor()

            columns = ', '.join(data.columns)
            placeholders = ', '.join(['%s'] * len(data.columns))
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(f'{col} TEXT' for col in data.columns)}            
            );
            """
            cursor.execute(create_table_query)

            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            for _, row in data.iterrows():
                cursor.execute(insert_query, tuple(row))

            connection.commit()
            print(f"Dados carregados na tabela {table_name} com sucesso!")
        
        except Exception as e:
            print(f"Erro ao carregar os dados no PostgreSQL: {e}")
        finally:
            if connection:
                cursor.close()
                connection.close()



        