import pandas   as pd
import logging
from sqlalchemy import create_engine, text
from urllib.parse import quote
import oracledb as od

from sqlalchemy import create_engine, text
from sqlalchemy.dialects.oracle import VARCHAR2, NUMBER
from sqlalchemy.types import DateTime, String, Integer, Boolean
from urllib.parse import quote

import logging

class DatabaseManager:
    def __init__(self, login, password, scheme, host, port):
        self.login = login
        self.password = password
        self.scheme = scheme
        self.host = host
        self.port = port
        self.engine = self.create_engine()

    def create_engine(self):
        encoded_password = quote(self.password)
        try:
            database_url = f"oracle+oracledb://{self.login}:{encoded_password}@{self.host}:{self.port}/?service_name={self.scheme}"
        except Exception as e:
            print(e)
        return create_engine(database_url)

    def execute_query(self, sql_query, **kwargs):
        try:
            with self.engine.connect() as connection:
                trans = connection.begin()
                try:
                    sql_query = text(sql_query)
                    connection.execute(sql_query, **kwargs)  # For parameterized queries
                    logging.info('✅ Executed')
                    trans.commit()
                    logging.info('✅ Committed')
                except Exception as e:
                    trans.rollback()
                    logging.error(f"❌💀 An error occurred: {e}")
                    raise  # Optionally re-raise the exception after logging it
                finally:
                    if connection is not None:
                        connection.close()
        except Exception as e:
            logging.error(f"❌💀 An error occurred: {e}")


    def fetch_data(self, query, **kwargs):
        """
        Fetches data from the database and returns it as a Pandas DataFrame.
        
        Args:
            sql_query (str): The SELECT SQL query to execute.
            **kwargs: Additional keyword arguments to pass to pandas.read_sql_query().

        Returns:
            DataFrame: The result set of the query as a Pandas DataFrame.
        """
        try:
            # Use pandas.read_sql_query to execute the query and fetch the result directly into a DataFrame
            df = pd.read_sql_query(sql=query, con=self.engine, **kwargs)
            logging.info('✅ Fetched')
            return df
        except Exception as e:
            logging.info(f"❌ An error occurred while fetching data: {e}")
            return None