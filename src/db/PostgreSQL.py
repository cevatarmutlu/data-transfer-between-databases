#### Built-in Module ####
import logging

#### Installed Modules ####
import psycopg2 as py

#### Project Scripts ####
from src.db.IDB import IDB
from src.db.DBFormatEnum import DBFormatEnum

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PostgreSQL(IDB):
    """
        This class connect and insert data to PostgreSQL.

        Args:
            host: host
            port: port
            dbname: dbname
            user: user
            password: password
        
        Raises:
            TypeError: if `host`, `dbname`, `user`, `password` 
                is not instance of `str` and `port is not
                instance of `int` then raises `TypeError`.
            ValueError: if `host`, `dbname`, `user`, `password` 
                is empty and `port` is negative then raises `ValueError`.

    """

    FORMAT = DBFormatEnum.TUPLE

    def __init__(self, 
        host='localhost',
        port=5432,
        dbname='postgres',
        user='postgres',
        password='123'
    ):

        logger.debug(f'{self.__class__.__name__} class to be generated')
        logger.debug(f'Class parameters: host={host}, port={port}, dbname={dbname}, user={user}, password={password}' )

        try: 
            if  not isinstance(host, str) or \
                not isinstance(port, int) or \
                not isinstance(dbname, str) or \
                not isinstance(user, str) or \
                not isinstance(password, str):
                raise TypeError('Port parameter must be integer and other parameters must be string.')
            
            if  not host or \
                not dbname or \
                not user or \
                not password:
                raise ValueError('Postgre class parameters not must be empty.')
            
            if port < 0:
                raise ValueError('port parameter must be positive')
        
            self.auth = {
                "host": host,
                "port": port,
                "dbname": dbname,
                "user": user,
                "password": password
            }

            logger.info(f"{self.__class__.__name__} class generated successful")

        except TypeError as e:
            logger.error(str(e))
            raise
        except ValueError as e:
            logger.error(str(e))
            raise

    def connect(self):
        """
            Connect to PostgreSQL.
        """

        try:
            self.conn = py.connect(**self.auth)
        except Exception as e:
            logger.error(e)
            raise
        
        logger.info(f"{self.__class__.__name__} DB connection is successful")

    def fetch(self, query: str):
        """
            Fetch data from DB.

            Args:
                query: The SQL query that fetch the data.

            Returns:
                data (list): Constains the data.
            
            Raises:
                TypeError: if `query` is not instance of `str` then raises `TypeError`.
        """

        try:

            if type(query) != str:
                raise TypeError(f"PostgreSQL query argument must be string: query={query}, query type= {type(query).__name__}")

            cur = self.conn.cursor()

            cur.execute(query)
            data = cur.fetchall()

            cur.close()

            logger.info("Data Fetched successful")

            return data

        except TypeError as e:
            logger.error(str(e))
            raise
        except Exception as e:
            logger.error(str(e))
            raise

    
    def __generate_query(self, data: list):
        """
            Generate query suffix.

            Args:
                data: Data to be concatenation.
            
            Returns:
                query_suffix(str): Concatenated values
        """

        query_suffix = ''
        for row in data:
            query_suffix += f'{str(row)}, '
        
        query_suffix = query_suffix[:-2] + ';'

        return query_suffix

    def insert(self, data: list):
        """
            Inserts the values to PostgreSQL.

            Args.
                data: Data to insert.
            
            Raises:
                TypeError: if `data` is not instance of `list` then raises `TypeError`.

        """
        try:
            if not isinstance(data, list):
                raise TypeError(f'Data must be list: data= {data}, data type= {type(data).__name__}')

            query = 'Insert into person values ' + self.__generate_query(data)
            cur = self.conn.cursor()

            cur.execute(query)

            self.conn.commit()
            cur.close()

            logger.info('Insert successful.')
        
        except TypeError as e:
            logger.error(str(e))
            raise
        except py.errors.SyntaxError as e:
            logger.error(str(e))
            raise
        

    def disconnect(self):
        """
            Disconnect to DB.
        """

        try:
            self.conn.close()
            logger.info(f"{self.__class__.__name__} DB connection is closed")
        except Exception as e:
            logger.error(str(e))
            raise

        

if __name__ == "__main__":
    postgre = PostgreSQL()
    postgre.connect()
    postgre.fetch('SELECT * FROM person')
    postgre.insert(
        data=[
                ('7', 'Ayşe', 'GÜRDERELİ'), 
                ('8', 'Burhan', 'KUZU'),
                ('9', 'Orhan Veli', 'KINIK',)]
    )
    postgre.disconnect()
    