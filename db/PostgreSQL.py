#### Installed Modules ####
import psycopg2 as py

#### Project Scripts ####
from IDB import IDB
from DBFormatEnum import DBFormatEnum

class PostgreSQL(IDB):
    """
        This class connect and insert data to PostgreSQL.

        Args:
            FORMAT(static): Represents the type of obtained data from PostgreSQL.
            host: host
            port: port
            dbname:dbname
            user: user
            password: password

    """

    FORMAT = DBFormatEnum.TUPLE

    def __init__(self, 
        host='localhost',
        port=5432,
        dbname='postgres',
        user='postgres',
        password='123'
    ):

        self.auth = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password
        }

    def connect(self):
        """
            Connect to PostgreSQL.

            Returns:
                None	
        """

        try:
                
        except Exception as e:
            print(e)
            exit(1)
        
        print(f"{self.__class__.__name__} DB connection is successful")

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

        if type(query) != str:
            raise TypeError("PostgreSQL Query argument must be string.")
        
        cur = self.conn.cursor()
        cur.execute(query)
        data = cur.fetchall()
        cur.close()
        print("Data Fetched successful")
        
        return data
    
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
            query_suffix += '(' + ', '.join(row) + '), '
        
        return query_suffix

    def insert(self, data: list):
        """
            Inserts the values to PostgreSQL.

            Args.
                data: Data to insert.
            
            Returns:
                None
        """

        if not isinstance(data, list):
            raise TypeError('data must be list.')

        query = 'Insert into client values ' + self.__generate_query(data)
        cur = self.conn.cursor()
        cur.execute(query)
        self.conn.commit()
        cur.close()

    def disconnect(self):
        """
            Disonnect to DB.

            Returns:
                None
        """

        try:
            self.conn.close()
        except Exception as e:
            print(e)
            exit(1)
        print(f"{self.__class__.__name__} DB connection is closed")

if __name__ == "__main__":
    postgre = PostgreSQL()
    # postgre.connect()
    # print(postgre.fetch("SELECT * FROM client"))
    # postgre.disconnect()
    postgre.insert(data=[('bir', 'iki', 'üç'), ('4', '5', '6')])      