#### Built-in Modules ####
import logging

#### Installed Modules ####
from elasticsearch import Elasticsearch

#### Project Scripts ####
from db.IDB import IDB
from db.DBFormatEnum import DBFormatEnum

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ElasticSearch(IDB):
    """
        This class connect ElasticSearch and insert data.

        Args:
            FORMAT(static): Represents the type of obtained data from ElasticSearch.
            host: host
            port: port
        
        Raises:
            TypeError: if `host` is not instance of `str` and 
                `port is not instance of `int` then raises `TypeError`.
            ValueError: if `host`, `port` is empty then raises `ValueError`.

    """

    FORMAT = DBFormatEnum.DICT

    def __init__(self, 
        host='localhost',
        port=9200,
    ):

        logger.debug(f'{self.__class__.__name__} class to be generated')
        logger.debug(f'Class parameters: host={host}, port={port}' )

        try: 
            if  not isinstance(host, str) or \
                not isinstance(port, int):
                raise TypeError('Host parameter must be string and port parameter must be integer')
            
            if  not host:
                raise ValueError('Host parameter not must be empty.')
            
            if port < 0:
                raise ValueError('port parameter must be positive')
        
            self.auth = {
                "host": host,
                "port": port
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
            Connect to ElasticSearch.
        """

        try:
            self.es = Elasticsearch([self.auth])

            logger.info(f"{self.__class__.__name__} connection is successful")
        except Exception as e:
            logger.error(e)
            raise
        

    def fetch(self, body: dict):
        """
            Fetch data from ElasticSearch.

            Args:
                body: The body that fetch the data.

            Returns:
                data (list): Contains obtained the data from ElasticSearch.
            
            Raises:
                TypeError: if `body` is not instance of `dict` then raises `TypeError`.
        """

        try:

            if not isinstance(body, dict):
                raise TypeError(f"body argument must be dict: body={body}, body type: {type(body).__name__}")

            res = self.es.search(
                index='my-index',
                doc_type='my-doc',
                body=body
            )

            raw_data = res.get('hits').get('hits')
            
            data = [row.get('_source') for row in raw_data]

            return data

        except TypeError as e:
            logger.error(str(e))
            raise
        except Exception as e:
            logger.error(str(e))
            raise

    def insert(self, body: dict):
        """
            Inserts the values to ElasticSearch.

            Args.
                body: Data to insert.
            
            Raises:
                TypeError: if `body` is not instance of `dict` then raises `TypeError`.

        """

        try:
            if not isinstance(body, dict):
                raise TypeError(f'`body` must be dict: body= {body}, body type= {type(body).__name__}')

            res = self.es.index(index='my-index', doc_type='my-doc', body=body)

            logger.info('Insert successful.')
        
        except TypeError as e:
            logger.error(str(e))
            raise
        except Exception as e:
            logger.error(str(e))
            raise
        

    def disconnect(self):
        """
            Disconnect to ElasticSearch.
        """

        try:
            self.es.close()
            logger.info(f"{self.__class__.__name__} connection is closed")
        except Exception as e:
            logger.error(str(e))
            raise

        

if __name__ == "__main__":
    es = ElasticSearch()
    es.connect()
    es.fetch({})
    # es.insert(
    #     body={
    #         'name': 'ahmet',
    #         'surname': 'mehmet',
    #         'age': 28
    #     }
    # )
    es.disconnect()
    