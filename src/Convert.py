#### Built-in Module ####
import logging

#### Project Scripts ####
from src.db.DBFormatEnum import DBFormatEnum
from src.db.ElasticSearch import ElasticSearch
from src.db.PostgreSQL import PostgreSQL

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def dictToTuple(data: list):
    """
        Convert dictionary list to tuple list

        Args:
            data: The list with dictionaries
        
        Return:
            list: The list with tuples
    """
    return [tuple(row.values()) for row in data]

def tupleToDict(data: list):
    """
        Convert tuple list to dictionary list

        Args:
            list: The list with tuples
        
        Return:
            list: The list with dictionaries
    """

    NAME = 0
    SURNAME = 1
    AGE = 2

    convert_data = list()

    for row in data:
        convert_data.append(
            {
                'name': row[NAME],
                'surname': row[SURNAME],
                'age': row[AGE]
            }
        )
    
    return convert_data
        
def convert(source, target, data):
    """
        Convert source type list to target type list

        Args:
            source: Type of list
            target: Type of list
            data: Data
        
        Return:
            list: 

        Raises:

    """
    try:
        if not isinstance(source, DBFormatEnum) or \
         not isinstance(target, DBFormatEnum):
            raise TypeError(f"source and target must be DBFormatEnum")
        
        if not isinstance(data, list):
            raise TypeError(f"data must be list: data= {data}, data type= {type(data).__name__}")
        
        if source == DBFormatEnum.DICT and target == DBFormatEnum.TUPLE:
            return dictToTuple(data)
        elif source == DBFormatEnum.TUPLE and target == DBFormatEnum.DICT:
            return tupleToDict(data)
            
    except Exception as e:
        logger.error(str(e))
        raise


if __name__ == "__main__":
    es = PostgreSQL()
    es.connect()
    raw_data = es.fetch('Select * from students')
    es.disconnect()
    
    data = convert(DBFormatEnum.TUPLE, DBFormatEnum.DICT, raw_data)
    print(data)