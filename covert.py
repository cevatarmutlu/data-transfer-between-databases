from db.DBFormatEnum import DBFormatEnum
from db.ElasticSearch import ElasticSearch
from db.PostgreSQL import PostgreSQL

def dictToTuple(data: list):
    """

        Args:
            data:
        
        Return:
            list:
    """
    return [tuple(row.values()) for row in data]

def tupleToDict(data: list):
    """
        
        Args:
            list:
        
        Return:
            list:
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
        

        Args:
            source: 
            target:
            data:
        
        Return:
            list: 

        Raises:

    """
    
    if source == DBFormatEnum.DICT and target == DBFormatEnum.TUPLE:
        return dictToList(data)
    elif source == DBFormatEnum.TUPLE and target == DBFormatEnum.DICT:
        return listToDict(data)


if __name__ == "__main__":
    es = PostgreSQL()
    es.connect()
    raw_data = es.fetch('Select * from students')
    es.disconnect()
    
    data = convert(DBFormatEnum.TUPLE, DBFormatEnum.DICT, raw_data)
    print(data)