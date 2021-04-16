#### Built-in Module ####
import logging

#### Project Scripts ####
from src.DBFactory import DBFactory
from src.db.DBEnum import DBEnum
from src.convert import convert

def main():

    logging.basicConfig(
        filename='data-transfer.log', 
        filemode='w', 
        level=logging.DEBUG, 
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    logger.info('Data Transfer Started')

    try:
        source = DBEnum.PostgreSQL
        target = DBEnum.ElasticSearch

        source_db = DBFactory.getDB(source)
        target_db = DBFactory.getDB(target)

        source_db.connect()
        raw_data = source_db.fetch('SELECT * FROM studenssts')
        source_db.disconnect()

        data = convert(source_db.FORMAT, target_db.FORMAT, raw_data)

        target_db.connect()
        target_db.insert(data)
        target_db.disconnect()

        logger.info('Data Transfer Finished')

    except Exception as e:
        print(str(e), "Please read data-transfer.log")

if __name__ == '__main__':
    main()