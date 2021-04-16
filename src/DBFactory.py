#### Built-in Modules ####
import logging

#### Project Scripts ####
from src.db.DBEnum import DBEnum
from src.db.IDB import IDB
from src.db.ElasticSearch import ElasticSearch
from src.db.PostgreSQL import PostgreSQL

logger = logging.getLogger(__name__)


class DBFactory:
    """
        Return specific database instance.

        Examples:
            DBFactory.getDB(DBEnum.ElasticSearch)
    """

    @staticmethod
    def getDB(dbType: DBEnum):
        """
            Return specific database instance.

            Args:
                dbType: Database type.

            Returns:
                IDB subclass: Your DB instance.
            
            Raises:
                TypeError: if `dbType` is not instance of `DBEnum` raises `TypeError`.
                     
        """
        try:
            if not isinstance(dbType, DBEnum):
                raise TypeError(f'dbType must be DBEnum type, not {type(filename).__name__}')
                    

            if (dbType == DBEnum.PostgreSQL):
                return PostgreSQL()
            elif (dbType == DBEnum.ElasticSearch):
                return ElasticSearch()
        
        except TypeError as e:
            logger.error(str(e))
            raise
