#### Project Scripts ####
from db.DBEnum import DBEnum
from db.IDB import IDB
from db.ElasticSearch import ElasticSearch
from db.PostgreSQL import PostgreSQL

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

        if not isinstance(dbType, DBEnum):
            raise TypeError(f'dbType must be DBEnum type, not {type(filename).__name__}')
                

        if (dbType == DBEnum.PostgreSQL):
            return PostgreSQL()
        elif (dbType == DBEnum.ElasticSearch):
            return ElasticSearch()