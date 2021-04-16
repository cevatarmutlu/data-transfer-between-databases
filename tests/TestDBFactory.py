# In-Python module
import unittest

#### Project Scripts ####
from src.DBFactory import DBFactory
from src.db.DBEnum import DBEnum
from src.db.IDB import IDB

class TestDBFactory(unittest.TestCase):

    class_name = "DBFactory"

    def test1_getDB(self):

        print(f'\n**Start {TestDBFactory.class_name} getDB() test**\n')

        #### Invalid ####
        with self.assertRaises(TypeError):
            DBFactory.getDB(dBType=5)
            DBFactory.getDB(dBType='')
            DBFactory.getDB(dBType='Hello World!')
            DBFactory.getDB(dBType='MongoDB')


        #### Valid ####
        self.assertIsInstance(
            DBFactory.getDB(DBEnum.PostgreSQL),
            IDB
        )
        
        print(f'\n**End {TestDBFactory.class_name} getDB() test**\n')