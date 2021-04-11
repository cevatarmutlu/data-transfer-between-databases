 In-Python module
import unittest

#### Project Scripts ####
from db.PostgreSQL import PostgreSQL

class TestPostgreSQL(unittest.TestCase):
    postgre = PostgreSQL()

    def test1Connect(self):
        # Test DB Connection.
        print('\n**Start MySQL connect() test**\n')
        self.assertEqual(TestMySQL.postgre.connect(), None)
        print('\n**End MySQL connect() test**\n')


    def test2Fetch(self):

        print('\n**Start MySQL fetch() test**\n')    
        with self.assertRaises(TypeError):
            TestMySQL.postgre.fetch(query=5)
            TestMySQL.postgre.fetch(query=[1, 2, 3])
            TestMySQL.postgre.fetch(query=pandas.DataFrame([1, 2, 3]))
            TestMySQL.postgre.fetch(query='SELECT * FROM client')
        
        with self.assertRaises(pandas.io.sql.DatabaseError):
            TestMySQL.postgre.fetch(query='merhaba dunya')
            TestMySQL.postgre.fetch(query='SELECT * FROM client where order=5')

        self.assertEqual(type(TestMySQL.postgre.fetch(query='SELECT * FROM client')), type(pandas.DataFrame()))

        print('\n**End MySQL fetch() test**\n')   

    def test3Insert(self):
        pass

    def test4Disconnect(self):
        # Test DB Connection.

        print('\n**Start MySQL disconnect() test**\n')  
        self.assertEqual(TestMySQL.postgre.disconnect(), None)
        print('\n**End MySQL disconnect() test**\n')  