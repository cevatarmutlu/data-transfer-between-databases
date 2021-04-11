 In-Python module
import unittest

#### Project Scripts ####
from db.PostgreSQL import PostgreSQL

class PostgreSQL(unittest.TestCase):
    postgre = PostgreSQL()

    def test0Init(self):

        print('\n**Start PostgreSQL __init__() test**\n')

        #### Invalid ####
        with self.assertRaises(TypeError):
            PostgreSQL(host=4)
            PostgreSQL(host=[1, 2, 3])
        
        with self.assertRaises(ValueError):
            PostgreSQL(host='')

        #### Valid ####
        PostgreSQL(host='localhost')

        print('\n**End PostgreSQL __init__() test**\n')

    def test1Connect(self):
        # Test DB Connection.
        print('\n**Start PostgreSQL connect() test**\n')

        #### Invalid
        self.assertEqual(PostgreSQL.postgre.connect(), None)

        print('\n**End PostgreSQL connect() test**\n')


    def test2Fetch(self):

        print('\n**Start PostgreSQL fetch() test**\n')    

        #### Invalid ####
        with self.assertRaises(TypeError):
            PostgreSQL.postgre.fetch(query=5)
            PostgreSQL.postgre.fetch(query=[1, 2, 3])
            PostgreSQL.postgre.fetch(query=pandas.DataFrame([1, 2, 3]))
            PostgreSQL.postgre.fetch(query='SELECT * FROM client')
        
        with self.assertRaises(pandas.io.sql.DatabaseError):
            PostgreSQL.postgre.fetch(query='merhaba dunya')
            PostgreSQL.postgre.fetch(query='SELECT * FROM client where order=5')


        #### Valid ####
        self.assertEqual(type(PostgreSQL.postgre.fetch(query='SELECT * FROM client')), type(pandas.DataFrame()))

        print('\n**End PostgreSQL fetch() test**\n')   

    def test3Insert(self):

        print('\n**Start PostgreSQL insert() test**\n')  

        #### Invalid ####
        with self.assertRaises(TypeError):
            TestPostgreSQL.postgre.insert(data=2)
            TestPostgreSQL.postgre.insert(data='asdf')
        
        #### Valid ####
        TestPostgreSQL.postgre.insert(data=[('1', '2'), ('3', '4')])
        print('\n**End PostgreSQL insert() test**\n')   
  

    def test4Disconnect(self):
        # Test DB Connection.

        print('\n**Start PostgreSQL disconnect() test**\n')  

        #### Valid ####
        self.assertEqual(PostgreSQL.postgre.disconnect(), None)

        print('\n**End PostgreSQL disconnect() test**\n')  