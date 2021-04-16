#### In-Python module ####
import unittest

#### Project Scripts ####
from src.db.PostgreSQL import PostgreSQL

class TestPostgreSQL(unittest.TestCase):
    postgre = PostgreSQL()

    def test0Init(self):

        print('\n**Start PostgreSQL __init__() test**\n')

        #### Invalid ####
        with self.assertRaises(TypeError):
            PostgreSQL(host=4)
            PostgreSQL(host=[1, 2, 3])
            PostgreSQL(port=[1, 2, 3])
            PostgreSQL(port='123')
        
        with self.assertRaises(ValueError):
            PostgreSQL(host='')

        #### Valid ####
        PostgreSQL(host='localhost')

        print('\n**End PostgreSQL __init__() test**\n')

    def test1Connect(self):
        # Test DB Connection.
        print('\n**Start PostgreSQL connect() test**\n')

        #### Invalid
        self.assertEqual(TestPostgreSQL.postgre.connect(), None)

        print('\n**End PostgreSQL connect() test**\n')


    def test2Fetch(self):

        print('\n**Start PostgreSQL fetch() test**\n')    

        #### Invalid ####
        with self.assertRaises(TypeError):
            TestPostgreSQL.postgre.fetch(query=5)
            TestPostgreSQL.postgre.fetch(query=[1, 2, 3])
            TestPostgreSQL.postgre.fetch(query=pandas.DataFrame([1, 2, 3]))
            TestPostgreSQL.postgre.fetch(query='SELECT * FROM client')
        
        
        #### Valid ####
        TestPostgreSQL.postgre.fetch(query='SELECT * FROM person')
        

        print('\n**End PostgreSQL fetch() test**\n')   

    def test3Insert(self):

        print('\n**Start PostgreSQL insert() test**\n')  

        #### Invalid ####
        with self.assertRaises(TypeError):
            TestPostgreSQL.postgre.insert(data=2)
            TestPostgreSQL.postgre.insert(data='asdf')
        
        #### Valid ####
        TestPostgreSQL.postgre.insert(data=[
                ('7', 'Ayşe', 'GÜRDERELİ'), 
                ('8', 'Burhan', 'KUZU'),
                ('9', 'Orhan Veli', 'KINIK',)])

        print('\n**End PostgreSQL insert() test**\n')   
  

    def test4Disconnect(self):
        # Test DB Connection.

        print('\n**Start PostgreSQL disconnect() test**\n')  

        #### Valid ####
        self.assertEqual(TestPostgreSQL.postgre.disconnect(), None)

        print('\n**End PostgreSQL disconnect() test**\n')