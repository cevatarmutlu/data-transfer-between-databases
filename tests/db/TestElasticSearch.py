#### In-Python module ####
import unittest

#### Project Scripts ####
from db.ElasticSearch import ElasticSearch

class TestElasticSearch(unittest.TestCase):
    es = ElasticSearch()
    class_name = 'ElasticSearch'

    def test0_init(self):

        print(f'\n**Start {TestElasticSearch.class_name} __init__() test**\n')

        #### Invalid ####
        with self.assertRaises(TypeError):
            # Host test
            ElasticSearch(host=4)
            ElasticSearch(host=[1, 2, 3])
            ElasticSearch(host={})

            # Port test
            ElasticSearch(port='123')
            ElasticSearch(port=[1, 2, 3])
            ElasticSearch(port={})
        
        with self.assertRaises(ValueError):
            # Host test
            ElasticSearch(host='')

            # Port test
            ElasticSearch(port='')

        #### Valid ####
        ElasticSearch(host='localhost', port=9200)

        print(f'\n**End {TestElasticSearch.class_name} __init__() test**\n')


    def test1_connect(self):
        # Test DB Connection.
        print(f'\n**Start {TestElasticSearch.class_name} connect() test**\n')

        #### Invalid
        self.assertEqual(TestElasticSearch.es.connect(), None)

        print(f'\n**End {TestElasticSearch.class_name} connect() test**\n')


    def test2_fetch(self):

        print(f'\n**Start {TestElasticSearch.class_name} fetch() test**\n')    

        #### Invalid ####
        with self.assertRaises(TypeError):
            TestElasticSearch.es.fetch(body=5)
            TestElasticSearch.es.fetch(body=[1, 2, 3])
            TestElasticSearch.es.fetch(body=pandas.DataFrame([1, 2, 3]))
            TestElasticSearch.es.fetch(body='SELECT * FROM client')
        
        with self.assertRaises(Exception):
            TestElasticSearch.es.fetch(body={'wa': 'wa', 'aw': 'aw'})
        
        #### Valid ####
        TestElasticSearch.es.fetch(body={})
        TestElasticSearch.es.fetch(body={
            'query': {
                'match': {
                    'name': 'cevat'
                }
            }
        })
        

        print(f'\n**End {TestElasticSearch.class_name} fetch() test**\n')

    def test3_insert(self):

        print(f'\n**Start {TestElasticSearch.class_name} insert() test**\n')  

        #### Invalid ####
        with self.assertRaises(TypeError):
            TestElasticSearch.es.insert(body=2)
            TestElasticSearch.es.insert(body='asdf')
            TestElasticSearch.es.insert(body=[1, 2, 3])
        
        #### Valid ####
        TestElasticSearch.es.insert(body={
            'name': 'firuzan',
            'surname': 'arkayabakmaz',
            'age': 26,
            'status': 'dead',
            'degree': 5
        })

        print(f'\n**End {TestElasticSearch.class_name} insert() test**\n')
  

    def test4_disconnect(self):
        # Test DB Connection.

        print(f'\n**Start {TestElasticSearch.class_name} disconnect() test**\n')  

        #### Valid ####
        self.assertEqual(TestElasticSearch.es.disconnect(), None)

        print(f'\n**End {TestElasticSearch.class_name} disconnect() test**\n')
