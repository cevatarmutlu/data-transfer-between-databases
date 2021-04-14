# source: https://medium.com/naukri-engineering/elasticsearch-tutorial-for-beginners-using-python-b9cb48edcedc

from elasticsearch import Elasticsearch, exceptions


# By default we connect to localhost:9200
es = Elasticsearch()
print(es)

# Connection elasticsearch
# es = Elasticsearch(
#     [
#         {
#             'host': 'localhost',
#             'port': 9200
#         }
#     ]
# )
# print(es)

# Create Index. Go http://localhost:9200/my-index
# res = es.indices.create(index='my-index', ignore=400)
# print(res)

# Delete Index
# res = es.indices.delete(index='my-index')
# print(res)

# Generate my-doc document and insert body param. id auto generated.
# res = es.index(index='my-index', doc_type='my-doc', body={})
# print(res)

# Generate my-doc document and insert body param. id 1. Go http://localhost:9200/my-index/my-doc/1
# res = es.index(index='my-index', doc_type='my-doc', id=3, body={
#     'name': 'ahmet',
#     'surname': 'mehmet',
#     'age': 28
# })
# print(res.get('result'))


# Get document
# res = es.get(index='my-index', doc_type='my-doc', id=1)
# print(res.get('_source'))

# Delete document
# res = es.delete(index='my-index', doc_type='my-doc', id=1)
# print(res.get('result'))


# Search index
# res = es.search(
#     index='my-index',
#     body={}
# )
# print(res.get('hits').get('hits'))

#Search doc
# res = es.search(
#     index='my-index',
#     doc_type='my-doc',
#     body={}
# )
# print(res.get('hits').get('hits'))

# datas = res.get('hits').get('hits')
# for i in datas:
#     print(i.get('_source'))



# Search: Math operator
res = es.search(index='my-index', doc_type='my-doc', body={
    'query': {
        'match': {
            'name': 'cevat'
        }
    }
})
print(res.get('hits').get('hits'))