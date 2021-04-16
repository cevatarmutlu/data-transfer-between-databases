from DBFactory import DBFactory
from db.DBEnum import DBEnum
from Covert import convert

def main():
    source = DBEnum.PostgreSQL
    target = DBEnum.ElasticSearch

    source_db = DBFactory.getDB(source)
    target_db = DBFactory.getDB(target)

    source_db.connect()
    raw_data = source_db.fetch('SELECT * FROM students')
    source_db.disconnect()

    data = convert(source_db.FORMAT, target_db.FORMAT, raw_data)

    target_db.connect()
    target_db.insert(data)
    target_db.disconnect()


if __name__ == '__main__':
    main()