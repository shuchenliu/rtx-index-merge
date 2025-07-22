import os

from elasticsearch import Elasticsearch

from utils.env import check_is_prod
from utils.es import create_nested_index


def main():
    is_prod = check_is_prod()
    SERVER = os.getenv("SERVER")
    PORT = os.getenv("PORT")
    ES_URL = "http://%s:%s" % (SERVER, PORT)

    # create new index
    migrate_handle = create_nested_index(ES_URL)


    # one record test
    # nested_index_name = os.environ.get("NESTED_INDEX_NAME")
    # merged_index_name = os.environ.get("INDEX_NAME")
    # es = Elasticsearch(ES_URL, request_timeout=240)
    # doc_id = "1755719"
    # doc = es.get(index=merged_index_name, id=doc_id)
    # es.index(index=nested_index_name, id=doc_id, body=doc['_source'])



    # migrate
    migrate_handle()


if __name__ == '__main__':
    main()



