from elasticsearch import Elasticsearch


def get_es_docs_using_ids(client: Elasticsearch, index_name: str, ids: list[str], return_id_dict=False) -> list | dict:
    res = client.mget(index=index_name, ids=ids)
    docs = res['docs']

    def get_source(node_doc):
        return node_doc["_source"]

    def get_source_dict(node_doc):
        """
        Generate k-v pairs for id, source
        """
        _id = node_doc["_id"]
        source = node_doc["_source"]
        return _id, source

    def filter_doc(doc):
        return '_source' in doc and doc['_source'] is not None

    def filter_invalid_doc(doc):
        return not doc['found']

    valid_docs_filter = filter(filter_doc, docs)

    if return_id_dict:
        return dict(map(get_source_dict, valid_docs_filter))

    return list(map(get_source, valid_docs_filter))