from elasticsearch import Elasticsearch
from utils.constants import NODE_INDEX


def get_nodes_details(client: Elasticsearch, ids: list[str]):
    """
    Get source details for given list of ids of nodes

    :param client: an elasticsearch client
    :param ids: a list of ids of nodes
    :return: dict, where keys are node ids and values are node details
    """


    res = client.mget(index=NODE_INDEX, ids=ids)
    docs = res['docs']

    def get_sources(node_doc):
        """
        Generate k-v pairs for id, source
        """
        _id = node_doc["_id"]
        source = node_doc["_source"]
        return _id, source

    def filter_doc(doc):
        return '_source' in doc and doc['_source'] is not None


    # we generate a dict like {"NCBITaxon:2051579" : {...}} for fast accessing
    details = dict(
        map(
            get_sources,
            filter(filter_doc, docs)
        ),
    )

    return details