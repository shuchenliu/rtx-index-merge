from elasticsearch import Elasticsearch
from utils.constants import NODE_INDEX
from utils.es import get_es_docs_using_ids


def get_nodes_details(client: Elasticsearch, ids: list[str]) -> dict:
    """
    Get source details for given list of ids of nodes

    :param client: an elasticsearch client
    :param ids: a list of ids of nodes
    :return: dict, where keys are node ids and values are node details
    """
    # we generate a dict like {"NCBITaxon:2051579" : {...}} for fast accessing
    details = get_es_docs_using_ids(client, NODE_INDEX, ids, return_id_dict=True)

    return details