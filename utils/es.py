import os

from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
from utils.constants import NODE_INDEX, EDGE_INDEX


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


def insert_docs_to_index(es_client: Elasticsearch, operations: list):
    try:
        helpers.bulk(es_client, operations, chunk_size=5000, request_timeout=240)
        # helpers.bulk(es_client, operations, chunk_size=5000)
    except BulkIndexError as e:
        for i, error in enumerate(e.errors[:1]):  # Only show first
            doc_id = error["index"].get("_id", "N/A")
            reason = error["index"]["error"].get("reason", "Unknown")
            print(f"[{i}] ID={doc_id} → {reason}")


def reindex_edges(es_client: Elasticsearch, edges_index, dest_index_name: str):
    reindex_body = {
        "source": {
            "index": edges_index,
            "_source": {
                "excludes": ["subject", "object"]
            }
        },
        "dest": {
            "index": dest_index_name,
        }
    }
    es_client.reindex(body=reindex_body)


def refresh_es_index(es_url: str):
    es_client = Elasticsearch(es_url, request_timeout=240)
    INDEX_NAME = os.environ.get("INDEX_NAME")

    node_mapping = es_client.indices.get_mapping(index=NODE_INDEX)
    node_props = node_mapping[NODE_INDEX]["mappings"]["properties"]

    edge_mapping = es_client.indices.get_mapping(index=EDGE_INDEX)
    edge_props = edge_mapping[EDGE_INDEX]["mappings"]["properties"]

    edge_props["subject"] = {"properties": node_props}
    edge_props["object"] = {"properties": node_props}


    index_settings_and_mappings = {
        "mappings": {
            "properties": edge_props
        },
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0,
            "codec": "best_compression"
        }
    }

    # refresh index
    if es_client.indices.exists(index=INDEX_NAME):
        es_client.indices.delete(index=INDEX_NAME)

    es_client.indices.create(index=INDEX_NAME, body=index_settings_and_mappings)

    # copy index from edge, preparing for update
    # reindex_edges(es_client, INDEX_NAME)
