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


def reindex(es_client: Elasticsearch, source_index_name, dest_index_name: str):
    reindex_body = {
        "source": {
            "index": source_index_name,
        },
        "dest": {
            "index": dest_index_name,
        }
    }
    es_client.reindex(body=reindex_body, wait_for_completion=False)


def migrate_merged_index_to_nested(es_client):
    nested_index_name = os.environ.get("NESTED_INDEX_NAME")
    merged_index_name = os.environ.get("INDEX_NAME")

    reindex(es_client, source_index_name=merged_index_name, dest_index_name=nested_index_name)


def create_index_using_mapping(es_client:Elasticsearch, name: str, props):
    mapping_and_settings = {
        "mappings": {
            "properties": props
        },
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0,
            "codec": "best_compression"
        }
    }

    if es_client.indices.exists(index=name):
        es_client.indices.delete(index=name)

    es_client.indices.create(index=name, body=mapping_and_settings)


def created_adjacency_list_index(es_url:str):
    es_client = Elasticsearch(es_url, request_timeout=240)
    adj_index_name = os.environ.get("ADJACENCY_LIST_INDEX_NAME")

    node_mapping = es_client.indices.get_mapping(index=NODE_INDEX)
    node_props = node_mapping[NODE_INDEX]["mappings"]["properties"]

    edge_mapping = es_client.indices.get_mapping(index=EDGE_INDEX)
    edge_props = edge_mapping[EDGE_INDEX]["mappings"]["properties"]

    # add edges props
    fields = ['in_edges', 'out_edges']
    for field in fields:
        node_props[field] = {
            'type': 'nested',
            'properties': edge_props
        }

    create_index_using_mapping(es_client, adj_index_name, node_props)



def create_nested_index(es_url:str):
    es_client = Elasticsearch(es_url, request_timeout=240)
    nested_index_name = os.environ.get("NESTED_INDEX_NAME")
    merged_index_name = os.environ.get("INDEX_NAME")

    merged_edges_mappings = es_client.indices.get_mapping(index=merged_index_name)
    nested_props = merged_edges_mappings[merged_index_name]["mappings"]["properties"]

    # modify `subject` and `object` field to be nested
    fields = ["object", "subject"]
    for field in fields:
        mapping = nested_props[field]
        nested_props[field] = {
            **mapping,
            "type": "nested"
        }

    create_index_using_mapping(es_client, nested_index_name, nested_props)

    def migrate_handle():
        migrate_merged_index_to_nested(es_client)

    return migrate_handle


def refresh_es_index(es_url: str):
    es_client = Elasticsearch(es_url, request_timeout=240)
    INDEX_NAME = os.environ.get("INDEX_NAME")

    node_mapping = es_client.indices.get_mapping(index=NODE_INDEX)
    node_props = node_mapping[NODE_INDEX]["mappings"]["properties"]

    edge_mapping = es_client.indices.get_mapping(index=EDGE_INDEX)
    edge_props = edge_mapping[EDGE_INDEX]["mappings"]["properties"]

    edge_props["subject"] = {"properties": node_props}
    edge_props["object"] = {"properties": node_props}


    create_index_using_mapping(es_client, INDEX_NAME, edge_props)
