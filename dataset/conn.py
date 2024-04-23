import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir))

from cassandra.cluster import Cluster
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.connection import Connection
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory

from config.config import config_dict_cluster

def connect_to_cql():
    cluster = Cluster(config_dict_cluster['contact_points'], config_dict_cluster['port'], cql_version=config_dict_cluster['cql_version'])   
    try:
        session = cluster.connect()
        print("Connected to the cluster.")
        return session
    except Exception as e:
        print(e)
        return None
    
def create_keyspace(session, keyspace_name):
    try:
        session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
        print(f"Keyspace {keyspace_name} created.")
    except Exception as e:
        print(e)
        
def close_connection(session):
    session.shutdown()
    print("Connection closed.")
    
def connect_to_keyspace(keyspace_name):
    cluster = Cluster(config_dict_cluster['contact_points'], config_dict_cluster['port'], cql_version=config_dict_cluster['cql_version'])   
    try:
        session = cluster.connect(keyspace_name)
        session.row_factory = dict_factory
        print(f"Connected to keyspace {keyspace_name}.")
        return session
    except Exception as e:
        print(e)
        return None

if __name__ == "__main__":
    session = connect_to_cql()
    create_keyspace(session, "test_keyspace")
    close_connection(session)
    session = connect_to_keyspace("test_keyspace")
    close_connection(session)