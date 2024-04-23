config_dict_producer = {'bootstrap.servers': 'localhost:45055'}
config_dict_consumer = {'bootstrap.servers': 'localhost:45055', 'group.id': 'advertisex', 'auto.offset.reset': 'earliest'}
config_dict_cluster = {'contact_points': ['127.0.0.1'], 'port': 9042, 'cql_version': '3.4.6'}
config_keyspace = 'test_keyspace'