from oracle_executioner import OracleClient
db_parameters = {
    "connection_type": "direct",
    "host_name": "localhost",
    "service_name": "XE",
    "port": "1521"
}
oe = OracleClient(db_parameters, username='test', password='test')
oe.getTableLastPartitionKey('test', 'table_test_t1', 'date')