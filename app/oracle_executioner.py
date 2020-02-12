import cx_Oracle
from loguru import logger
import sqls4merlin
from pprint import pprint


class OracleClient:
    oracleConnection = cx_Oracle
    db = None
    dsn = None
    cursor = None
    debug_sql = False

    # "db": "test_db",
    # "db_parameters": {
    #     "connection_type": "direct",
    #     "host_name": "localhost",
    #     "service_name": "XE",
    #     "port": "1521"
    # },
    # or
    # "db_parameters": {
    #      "connection_type": "tnsnames",
    #      "connection_name": "testdb"
    # connection type can be direct or tnsnames
    def __init__(self, connection_info, **kwargs):
        # 2DO: add check for connection type and change connection style depending on this
        if connection_info["connection_type"] == 'direct':
            self.dsn = cx_Oracle.makedsn(connection_info['host_name'], connection_info['port'],
                                         connection_info['service_name'])
            self.oracleConnection = cx_Oracle.connect(kwargs['username'], kwargs['password'], self.dsn,
                                                      encoding='UTF-8')
        elif connection_info["connection_type"] == 'tnsnames':
            self.oracleConnection = cx_Oracle.connect(kwargs['username'], kwargs['password'],
                                                      connection_info['connection_name'], encoding='UTF-8')
        self.cursor = self.oracleConnection.cursor()
        logger.info("Oracle connection created")

    def __del__(self):
        logger.info("Oracle connection close")
        self.oracleConnection.close()

    def run_sql(self, sql):
        if self.debug_sql:
            logger.debug("Executing {}".format(sql))

        self.cursor.execute(sql)
        res = []
        for item in self.cursor:
            res.append(item)
        return res

    def run_sql_block_for_key_value(self, sql_block):
        o_result = self.cursor.var(str)
        if self.debug_sql:
            logger.debug("Executing PL/SQL block {}".format(sql_block))
        self.cursor.execute(sql_block, o_result=o_result)
        return o_result.getvalue()

    def getTableLastPartitionInfo(self, table_owner, table_name, partition_key_type):

        res_id = self.run_sql(sqls4merlin.sqls['get_last_partition_id'].format(table_owner.upper(), table_name.upper()))
        # select which code to run in order to get last key value

        if partition_key_type == 'date':
            sql_code = 'get_last_partition_key_date'
        elif partition_key_type == 'date_as_number':
            sql_code = 'get_last_partition_key_date_as_number'

        res_key = self.run_sql_block_for_key_value(
            sqls4merlin.sqls[sql_code].format(table_owner.upper(), table_name.upper(),
                                                                   res_id[0][4]))

        logger.debug("Latest partition key value is: {}".format(res_key))
        results = []
        results.append(res_key)
        results.append(res_id[0][4])
        return results

    def getPartitionedIndexes(self, table_owner, table_name, partition_position):
        index_list = []
        all_indexes = self.run_sql(
            sqls4merlin.sqls['get_index_name_and_index_tablespace'].format(table_owner.upper(), table_name.upper(),
                                                                           partition_position))
        # res_list = self.run_sql
        logger.info(all_indexes)
        return all_indexes
