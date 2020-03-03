import cx_Oracle
from loguru import logger

import sqls4merlin


class OracleClient:
    oracleConnection = cx_Oracle
    db = None
    dsn = None
    cursor = None
    # set to True if you need to log SQLs
    debug_sql = False

    # connection type can be direct or tnsnames
    # you can use SID or service name in direct connection

    def __init__(self, connection_info, **kwargs):
        try:
            # 2DO: add check for connection type and change connection style depending on this
            logger.debug(connection_info)
            if connection_info["connection_type"] == 'direct':
                # check if there SID or SERVICE NAME and create DSN accordingly
                if 'service_name' in connection_info:
                    self.dsn = cx_Oracle.makedsn(host=connection_info['host_name'], port=connection_info['port'],
                                                 service_name=connection_info['service_name'])
                elif 'sid' in connection_info:
                    self.dsn = cx_Oracle.makedsn(host=connection_info['host_name'], port=connection_info['port'],
                                                 sid=connection_info['sid'])
                logger.debug("Current DSN: {}".format(self.dsn))
                self.oracleConnection = cx_Oracle.connect(kwargs['username'], kwargs['password'], self.dsn,
                                                          encoding='UTF-8')
            elif connection_info["connection_type"] == 'tnsnames':
                self.oracleConnection = cx_Oracle.connect(kwargs['username'], kwargs['password'],
                                                          connection_info['connection_name'], encoding='UTF-8')
            logger.debug("Connected to oracle")
            self.cursor = self.oracleConnection.cursor()
            logger.info("Oracle connection created")
        except Exception as e:
            logger.critical("cx_Oracle error: {}".format(e.args[0]))
            raise

    def __del__(self):
        logger.info("Closing oracle connection.")
        try:
            if self.cursor is not None:
                self.cursor.close()
                logger.info("Oracle connection cursor do not exists.")
            # catch exception if connection is not open
            if self.oracleConnection is not None:
                try:
                    self.oracleConnection.disconnect()
                except Exception as e:
                    logger.debug("Looks like connection wasn't even open since we have exception here: {}".format(e))
                    pass
                logger.info("Oracle connection has been closed.")
        except cx_Oracle.DatabaseError:
            logger.critical('Finally')
            logger.debug("Oracle connection wasn't even open")

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

        res_id = self.run_sql(sqls4merlin.sqls['get_last_partition_id'].format(table_owner, table_name))
        # select which code to run in order to get last key value
        sql_code = None
        if partition_key_type == 'date':
            sql_code = 'get_last_partition_key_date'
        elif partition_key_type == 'date_as_number':
            sql_code = 'get_last_partition_key_date_as_number'

        res_key = self.run_sql_block_for_key_value(
            sqls4merlin.sqls[sql_code].format(table_owner, table_name,
                                              res_id[0][4]))

        logger.debug("Latest partition key value is: {}".format(res_key))
        results = [res_key, res_id[0][4], res_id[0][3]]
        return results

    def getPartitionedIndexes(self, table_owner, table_name, partition_position):
        all_indexes = self.run_sql(
            sqls4merlin.sqls['get_index_name_and_index_tablespace'].format(table_owner, table_name,
                                                                           partition_position))
        # res_list = self.run_sql
        logger.info(all_indexes)
        return all_indexes

    def runSQLS(self, sql_list):
        itemId = None
        try:
            for itemId, sql_item in enumerate(sql_list):
                for line in sql_item.split('\n'):
                    logger.debug("Executing {}.".format(sql_item))
                    self.cursor.execute(line)
        except cx_Oracle.DatabaseError as e:
            errorObj, = e.args
            logger.error("Row {} has error {}".format(itemId, errorObj.message))

    def preCheck(self, table_owner, table_name):
        """
        :param table_owner: str, table owner (schema)
        :param table_name: str, table name
        :return:
        :rtype: Boolean
        """
        logger.debug("Punning pre-checks for table")
        res = self.run_sql(sqls4merlin.sqls['preCheck'].format(table_owner, table_name))
        # check if this table exists
        if res[0][0] == 1:
            return True
        else:
            return False
