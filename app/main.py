###################################################
from partition_master import PartGenerator
from configuration_getter import GetConfig
from loguru import logger
from oracle_executioner import OracleClient
from datetime import datetime, timedelta
###################################################
logger.info("Test")
config = GetConfig('test_config_test_db.json')
oracle_date_format_python = '%d%m%Y'

# print (config.config)

# get last partition info, via oracle connection

# 2DO CHANGE TO PARAMETRIZED USWERNAME / PASSWORD
oe = OracleClient(config.db_info, username='test', password='test')
all_sqls = []
for current_table in config.flat_tables_info:
    ###
    # for testing only
    # let's set initial date
    all_info = oe.getTableLastPartitionInfo(current_table['table_owner'], current_table['table_name'],
                                            current_table['partition_key_type'])
    current_partition_date_str = all_info[0]
    max_partition_position = all_info[1]
    all_indexes = oe.getPartitionedIndexes(current_table['table_owner'], current_table['table_name'],
                                           max_partition_position)
    # this is date for highest partition key in python format
    logger.debug(
        "Key value is {} will be formatted with {}.".format(current_partition_date_str, oracle_date_format_python))
    if current_table['partition_key_type'] == 'date':
        new_partition_name_dt = datetime.strptime(current_partition_date_str, oracle_date_format_python)
    elif current_table['partition_key_type'] == 'date_as_number':
        new_partition_name_dt = datetime.strptime(current_partition_date_str,
                                                  PartGenerator.ora2pythonDT(current_table['ora_date_format']))
    # create parameters for procedure dynamically
    parameters = {'latest_partition_date': new_partition_name_dt, 'indexes_list': all_indexes}
    for key, value in current_table.items():
        parameters[key] = value
    csql = PartGenerator(**parameters)

    for single_sql in csql.generateSQLs():
        logger.info(single_sql)
        all_sqls.append(single_sql)
    logger.success("Done")

# debug print while still implementing
for single_sql in all_sqls:
    for line in single_sql.split('\n'):
        print('{};'.format(line))

# close oracle connection
del oe
