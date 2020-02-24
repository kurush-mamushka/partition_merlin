###################################################
from partition_master import PartGenerator
from configuration_getter import GetConfig
from loguru import logger
from oracle_executioner import OracleClient
from datetime import datetime
import argparse

###################################################
logger.info("Partition Merlin")
oracle_date_format_python = '%d%m%Y'
new_partition_name_dt = None
args = None


########################################################################################################################
def parseArgs():
    parser = argparse.ArgumentParser(description='This is Partition Merlin',
                                     epilog='--\nHave a great day from partition Merlin')
    parser.add_argument('--db_connection_config', help='json config file for database connection', required=True)
    parser.add_argument('--db_tables_config', help='Tables configuration file for partition maintenance', required=True)
    parser.add_argument('--run_sql', help='If you want to execute SQLs in database', action='store_true')
    parser.add_argument('--username', help='Username for DB connection', required=True)
    parser.add_argument('--password', help='Password for DB connection', required=True)
    args = parser.parse_args()
    return args


########################################################################################################################
# get last partition info, via oracle connection
args = parseArgs()
config = GetConfig(args.db_connection_config, args.db_tables_config)

oe = OracleClient(config.db_info, username=args.username, password=args.password)
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
    logger.success("*" * 80)
    all_sqls.append("/* ************************************************************ */")
# debug print while still implementing
for single_sql in all_sqls:
    for line in single_sql.split('\n'):
        print('{};'.format(line))
        if args.run_sql:
            logger.debug("Running sql: {}".format(single_sql))

# close oracle connection
del oe
