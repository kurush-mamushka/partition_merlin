from oracle_executioner import OracleClient
from datetime import datetime
from loguru import logger
import sys
import csv

SETTINGS_FILE = 'advisor_list.csv'
OPERATION = 'truncate'


# you can use OPERATION = 'drop'

def read_advisor_settings():
    with open(SETTINGS_FILE) as f:
        reader = csv.reader(f)
        data = list(reader)
        res = [[item.upper() for item in ln] for ln in data]

        return res


def getPartInfo(dtinfo: str):
    dt_today = datetime.now()
    dt = datetime.strptime(dtinfo.split(',')[0].split(' ')[1], '%Y-%m-%d')
    dt_diff = dt_today - dt
    return dt, dt_diff.days


def main():
    tbl_list = read_advisor_settings()
    logger.debug(tbl_list)
    partitions2Drop = []

    # Settings part
    db_info = {
        "connection_type": "direct",
        "host_name": "127.0.0.1",
        "service_name": "XE",
        "port": "1521"
    }
    connection_auth = {
        'username': 'test2',
        'password': 'test'
    }
    if tbl_list:
        oe = OracleClient(db_info, username=connection_auth['username'], password=connection_auth['password'], debugSQL=False)
        for item2check in tbl_list:
            cOwner, cTable, retention = item2check[0], item2check[1], int(item2check[2])
            logger.info(f"Processing table {cOwner}.{cTable}. Retention policy for this table is: {retention}.")
            globalIndexExist = oe.checkGlobalIndexes(cOwner, cTable)
            if globalIndexExist:
                logger.error(
                    f"Global index exists for table {cOwner}.{cTable}. We will comment out all operationss for this table and just inform you with possible SQL.")

            res = oe.run_sql(f"select table_owner, table_name, partition_name, high_value "
                             f"from dba_tab_partitions where table_owner = '{cOwner}' and table_name = '{cTable}' order by partition_position")
            # calculation itself
            for ln in res:
                [tOwner, tName, tPart, tHV] = ln
                dt, diff = getPartInfo(tHV)
                logger.debug(f"Partition {tPart} having difference {diff}. Partition name is {tPart}")
                comment = '-- ' if globalIndexExist else ''
                if diff > retention:
                    logger.debug("This is good candidate to drop, adding to list")
                    partitions2Drop.append(f"/* Difference is: {diff}. and HV: {dt} */")
                    partitions2Drop.append(f"{comment}alter table {tOwner}.{tName} {OPERATION} partition {tPart};")
                    partitions2Drop.append("\n")

    for item in partitions2Drop:
        logger.info(item)


if __name__ == '__main__':
    sys.exit(main())
