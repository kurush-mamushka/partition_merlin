import json
from loguru import logger


class GetConfig:
    config = {}
    db_info = {}
    schemas = {}
    flat_tables_info = []

    def __init__(self, db_connection_config, db_tables_config):
        logger.info("Config file for DB connection : {}".format(db_connection_config))
        with open(db_connection_config, 'r') as current_config_connection:
            config_file_connection = current_config_connection.read()
            self.db_info = json.loads(config_file_connection)

        logger.info("Config file for DB tables : {}".format(db_connection_config))

        with open(db_tables_config, 'r') as current_config_tables:
            config_file_tables = current_config_tables.read()
            self.schemas = json.loads(config_file_tables)

        logger.debug("Total number of schemas in this setup {}".format(len(self.schemas)))
        for schema in self.schemas:
            logger.debug("Loading schema {}".format(schema))
            all_tables = self.schemas[schema]
            for table in all_tables:
                table["table_owner"] = schema
                self.flat_tables_info.append(table)
