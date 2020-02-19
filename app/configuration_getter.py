import json
from loguru import logger


class GetConfig:
    config = {}
    db_info = {}
    flat_tables_info = []

    def __init__(self, file_name):
        logger.info("Config file: {}".format(file_name))
        with open(file_name, 'r') as current_config:
            config_file = current_config.read()
            self.config = json.loads(config_file)

        all_schemas = self.config['schemas']
        self.db_info = self.config['db_parameters']
        logger.debug("Total number of schemas in this setup {}".format(len(all_schemas)))
        for schema in all_schemas:
            all_tables = all_schemas[schema]["tables"]
            for table in all_tables:
                table["table_owner"] = schema
                self.flat_tables_info.append(table)