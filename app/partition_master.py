from datetime import datetime, timedelta
from loguru import logger


class PartGenerator:
    # here is partition keys
    # date as is
    key_date = 'date'
    # date represent as a number not a date
    key_number_as_date = 'number_as_date'
    # this is to redefine oracle DDMMYYYY
    oracle_date_format_python = '%m%d%Y'

    # here is partition increment keys values

    def __init__(self, key_type, cnt, **kwargs):
        # initial value is start point for generating partitions
        self.key_type = key_type
        self.periods = 12
        self.kwargs = kwargs

    # this method is static so we will be able to call it w/out creating class (as far as this needed now
    @staticmethod
    def ora2pythonDT(ora_fmt):
        logger.debug("ora2pythonDT: {} ".format(ora_fmt))
        # change date format from Oracle to Python one so we can increment it later
        p_fmt = ''
        p_fmt = ora_fmt.replace('YYYY', '%Y')
        p_fmt = p_fmt.replace('YY', '%y')
        p_fmt = p_fmt.replace('MM', '%m')
        p_fmt = p_fmt.replace('DD', '%d')
        return p_fmt

    def get1stDayNextMonth(self, dt):
        # get 1st day of the next month for monthly partitions
        return (dt.replace(day=1) + timedelta(days=32)).replace(day=1)

    def getDifferenceMonth(self, dtStart, dtEnd):
        return (dtEnd.year - dtStart.year) * 12 + dtEnd.month - dtStart.month


    def addNextPeriod(self, dt, longetivity):

        if longetivity == 'day':
            res = dt + timedelta(days=1)
        if longetivity == 'week':
            res = dt + timedelta(weeks=1)
        if longetivity == 'month':
            res = self.get1stDayNextMonth(dt)
        return res

    def generateSQLs(self):
        if 'latest_partition_date' in self.kwargs:
            new_partition_name_dt = self.kwargs['latest_partition_date']
        if 'partition_name_suffix' in self.kwargs:
            suffix = self.kwargs['partition_name_suffix']
        else:
            suffix = ''
        if 'partition_name_prefix' in self.kwargs:
            prefix = self.kwargs['partition_name_prefix']
        else:
            prefix = ''
        #
        if 'partition_key_longetivitity' in self.kwargs:
            partition_longetivity = self.kwargs['partition_key_longetivitity']

        if 'ora_date_format' in self.kwargs:
            ora_date_format = self.kwargs['ora_date_format']
            py_dt_format = self.ora2pythonDT(ora_date_format)
            logger.debug(ora_date_format)
            logger.debug(py_dt_format)

        if 'table_owner' in self.kwargs:
            table_owner = self.kwargs['table_owner']
        if 'table_name' in self.kwargs:
            table_name = self.kwargs['table_name']
        if 'partition_key_type' in self.kwargs:
            partition_key_type = self.kwargs['partition_key_type']

        logger.info(
            "Working on table {}.{} for {} periods with period type [{}].".format(table_owner, table_name, self.periods,
                                                                                  partition_longetivity))
        dtNow = datetime.now()
        logger.debug("Today date is {}".format(dtNow))
        logger.debug("New partition name dt: {}".format(new_partition_name_dt))

        if 'periods' in self.kwargs:
            self.periods = self.kwargs['periods']
        # 2DO add
        if partition_longetivity == 'day':
            current_difference = (dtNow - new_partition_name_dt).days
        elif partition_longetivity == 'month':
            current_difference = self.getDifferenceMonth(dtNow, new_partition_name_dt)
        else:
            logger.critical("Unknown partition longetivity. Please fix it or add to feature")

        logger.info("Current date is {} and number for pre-existing periods are {}".format(new_partition_name_dt,
                                                                                           current_difference))

        if current_difference < 0:
            self.periods = abs(current_difference) + self.periods
        else:
            self.periods = self.periods - current_difference
        if self.periods <= 0:
            logger.info("Looks ike we are good with current table {}.{}".format(table_owner, table_name))
        else:
            logger.info("Periods to add are: {}".format(self.periods))

        # replace this with function returning partition name and partition date (+1)
        for n in range(0, self.periods):
            # logger.debug("Latest date: {} to be formated to {}".format(new_partition_name_dt, py_dt_format))
            # logger.debug("Python date: {}".format(new_partition_name_dt))
            # we should feel 2 variables:
            # partition name in format that we have in setup
            # and real date for partition bound values


            new_partition_name_dt = self.addNextPeriod(new_partition_name_dt, partition_longetivity)
            new_partition_name_str = datetime.strftime(new_partition_name_dt, py_dt_format)
            new_partition_date_dt = self.addNextPeriod(new_partition_name_dt, partition_longetivity)
            new_partition_date_str = datetime.strftime(new_partition_date_dt, self.oracle_date_format_python)

            new_partition_name = prefix + new_partition_name_str + suffix

            pre_sql = 'alter table {}.{} add partition  '.format(table_owner, table_name)
            # here we should add analysis of key type, if this date OR number as date
            values_sql = ' values less than ('

            if partition_key_type == 'date':
                values_sql += "to_date('{}', '{}'))".format(new_partition_date_str, 'DDMMYYYY')
            elif partition_key_type == 'date_as_number':
                values_sql += new_partition_name_str + ')'

            # now let's do indexes
            index_sql = ''
            for index_item in self.kwargs['indexes_list']:
                index_sql = '\nalter index {}.{} modify default attributes tablespace {}'.format(index_item[0],
                                                                                                 index_item[1],
                                                                                                 index_item[2], )
            # ora_date_format _
            yield pre_sql + new_partition_name + values_sql + index_sql
