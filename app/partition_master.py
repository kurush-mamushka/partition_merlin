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

    def __init__(self, **kwargs):
        # initial value is start point for generating partitions
        self.periods = 1
        self.kwargs = kwargs

    # this method is static so we will be able to call it w/out creating class (as far as this needed now
    @staticmethod
    def ora2pythonDT(ora_fmt: str) -> str:
        # translate oracle date format into Python date format, only YYYY, YY, MM and DD are supported
        logger.debug("ora2pythonDT: {} ".format(ora_fmt))
        # change date format from Oracle to Python one so we can increment it later
        ora2python_translation = (('YYYY', '%Y'), ('YY', '%y'), ('MM', '%m'), ('MON', '%b'), ('DD', '%d'))
        for fOra, fPython in ora2python_translation:
            ora_fmt = ora_fmt.replace(fOra, fPython)
        return ora_fmt

    @staticmethod
    def get1stDayNextMonth(dt: datetime) -> datetime:
        # get 1st day of the next month for monthly partitions
        return (dt.replace(day=1) + timedelta(days=32)).replace(day=1)

    @staticmethod
    def getDifferenceMonth(dtEnd: datetime, dtStart: datetime) -> int:
        if dtStart < dtEnd:
            multiplier = 1
        else:
            multiplier = -1
        return abs((dtEnd.year - dtStart.year) * 12 + dtEnd.month - dtStart.month) * multiplier

    def addNextPeriod(self, dt: datetime, longetivity: int) -> datetime:
        # add period to current date,
        # supported: day, week, month
        res = None
        if longetivity == 'day':
            res = dt + timedelta(days=1)
        if longetivity == 'week':
            res = dt + timedelta(weeks=1)
        if longetivity == 'month':
            res = self.get1stDayNextMonth(dt)
        return res

    def generateSQLs(self):
        latest_partition_key = self.kwargs.get('latest_partition_date')
        suffix = self.kwargs.get('partition_name_suffix')
        prefix = self.kwargs.get('partition_name_prefix')
        partition_longetivity = self.kwargs.get('partition_key_longetivitity')
        # check partitioning type, by default (not defined) is range, we can have LIST now
        partitioning_type = self.kwargs.get('partitioning_type', 'range')
        ora_date_format = self.kwargs.get('ora_date_format')
        py_dt_format = self.ora2pythonDT(ora_date_format)
        logger.debug("Oracle date format: {}".format(ora_date_format))
        logger.debug("Python date format: {}".format(py_dt_format))
        table_owner = self.kwargs.get('table_owner')
        table_name = self.kwargs.get('table_name')
        partition_key_type = self.kwargs.get('partition_key_type', 'date')
        latest_data_tablespace = self.kwargs.get('latest_data_tablespace', None)
        dtNow = datetime.now()
        logger.debug("Latest partition key: {}".format(latest_partition_key))
        logger.debug("Today date is {}".format(dtNow))
        partition_name_same_with_value = self.kwargs.get('partition_name_same_with_value', 'False') == 'True'
        # get periods if exists or set it as default value in this class
        self.periods = self.kwargs.get('periods', self.periods)
        logger.info(
            "Working on table {}.{} for [{}] periods with period type [{}] and partition type {}.".format(table_owner,
                                                                                                          table_name,
                                                                                                          self.periods,
                                                                                                          partition_longetivity,
                                                                                                          partitioning_type))
        # 2DO add
        current_difference = None
        if partition_longetivity == 'day':
            current_difference = (latest_partition_key - dtNow).days
        elif partition_longetivity == 'month':
            current_difference = self.getDifferenceMonth(latest_partition_key, dtNow)
        else:
            logger.critical("Unknown partition longetivity. Please fix it or add to feature")

        logger.info(
            "Latest partition date is {} and number for pre-existing periods are {}".format(latest_partition_key,
                                                                                            current_difference))

        if current_difference <= 0:
            self.periods = abs(current_difference) + self.periods
        else:
            self.periods = self.periods - current_difference

        if self.periods <= 0:
            logger.info("Looks like we are good with current table {}.{}".format(table_owner, table_name))
        else:
            logger.info("Periods to add are: {}".format(self.periods))

        # replace this with function returning partition name and partition date (+1)
        new_partition_name_dt = latest_partition_key
        new_partition_name_str = datetime.strftime(new_partition_name_dt, py_dt_format)
        new_partition_date_dt = latest_partition_key

        for n in range(0, self.periods):
            # we should have 2 variables:
            # partition name in format that we have in setup
            # and real date for partition bound values
            new_partition_date_dt = self.addNextPeriod(new_partition_date_dt, partition_longetivity)
            new_partition_date_str = datetime.strftime(new_partition_date_dt, py_dt_format)
            if partition_name_same_with_value:
                new_partition_name_dt = new_partition_date_dt
            if n > 0 and not partition_name_same_with_value:
                # if this is not 1st iteration - add one period to partition name
                new_partition_name_dt = self.addNextPeriod(new_partition_name_dt, partition_longetivity)
            new_partition_name_str = datetime.strftime(new_partition_name_dt, py_dt_format)

            # here we need to work with new parameter
            # partition_name_same_with_value
            new_partition_name = prefix + new_partition_name_str + suffix

            pre_sql = 'alter table {}.{} add partition '.format(table_owner, table_name)
            # here we should add analysis of key type, if this date OR number as date
            values_sql = ''
            if partitioning_type == 'range':
                values_sql = ' values less than ('
            elif partitioning_type == 'list':
                values_sql = ' values ('
            else:
                logger.critical(
                    "Not implemented type of partitioning {}. Please implement it.".format(partitioning_type))

            if partition_key_type == 'date':
                values_sql += " to_date('{}', '{}'))".format(new_partition_date_str.upper(), ora_date_format)
            elif partition_key_type == 'date_as_number':
                values_sql += new_partition_date_str + ')'

            if latest_data_tablespace is not None:
                values_sql = values_sql + ' tablespace {}'.format(latest_data_tablespace)

            # now let's do indexes
            index_sql = ''
            for index_item in self.kwargs['indexes_list']:
                index_sql = '\nalter index {}.{} modify default attributes tablespace {}'.format(index_item[0],
                                                                                                 index_item[1],
                                                                                                 index_item[2], )
            # ora_date_format _
            yield pre_sql + new_partition_name.upper() + values_sql + index_sql