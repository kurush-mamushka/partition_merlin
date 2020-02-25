-- Drop test tables
drop table test.table_test_dt purge;
drop table test.table_test_dt_month purge;
drop table test.table_test_dt_num purge;
drop table test.test_dt_num_month purge;
drop table test.test_list_partitions purge;

create table test.table_test_dt
(
    dt  date,
    fid number
)
    tablespace test
    partition by range (dt)
(
    partition test_20200101 values less than (to_date(' 2020-01-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss',
                                                      'nls_calendar=gregorian'))
        tablespace test
);

create table test.table_test_dt_month(
    dt  date,
    fid number
)
    tablespace test
    partition by range (dt)
(
    partition test_202001 values less than (to_date(' 2020-01-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss',
                                                    'nls_calendar=gregorian'))
        tablespace test
);

create table test.table_test_dt_num
(
    dt  number(10, 0),
    fid number
)
    tablespace test
    partition by range (dt)
(
    partition p_20200101 values less than (20200101)
        tablespace test
);

create table test.test_dt_num_month
(
    dt  number(10, 0),
    fid number
)
    tablespace test
    partition by range (dt)
(
    partition p_201912 values less than (202001)
        tablespace test
);

create table test.test_list_partitions
(
    id number,
    dt number(10, 0)
)
    tablespace test
    partition by list
(
    dt
)
(
    partition month_201901 values (201902)
);

