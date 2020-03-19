-- Drop test tables
drop table test.table_test_dt purge;
drop table test.table_test_dt_num purge;
drop table test.table_test_dt_month purge;
drop table test.test_dt_num_month purge;
drop table test.test_list_partitions purge;
drop table test.test_partitions_mon purge;
drop table test.table_test_dt_num purge;
drop table test.test_partitions_mon_same purge;
-- Creating tables

create table test.table_test_dt
(
    dt  date,
    fid number
)
    tablespace test
    partition by range (dt)
(
    partition test_20200224 values less than (to_date(' 2020-02-25 00:00:00', 'syyyy-mm-dd hh24:mi:ss',
                                                      'nls_calendar=gregorian'))
        tablespace test
);

create table test.table_test_dt_month
(
    dt  date,
    fid number
)
    tablespace test
    partition by range (dt)
(
    partition MONTH_202001 values less than (to_date(' 2020-02-01 00:00:00', 'syyyy-mm-dd hh24:mi:ss',
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
    partition p_20200101 values less than (20200102)
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
    partition p_202001 values less than (202002)
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
    partition month_202001 values (201902)
);

create table test.test_partitions_mon
(
    start_time date,
    val        number(2)
)
    partition by range (start_time)
(
    partition P_14FEB2020 values less than (to_date('20200215', 'YYYYMMDD'))
);

create table test.test_partitions_mon_same
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
    partition p_202001 values (202001)
);
