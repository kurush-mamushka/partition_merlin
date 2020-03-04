sqls = {}
# sql
sqls['get_last_partition_id'] = """
    with max_part
         as (  select table_owner, table_name, max (partition_position) maxp
                 from dba_tab_partitions
                 where table_name not like 'BIN$%'
             group by table_owner, table_name)
        select dbtp.table_owner, dbtp.table_name, partition_name, tablespace_name, partition_position
            from dba_tab_partitions dbtp, max_part mxp
        where dbtp.table_owner = mxp.table_owner
           and dbtp.table_name = mxp.table_name
           and DBTP.PARTITION_POSITION = mxp.maxp
           and dbtp.table_owner not in ('SYS', 'DATA_CACHE', 'SYSTEM')
           and dbtp.table_owner = '{}'
           and dbtp.table_name = '{}'
            order by dbtp.table_owner, dbtp.table_name
        """
# this is SQL is for getting last partition key
sqls['get_last_partition_key_date'] = """
    DECLARE
    long_val long;
    hv varchar2(128);
    dt date;

   BEGIN
        select high_value into long_val from dba_tab_partitions where table_owner='{}' and table_name = '{}'
        and partition_position = {};
        hv := substr(long_val,1,128);
        execute immediate 'select ' || hv || '  from dual' into dt;
        :o_result := to_char(dt, 'MMDDYYYY');
    END;
"""
sqls['get_last_partition_key_date_as_number'] = """
    DECLARE
    long_val long;
    hv varchar2(128);
    dt varchar2(12);

   BEGIN
        select high_value into long_val from dba_tab_partitions where  table_owner='{}' and table_name = '{}' 
        and partition_position = {};
        hv := substr(long_val,1,128);
        execute immediate 'select ' || hv || '  from dual' into dt;
        :o_result := hv;
        END;
"""

sqls['get_index_name_and_index_tablespace'] = """
    select b.owner, a.index_name, a.tablespace_name from dba_ind_partitions a, dba_indexes b where b.table_owner = '{}'
    and b.table_name = '{}'
    and a.index_name = b.index_name
    and a.index_owner = b.owner
    and a.partition_position = {}
"""

sqls['preCheck'] = """
    select count(1) from all_tables where owner = '{}' and table_name = '{}'
"""
