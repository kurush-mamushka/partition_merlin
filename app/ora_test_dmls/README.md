# Oracle tables DML's for testing purpose

| Table Name  | Purpose  |
|---|---|
| table_test_dt  |  Partitioned by range, daily, partition key is date |
| table_test_dt_month  |  Partitioned by range, monthly, partition key is date |
| table_test_dt_num  |  Partitioned by range, daily, partition key is date represented as number  |
| test_dt_num_month  |  Partitioned by range, monthly, partition key is date represented as number  |
| test_list_partitions |  Partition by list, monthly, date represented as number |

For testing purpose we used Oracle XE 18, since this version of Oracle supports partitions.

Url for docker file: https://github.com/fuzziebrain/docker-oracle-xe
