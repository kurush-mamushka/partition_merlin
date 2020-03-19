# Partition Merlin for Oracle Partition Maintenance (DB independent) [`stupidly simple`]

Implemented:

* [x] Add partition by range/list (simple one value, no complexity yet), partition key should be:
    * [x] Date
    * [x] Date, represented by number (YYYYMM for example for Monthly Partitions, number will be 202001)
* [x] Add parameter to config file for number of periods to be added (like 30 (days/months/etc))
* [x] List partitioning (simple)
* [x] Add username/password via command line
* [x] Pass config file via command line
    * [x] Split DB connection config and Tables config into 2 different files, so tables config can be used for different environments


**2DO (or is in progress)**
* [?] Add parameter to ignore errors (non existing tables), for example table exists in DEV but not exists in PROD

**Must**

* [x] Add username/password via command line
* [x] Pass config file via command line / split config files into 2
* [x] Add parameter to config file for number of periods to be added (like 30 (days/months/etc))
* [x] Add parameter to name partition in same format as date (so for date like 02/15/2020 rerpesented as number 20200215 partition name will be equal to date)
* [ ] Add partition list mode with 2/3 values, like 2019,01 for 2019 YYYY and 01 for MM

**Would**
* [ ] Add safety checks for config file
* [ ] Exception handling and reporting
* [ ] Email notifications
* [x] Add weekly type of partitions
* [ ] Add partition tablespace rotation schemas (like weekly, daily rotation...)

