# Partition Merlin for Oracle Partition Maintenance (DB independent) [`stupidly simple`]

Implemented:

* Add partition by range, partition key should be:
    * date
    * date, represented by number (YYYYMM for example for Monthly Partitions, number will be 202001)
    * Add parameter to config file for number of periods to be added (like 30 (days/months/etc))
    * List partitioning

**2DO (or is in progress)**
* Add username/password via command line
* Pass config file via command line
* Split DB connection config and Tables config into 2 different files, so tables config can be used for different environments
* Add parameter to ignore errors (non existing tables), for example table exists in DEV but not exists in PROD

**Must**

* ~~Add username/password via command line~~
* Pass config file via command line / split config files into 2
* ~~Add parameter to config file for number of periods to be added (like 30 (days/months/etc))~~

**Would**
* Add safety checks for config file
* Exception handling and reporting
* Email notifications
* Add weekly type of partitions
* Add partition rotation schemas (like weekly, daily rotation...)

