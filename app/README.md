# Partition Merlin for partition maintenance (DB independent)

Implemented:

* Add partition by range, partition key should be:
    * date
    * date, represented by number (YYYYMM for exaple for Monthly Partitions, number will be 202001)

**2DO (or in progress)**

**Must**

* Add username/password via command line
* Pass config file via command line
* Add parameter to config file for number of periods to be added (like 30 (days/months/etc))

**Would**
* Add safety checks for config file
* Exception handling and reporting
* Email notifications
* Add weekly type of partitions
* Add partition rotation schemas (like weekly, daily rotation...)

