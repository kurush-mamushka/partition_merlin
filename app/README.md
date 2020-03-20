# Partition Merlin for Oracle Partition Maintenance (DB independent) [`stupidly simple`] :relieved:

## Why

To support different databases, partitions schemes and environments we can use different partition maintenance methods.

My first approach was to create smart scriptm which will guess if there is a date in partition name, prefix, suffix and etc.
It was pretty easy to run but it was limited script and fetures. Also with added complexity of different partition methons and naming standards, keys and keys values script become overcomplicated and hard to support / add feature.

This is second approach, I would use stupidly simple and idiot prood setup for different partitions methods and naming standarts.



## Plans, what is in progress and what was done.

**Implemented:**

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
* [x] Add parameter to name partition in same format as date (so for date like 02/15/2020 represented as number 20200215 partition name will be equal to date)
* [ ] Add partition list mode with 2/3 values, like 2019,01 for 2019 YYYY and 01 for MM

**Would**

* [ ] Add safety checks for config file
* [ ] Exception handling and reporting
* [ ] Email notifications
* [x] Add weekly type of partitions
* [ ] Add partition tablespace rotation schemas (like weekly, daily rotation...)

## Config helper

###Setup(config) Files

**DB Connection info:**

Using **service_name** with direct connection mode: 

```
{
    "connection_type": "direct",
    "host_name": "localhost",
    "service_name": "XE",
    "port": "1521"
}
```

Using **sid** with direct connection mode:

```
{
    "connection_type": "direct",
    "host_name": "localhost",
    "sid": "XE",
    "port": "1521"
}
```

Using **tnsnames**:

```
{
    "connection_type": "tnsnames",
    "conenction_name": "test"
}
```

**Tables config:**
