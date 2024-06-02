# ELT Pipeline using dbt (Data Build Tool), Snowflake, and Airflow

This project demonstrates how to build a simple ELT pipeline that uses dbt for building a data model in Snowflake and is orchestrated using Airflow. This follows the [tutorial](https://www.youtube.com/watch?v=OLXkGB7krGo&t=71s) shared by [jayzern](https://www.youtube.com/@jayzern).


## Table of contents
[1. Snowflake setup](#snowflake_setup)
[2. Building our data model](#building_our_data_model)
[3. Orchestrating using Airflow](#orchestrating_using_airflow)

### Snowflake setup
We will be using Snowflake for our data warehouse and the source data will be Snowflake's sample data, TPCH_SF1.

#### Setup access control
For access control, we will use Snowflake's RBAC (Role-based Access Control). Access privileges are assigned to role dbt_role, which is in turn assigned to our user <USER_NAME> (Snowflake username).

```sql
use role accountadmin;
create role if not exists dbt_role;
grant role dbt_role to user <USER_NAME>;
```

#### Setup warehouse and database
```sql
create warehouse dbt_wh with warehouse_size='x-small';
create database if not exists dbt_db;
grant usage on warehouse dbt_wh to role dbt_role;
grant all on database dbt_db to role dbt_role;
use role dbt_role;
create schema if not exists dbt_db.dbt_schema;
```