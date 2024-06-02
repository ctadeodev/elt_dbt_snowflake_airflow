# ELT Pipeline using dbt, Snowflake, and Airflow

This project demonstrates how to build a simple ELT pipeline that uses [dbt(Data Build Tool)](#https://www.getdbt.com/) for building a data model in [Snowflake](#https://www.snowflake.com/en/) and is orchestrated using [Apache Airflow](#https://airflow.apache.org/). This follows the [tutorial](https://www.youtube.com/watch?v=OLXkGB7krGo&t=71s) shared by [jayzern](https://www.youtube.com/@jayzern).

## Table of contents
[1. Snowflake setup](#1-snowflake-setup)  
[2. dbt project setup](#2-dbt-project-setup)  
[3. Building our data model](#3-building-the-data-model)  
[4. Orchestrating using Airflow](#4-orchestrating-using-airflow)  

---

## 1. Snowflake setup
We will be using Snowflake for our data warehouse and the source data will be Snowflake's sample data, TPCH_SF1. 

#### Setup access control
For access control, we will use Snowflake's RBAC (Role-based Access Control). Access privileges are assigned to role dbt_role, which is in turn assigned to our user \<Snowflake username\>.

```sql
use role accountadmin;
create role if not exists dbt_role;
grant role dbt_role to user <Snowflake username>;
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

---

## 2. dbt project setup
To be able to run dbt-core projects as Apache Airflow DAGs, we are going to use [astronomer-cosmos](#https://github.com/astronomer/astronomer-cosmos). We will also use [astro-cli](#https://github.com/astronomer/astro-cli) to setup our project. To create and run our dbt project manually, we are going to use [dbt-core](#https://docs.getdbt.com/docs/core/installation-overview)

Initialize project by running `astro dev init`
```bash
$ curl -sSL install.astronomer.io | sudo bash -s  # install astro-cli
$ mkdir elt_project && cd elt_project
$ astro div init
```
this will generate a basic astro project directory:
```
elt_project 
├── dags
│   ├── exampledag.py
├── tests
│   ├── dags
│       ├── test_dag_example.py
├── Dockerfile
├── include
├── packages.txt
├── plugins
└── requirements.txt
```
now let's create our dbt project
```bash
$ python -m venv dbt-env  # create the environment
$ source dbt-env/bin/activate  # activate the environment
$ python -m pip install dbt-core dbt-snowflake  # install dbt-core and dbt adapter
$ mkdir dags/dbt && cd dags/dbt
$ dbt init  # this will return a prompt to setup your dbt project
```
`dbt init` will return a prompt which will ask you about your project details, enter the following:
```bash
Project name: data_pipeline
Database: Snowflake  # select appropriate option number
Account: (https://<this_value>.snowflakecomputing.com)
User: <Snowflake username>
Authentication type ([1] password, [2] keypair, [3] sso): 1  # select password authentication type
Password: <Snowflake password>
Warehouse: dbt_wh
Database: dbt_db
Schema: dbt_schema
Threads: 10
```
this will generate a basic dbt project directory:
```
data_pipeline
├── analyses
├── macros
├── models
│   ├── example
│       ├── my_first_dbt_model.sql
│       ├── my_second_dbt_model.sql
│       ├── schema.yml
├── seeds
├── snapshots
├── tests
├── .gitignore
└── dbt_project.yml
└── README.md
```

---

## 3. Building the data model
We will be using the orders and lineitem table from the __TPCH_SF1__ Snowflake sample data to build our orders data model.
![data model](res/data-model.png?raw=true)

Update dbt_project.yml:
```yaml
models:
  data_pipeline:
    staging:
      +materialized: view
      snowflake_warehouse: dbt_wh
    marts:
      +materialized: table
      snowflake_warehouse: dbt_wh
```
Add a packages.yml in the dbt project to include the package we are going to need for the project and run `dbt deps` to install these packages.
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```
```bash
$ dbt deps
```

Remove the sample models and create the needed sources, models, macros, and tests:
```
data_pipeline
├── macros
│   ├── pricing.sql
├── models
│   ├── marts
│       ├── dim_order_items_summary.sql
│       ├── dim_order_items.sql
│       ├── fct_orders.sql
│       ├── orders.yml
│   ├── staging
│       ├── stg_tpch_line_item.sql
│       ├── stg_tpch_orders.sql
│       ├── tpch_sources.yml
├── tests
│   ├── fct_orders_date_valid.sql
│   ├── fct_orders_discount.sql
```
The `models/staging/tpch_sources.yml` will contain details about our sources. For this project, we will only use TPCH_SF1 *orders* and *lineitem* tables. We will also specify some generic tests for our sources here
```yaml
version: 2

sources:
  - name: tpch
    database: snowflake_sample_data
    schema: tpch_sf1
    tables:
      - name: orders
        columns:
          - name: o_orderkey
            tests:
              - unique
              - not_null
      - name: lineitem
        columns:
          - name: l_orderkey
            tests:
              - relationships:
                  to: source('tpch', 'orders')
                  field: o_orderkey
```
The `models/staging/stg_tpch_orders.sql` will contain the SQL command to load the orders table. We will only get *order_key, customer_key, status_code, total_price, and order_date* columns.
```sql
select
    o_orderkey as order_key,
    o_custkey as customer_key,
    o_orderstatus as status_code,
    o_totalprice as total_price,
    o_orderdate as order_date
from
    {{ source('tpch', 'orders') }}
```
The `models/staging/stg_tpch_line_item.sql` will contain the SQL command to load the lineitem table. We will only get *order_key, part_key, line_number, quantity, extended_price, discount_percentage, and tax_rate* columns. We will also add a surrogate key, *order_item_key*, based on order_key and line_number.
```sql
select
    {{
        dbt_utils.generate_surrogate_key([
            'l_orderkey',
            'l_linenumber'
        ])
    }} as order_item_key,
    l_orderkey as order_key,
    l_partkey as part_key,
    l_linenumber as line_number,
    l_quantity as quantity,
    l_extendedprice as extended_price,
    l_discount as discount_percentage,
    l_tax as tax_rate
from
    {{ source('tpch', 'lineitem') }}
```
The `models/marts/dim_order_items.sql` will contain the SQL command to load order dimension table by joining the *stg_tpch_orders* and *stg_tpch_line_item* on *order_key*. Aside from the columns from stg_tpch_orders and stg_tpch_line_item, we will also include the calculated column *item_discount_amount*.
```sql
select
    line_item.order_item_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    orders.order_key,
    orders.customer_key,
    orders.order_date,
    {{ discounted_amount('line_item.extended_price', 'line_item.discount_percentage')}} as item_discount_amount
from
    {{ ref('stg_tpch_orders') }} as orders
join
    {{ ref('stg_tpch_line_item') }} as line_item
on orders.order_key = line_item.order_key
order by
    orders.order_date
```
The `models/marts/dim_order_items_summary.sql` will contain the SQL command to load order summary dimention table. It will show the *gross_item_sales_amount* and *item_discount_amount* for each order_key.
```sql
select
    order_key,
    sum(extended_price) as gross_item_sales_amount,
    sum(item_discount_amount) as item_discount_amount
from
    {{ ref('dim_order_items') }}
group by
    order_key
```
The `models/marts/fct_orders.sql` will contain the SQL command to load orders fact table.
```sql
select
    orders.*,
    order_item_summary.gross_item_sales_amount,
    order_item_summary.item_discount_amount
from
    {{ ref('stg_tpch_orders') }} as orders
join
    {{ ref('dim_order_items_summary') }} as order_item_summary
    on orders.order_key = order_item_summary.order_key
order by
    order_date
```
The `models/marts/orders.yml` will contain generic tests for our orders fact table. order_key will be tested for being unique and not_null as well as it's relationship to the order_key in stg_tpch_orders. The status_code will be tested with accepted_values specifying the only valid values to be 'P', 'O', and 'F'.
```yaml
models:
  - name: fct_orders
    columns:
      - name: order_key
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('stg_tpch_orders')
              field: order_key
              severity: warn
      - name: status_code
        tests:
          - accepted_values:
              values: ['P', 'O', 'F']
```
The `macros/pricing.sql` will contain the macro for calculating the discounted amount which is equal to __-1 * extended_price * discount_percentage__
```sql
{% macro discounted_amount(extended_price, discount_percentage, scale=2) %}
    (-1 * {{extended_price}} * {{discount_percentage}})::numeric(16, {{ scale }})
{% endmacro %}
```
dbt __singular tests__ are just SQL queries that will return failing rows.  

The `tests/fct_orders_discount.sql` will be used to test if fct_orders have no negative values for item_discount_amount.
```sql
select
    *
from
    {{ ref('fct_orders') }}
where
    item_discount_amount > 0
```
The `tests/fct_orders_date_valid.sql` will be used to test if fct_orders order_date is valid (should not be greater than current date and not less than 1990-01-01)
```sql
select
    *
from
    {{ ref('fct_orders') }}
where
    date(order_date) > CURRENT_DATE()
    or date(order_date) < date('1990-01-01')
```

To test the dbt project:
```bash
$ dbt test  # to run tests
$ dbt run  # to create views/tables
$ dbt build  # run tests and create views/tables
```
---

## 4. Orchestrating using Airflow
Setup astronomer-cosmos to run on Astro in local execution mode. It's recommended to use a virtual environment because dbt and Airflow can have conflicting dependencies. Let's add this to the generated `Dockerfile` file.
```yaml
FROM quay.io/astronomer/astro-runtime:11.4.0

# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate
```
To install cosmos and Airflow Snowflake providers, add the following to our `requirements.txt`:
```
astronomer-cosmos
apache-airflow-providers-snowflake
```
Setup DAG file `dags/dbt/dbt_dag.py`:
```python
import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name='default',
    target_name='dev',
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id='snowflake_conn',
        profile_args={'database': 'dbt_db', 'schema': 'dbt_schema'}
    )
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig('/usr/local/airflow/dags/dbt/data_pipeline',),
    operator_args={'install_deps': True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    schedule_interval='@daily',
    start_date=datetime(2024, 6, 1),
    catchup=False,
    dag_id='dbt_dag'
)
```
we can now start the Airflow instance by running:
```bash
$ astro dev start
```
A successful start should show the following:
```bash
Airflow Webserver: http://localhost:8080
Postgres Database: localhost:5432/postgres
The default Airflow UI credentials are: admin:admin
The default Postgres DB credentials are: postgres:postgres
```

Now open the Airflow webserver at http://localhost:8080/. After logging in, we should now be able to see our dbt_dag DAG in the webserver home.
![Airflow webserver home](res/dag.png?raw=true)

__Before we start the DAG, configure the snowflake connection first by going to Admin > Connections. Click + to add a new connection:__
![Add snowflake connection](res/snowflake_conn.png?raw=true)
Make sure to fill up the your own Snowflake Login, Password, and Account. Account can be found in your Snowflake accounts dashboard (https://\<this_value>.snowflakecomputing.com)

After setting the connection. We should now be able to run the `dbt_dag` DAG.
![dbt_dag successful run](res/dbt_dag_success.png?raw=true)
