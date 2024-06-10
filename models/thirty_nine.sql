{% if target.name == 'dev' %}
    {{ config(
        materialized='table',
        schema='DEV',
        database='DE_DEV'
    ) }}
{% elif target.name == 'qa' %}
    {{ config(
        materialized='table',
        schema='QA',
        database='DE_QA'
    ) }}
{% endif %}

-- Question
-- This query contains multiple, related iterations:
-- Iteration 1: Calculate the coefficient of variation and mean of every item and warehouse of two consecutive 
-- months
-- Iteration 2: Find items that had a coefficient of variation in the first months of 1.5 or large
-- Qualification Substitution Parameters:
-- YEAR.01 = 2001
-- MONTH.01 = 1

-- Query

with item_cte as (
    select i_item_sk
    from {{ source('de_project', 'item') }}
    -- limit 1000000000
),
warehouse_cte as (
    select w_warehouse_sk, w_warehouse_name
    from {{ source('de_project', 'warehouse') }}
    -- limit 1000000000
),
date_dim_cte as (
    select d_date_sk, d_moy, d_year
    from {{ source('de_project', 'date_dim') }}
    where d_year = 2001
    -- limit 1000000000
),
inventory_cte as (
    select inv_item_sk, inv_warehouse_sk, inv_date_sk, inv_quantity_on_hand
    from {{ source('de_project', 'inventory') }}
    -- limit 1000000000
),
joined_inventory_item as (
    select 
        inv.inv_item_sk,
        inv.inv_warehouse_sk,
        inv.inv_date_sk,
        inv.inv_quantity_on_hand
    from inventory_cte inv
    join item_cte it on inv.inv_item_sk = it.i_item_sk
),
joined_inventory_item_warehouse as (
    select 
        inv_item.inv_item_sk,
        inv_item.inv_warehouse_sk,
        inv_item.inv_date_sk,
        inv_item.inv_quantity_on_hand,
        wh.w_warehouse_name
    from joined_inventory_item inv_item
    join warehouse_cte wh on inv_item.inv_warehouse_sk = wh.w_warehouse_sk
),
joined_all as (
    select 
        inv_item_warehouse.inv_item_sk,
        inv_item_warehouse.inv_warehouse_sk,
        inv_item_warehouse.inv_date_sk,
        inv_item_warehouse.inv_quantity_on_hand,
        inv_item_warehouse.w_warehouse_name,
        dd.d_moy
    from joined_inventory_item_warehouse inv_item_warehouse
    join date_dim_cte dd on inv_item_warehouse.inv_date_sk = dd.d_date_sk
),
aggregated as (
    select 
        w_warehouse_name,
        inv_warehouse_sk,
        inv_item_sk,
        d_moy,
        stddev_samp(inv_quantity_on_hand) as stdev,
        avg(inv_quantity_on_hand) as mean
    from joined_all
    group by w_warehouse_name, inv_warehouse_sk, inv_item_sk, d_moy
),
cov_calculated as (
    select 
        w_warehouse_name,
        inv_warehouse_sk,
        inv_item_sk,
        d_moy,
        stdev,
        mean,
        case when mean = 0 then null else stdev/mean end as cov
    from aggregated
),
inv as (
    select *
    from cov_calculated
    where case when mean = 0 then 0 else stdev/mean end > 1
),
-- Combine the results of the two queries
combined_results as (
    select 
        inv1.inv_warehouse_sk,
        inv1.inv_item_sk,
        inv1.d_moy as d_moy1,
        inv1.mean as mean1,
        inv1.cov as cov1,
        inv2.inv_warehouse_sk as inv2_w_warehouse_sk,
        inv2.inv_item_sk as inv2_item_sk,
        inv2.d_moy as d_moy2,
        inv2.mean as mean2,
        inv2.cov as cov2
    from inv inv1
    join inv inv2 on inv1.inv_item_sk = inv2.inv_item_sk
        and inv1.inv_warehouse_sk = inv2.inv_warehouse_sk
    where inv1.d_moy = 1
        and inv2.d_moy = 2
        and inv1.cov > 1.5
    order by 
        inv1.inv_warehouse_sk,
        inv1.inv_item_sk,
        inv1.d_moy,
        inv1.mean,
        inv1.cov,
        inv2.inv_item_sk,
        inv2.d_moy,
        inv2.mean,
        inv2.cov
)
select * from combined_results
limit 100
