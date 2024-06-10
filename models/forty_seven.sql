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
-- Find the item brands and categories for each store and company, the monthly sales figures for a specified year, 
-- where the monthly sales figure deviated more than 10% of the average monthly sales for the year, sorted by 
-- deviation and store. Report deviation of sales from the previous and the following monthly sales.
-- Qualification Substitution Parameters
-- YEAR.01 = 1999
-- SELECTONE = v1.i_category, v1.i_brand, v1.s_store_name, v1.s_company_name
-- SELECTTWO = ,v1.d_year, v1.d_moy

-- Query

with item_cte as (
    select 
        i_item_sk, 
        i_category, 
        i_brand 
    from {{ source('de_project', 'item') }}
    limit 1000000000
),

store_sales_cte as (
    select 
        ss_item_sk, 
        ss_sold_date_sk, 
        ss_store_sk, 
        ss_sales_price 
    from {{ source('de_project', 'store_sales') }}
    limit 1000000000
),

date_dim_cte as (
    select 
        d_date_sk, 
        d_year, 
        d_moy 
    from {{ source('de_project', 'date_dim') }}
    limit 1000000000
),

store_cte as (
    select 
        s_store_sk, 
        s_store_name, 
        s_company_name 
    from {{ source('de_project', 'store') }}
    limit 1000000000
),

-- Join item and store_sales
item_store_sales_cte as (
    select 
        i.i_category, 
        i.i_brand, 
        ss.ss_sold_date_sk, 
        ss.ss_store_sk, 
        ss.ss_sales_price 
    from item_cte i
    join store_sales_cte ss
    on i.i_item_sk = ss.ss_item_sk
),

-- Join item_store_sales and date_dim
item_store_sales_date_cte as (
    select 
        iss.i_category, 
        iss.i_brand, 
        iss.ss_store_sk, 
        iss.ss_sales_price, 
        d.d_year, 
        d.d_moy 
    from item_store_sales_cte iss
    join date_dim_cte d
    on iss.ss_sold_date_sk = d.d_date_sk
),

-- Join item_store_sales_date and store
item_store_sales_date_store_cte as (
    select 
        issd.i_category, 
        issd.i_brand, 
        s.s_store_name, 
        s.s_company_name, 
        issd.d_year, 
        issd.d_moy, 
        issd.ss_sales_price 
    from item_store_sales_date_cte issd
    join store_cte s
    on issd.ss_store_sk = s.s_store_sk
),

-- Aggregate sales and calculate average monthly sales
v1 as (
    select 
        i_category, 
        i_brand, 
        s_store_name, 
        s_company_name, 
        d_year, 
        d_moy, 
        sum(ss_sales_price) as sum_sales, 
        avg(sum(ss_sales_price)) over (
            partition by i_category, i_brand, s_store_name, s_company_name, d_year
        ) as avg_monthly_sales, 
        rank() over (
            partition by i_category, i_brand, s_store_name, s_company_name 
            order by d_year, d_moy
        ) as rn 
    from item_store_sales_date_store_cte
    where d_year = 1999 
       or (d_year = 1998 and d_moy = 12) 
       or (d_year = 2000 and d_moy = 1)
    group by 
        i_category, 
        i_brand, 
        s_store_name, 
        s_company_name, 
        d_year, 
        d_moy
),

-- Self join v1 to get previous and next month sales
v2 as (
    select 
        v1.i_category, 
        v1.i_brand, 
        v1.s_store_name, 
        v1.s_company_name, 
        v1.d_year, 
        v1.d_moy, 
        v1.avg_monthly_sales, 
        v1.sum_sales, 
        v1_lag.sum_sales as psum, 
        v1_lead.sum_sales as nsum 
    from v1 
    left join v1 v1_lag 
    on v1.i_category = v1_lag.i_category 
       and v1.i_brand = v1_lag.i_brand 
       and v1.s_store_name = v1_lag.s_store_name 
       and v1.s_company_name = v1_lag.s_company_name 
       and v1.rn = v1_lag.rn + 1 
    left join v1 v1_lead 
    on v1.i_category = v1_lead.i_category 
       and v1.i_brand = v1_lead.i_brand 
       and v1.s_store_name = v1_lead.s_store_name 
       and v1.s_company_name = v1_lead.s_company_name 
       and v1.rn = v1_lead.rn - 1
)

select *
from v2
where d_year = 1999 
  and avg_monthly_sales > 0 
  and abs(sum_sales - avg_monthly_sales) / avg_monthly_sales > 0.1
order by sum_sales - avg_monthly_sales, avg_monthly_sales, sum_sales, psum, nsum
limit 100
