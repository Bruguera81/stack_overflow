
with dbt_users as (

    select 
       id,
        display_name,
        location,
        up_votes

    from {{ source('my-wiki-data-bq','users_temp') }}
    order by up_votes desc
)
    select * from dbt_users