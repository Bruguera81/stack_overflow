
with users_temp as (

    select 
       id,
        display_name,
        location,
        up_votes

    from {{ source('my-wiki-data-bq','users') }}
    order by up_votes desc
)
    select * from users_temp limit 100;