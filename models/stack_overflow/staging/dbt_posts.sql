
with dbt_posts as (

    select 
        id,
        title, 
        owner_user_id,
        score,
        accepted_answer_id,
        creation_date   as post_creation_date,
        view_count,
        tags
    from {{ source('my-wiki-data-bq','post_temp') }}
)
    select * from dbt_posts