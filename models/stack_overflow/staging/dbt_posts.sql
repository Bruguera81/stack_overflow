
with posts_temp as (

    select 
        title, 
        score,
        accepted_answer_id,
        creation_date   as post_creation_date,
        view_count,
        tags
    from {{ source('my-wiki-data-bq','stackoverflow_posts') }}
)
    select * from posts_temp limit 100