
with dbt_answers as  (

    select
        id as answers_id,
        title as answer_title,
        score,
        accepted_answer_id,
        creation_date as answers_creation_date,
        view_count,		
        tags,
        body,
        owner_user_id
        --sum(cast(answer_count as Numeric)) as No_Anws_Count
    from {{ source('my-wiki-data-bq','answers_temp') }}
)
    select * from dbt_answers