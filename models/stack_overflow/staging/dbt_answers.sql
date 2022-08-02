
with temp_answers as  (

    select
        id as answers_id,
        creation_date as answers_creation_date,
        body,
        accepted_answer_id,
        last_editor_user_id
        --sum(cast(answer_count as Numeric)) as No_Anws_Count
    from {{ source('my-wiki-data-bq','posts_answers') }}
)
    select * from temp_answers limit 100;