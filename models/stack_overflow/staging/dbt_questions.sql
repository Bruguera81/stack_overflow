
with dbt_questions as 
(
    select 
        id as question_id,
        title as question_title,
        body as question_body,
        accepted_answer_id,
        comment_count as question_comment_count,
        creation_date,
        owner_user_id as question_users_id,
        view_count,
        tags
    from {{ source('my-wiki-data-bq','questions_temp') }}
) 
    select * from dbt_questions