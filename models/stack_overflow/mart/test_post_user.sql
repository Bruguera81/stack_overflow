
with questions as 
(
    select 
        question_id,
        question_title,
        question_body,
        accepted_answer_id  as question_accepted_answer_id,
        question_comment_count,
        creation_date,
        question_users_id,
        view_count,
        tags
    from {{ ref('dbt_questions') }}
),
posts_answers as 
(
    select
        answers_id,
        answer_title,
        score,
        accepted_answer_id,
        answers_creation_date,
        view_count,		
        tags,
        body,
        owner_user_id 
    from {{ ref('dbt_answers') }}   
),
post_users as 
(
    select
        id as user_id,
        display_name as user_name,
        location,
        up_votes
    from {{ ref('dbt_users') }}
),
questions_and_users as 
--post creation user
(
    select 
        pu.user_id as question_user_id,
        pu.user_name as question_name_owner,
        pu.location as user_location,
        q.question_id,
        q.question_users_id,
        q.question_title,
        q.question_comment_count,
        q.tags as question_tag
    from questions q, post_users pu
    where q.question_users_id=pu.user_id
),
questions_and_answers as
--post answers
(
    select
        q.question_id,
        q.question_title,
        q.question_body,
        q.question_accepted_answer_id,
        q.creation_date,
        q.view_count as question_view_count,
        q.tags as question_tag,
        pa.answers_id,
        pa.score as answers_score,
        pa.answers_creation_date as answers_date,
        pa.view_count as answer_view_count,
        pa.body as answer_body
    from questions q, posts_answers pa
    where q.question_accepted_answer_id=pa.answers_id
)

select * from questions_and_answers

