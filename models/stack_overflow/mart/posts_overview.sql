
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
        EXTRACT(YEAR FROM q.creation_date) AS question_year,
        EXTRACT(MONTH FROM q.creation_date) AS question_month,
        q.view_count as question_view_count,
        q.tags as question_tag,
        pa.answers_id,
        pa.score as answers_score,
        pa.answers_creation_date as answers_date,
        pa.view_count as answer_view_count,
        pa.body as answer_body
    from questions q, posts_answers pa
    where q.question_accepted_answer_id=pa.answers_id
),
answer_and_user as 
--post answer user
(
    select
        pu.user_id as answer_user_id,
        pu.user_name as answer_user_name,
        pu.up_votes as user_votes,
        pa.answers_id as answer_id,
        pa.accepted_answer_id
    from posts_answers pa, post_users pu
    where pa.owner_user_id=pu.user_id
),
post_overview as
(
    select 
        qa.question_id,
        qa.question_title,
        qa.question_body,
        qa.question_accepted_answer_id,      
        qa.creation_date as question_creation_date,
        qa.question_year*100+question_month as question_year_month,
        DATETIME_DIFF(TIMESTAMP(qa.answers_date),TIMESTAMP(qa.creation_date), HOUR) as QA_data_diff,
        --DATETIME_DIFF(DATETIME
        qa.question_view_count,
        qa.answer_view_count,
        qa.question_tag,
        qa.answers_date,
        qu.question_name_owner,
        qu.user_location as question_user_location,
        au.answer_user_id,
        au.answer_user_name,
        au.user_votes
    from questions_and_users qu, questions_and_answers qa, answer_and_user au 
    where  
    au.answer_id=qa.answers_id
    and qu.question_id=qa.question_id
    and qa.question_year*100+question_month  = 202203
)

select * from post_overview 

-- With tem_date as 
-- (
-- select 
--   EXTRACT(YEAR FROM question_creation_date) AS Year,
--   EXTRACT(MONTH FROM question_creation_date) AS Month
-- from `dbt_stack_overflow.posts_overview`
-- )

-- select Year*100+Month from tem_date;

