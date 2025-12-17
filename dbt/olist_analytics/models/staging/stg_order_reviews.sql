with source as (
    select * from {{ source('olist_raw', 'reviews') }}
),

renamed as (
    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        cast(review_creation_date as timestamp) as created_at,
        cast(review_answer_timestamp as timestamp) as answer_timestamp
    from source
)

select * from renamed