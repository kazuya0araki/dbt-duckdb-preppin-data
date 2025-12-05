with
  input_data_1 as (
    select
      Date as date,
      Release_Year as release_year,
      User_Review as user_review,
      Name as name,
      Price_Sold as price_sold,
      Place_Sold as place_sold
    from
      {{ ref("2025__challenge_1__pd__2025__challenge_1__input_1") }}
  ),

  input_data_2 as (
    select
      Date as date,
      Currency as currency,
      "Exchange Rate" as exchange_rate
    from
      {{ ref("2025__challenge_1__pd__2025__challenge_1__input_2") }}
  ),

  -- - The field ‘Name’ contains game titles and their developers. Fix this by creating two fields: ‘Game Title’ and ‘Developer’.
  -- - The ‘User Review’ field also contains two pieces of information; a star rating out of 5 and any comments left by customers. Extract only the star rating. If no rating is present, keep it as N/A. Keep only 5 star rated games.
  prep_input_data_1 as (
    select
      date,
      release_year,
      regexp_extract(user_review, '^([1-5])/5 \(made by (.*?)\)$', 1, 'i') as star_rating,
      user_review as review_comments,
      regexp_extract(name, '^(.*?) \(made by (.*?)\)$', 1, 'i') as game_title,
      regexp_extract(name, '^(.*?) \(made by (.*?)\)$', 2, 'i') as developer,
      price_sold,
      place_sold
    from
      input_data_1
  )