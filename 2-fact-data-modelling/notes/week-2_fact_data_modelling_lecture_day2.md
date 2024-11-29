# BLURRY LINE BETWEEN FACT AND DIMENSION

## Is it a fact or a dimension?

- Did a user log in today?
    - dim_is_active - dimension based on log in event fact
    - dim_is_activated - representation of state, state-driven, not activity driven
- If you create a dimension based on aggregation of fact, consider CASE WHEN to bucketized the aggregated facts to reduce the cardinality
    - High cardinality can make analysis more complicated - eg finding pattern among user
    - How to bucketized?
        - Check on distribution - median, mean, box plot
        - Based on statistical distribution - dont just pull out from thin air
        - Also good to have the range value for bucketization, so end user still can change if they want
- TLDR, it can be BOTH!


## Properties of Facts vs Dimensions

- Dimensions
    - Usually GROUP BY when doing analytics
    - Can be 'high cardinality' or 'low cardinality'
    - Generally come from a snapshot of state
- Facts
    - Usually aggregated when doing analytics by things SUM, AVG, COUNT
    - Almost always higher volume than dimensions, although sometimes lower volume, think 'rare_events'
    - Generally come from events and logs

## Airbnb Example

- Is the price of a night on Airbnb a fact or a dimension?
- The host can set the price which sounds like an event.
- IT can easily be SUM, AVG, COUNT'd like regular facts.
- Prices on Airbnb are float, therefore extremely high cardinality.
- But actually this is an example of a DIMENSION.
- Why? because host can change the setting which impacted the price
- So think of a fact like something that has to be logged, while dimension comes from state of things
- Therefore price being derived from settings is a dimension

## Boolean / Existence-based Fact / Dimension - Dimension that based on Fact

- dim_is_active, dim_bought_something, etc
    - Usually on daily/hour basis
- dim_has_ever_booked, dim_ever_active, dim_ever_labeled_fake
    - A type of 'ever' dimension that once log, never goes back
    - Example from Airbnb - active listing who has never been booked
- 'Days since' dimension (days_since_last_active, days_since_sign_up, etc)
    - Very common in retention analytical patterns
    - Look for J Curve

## Categorial Fact/Dimension

- Scoring class in week 1
    - Dimension that is derived from fact data
- Often calculated with CASE WHEN logic and 'bucketizing'

## Should you use dimensions or facts to analyze user?

- Is the 'dim_is_activated' state or 'dim_is_active' logs a better metric?

- Is the 'dim_is_activated' state or 'dim_is_active' logs a better metric?
    - Great data question because it depends!
- It is the difference between 'signups' and 'growth' in some perspective
    - Sometimes you can also have both to analyze ratio between those 2

## The Extremely Efficient Date List Data Structure

- Extremely efficient way to manage user growth
- Imagine a cumulated schema
    - user_id
    - date
    - dates_active - array of all recent days that user was active
- You can turn the structure to this
    - user_id, date, datelist_int
    - 32, 2023-01-01, 100000001000000001
    - where 1s in integer represent the activity for 2023-01-01
