# FACT DATA MODELLING

## What is Fact?
- Something that happened or occured.
    - User login to app
    - Transaction is made
- Fact are not slowly changing which makes them easier to model than dimension in some respects

## What makes fact modelling hard?
- LOTS OF DATA
- Usually 10-100x volume of dimension data
- Fact data need a lot of context for effective analysis
    - Fact need another fact data to make sense
- Duplicates are way common than dimensional data
    - Can caused by data quality issues,
    - Sometimes can be genuine duplicates eg. 2 users clicking same link
    - Most pain of fact modelling is caused by duplication.

## How does fact modelling work?
- Normalization vs Denormalization
    - Normalization facts dont have any dimensional attributes, just IDs to join to retrieve data - to make performance faster
    - Denormalization facts have dimensional attributes for quicker analysis at the cost of more storage
- Both normalized and denormalized facts have place in this world
- Fact data and raw logs are not the same thing
    - Raw logs
        - Ugly schemas designed for online system that make data analysis sad
        - Potentially contains duplicates and other quality error - no quality guarantees
        - Usually have shorter retention
    - Fact data
        - Nice column names
        - Quality guaranteed - uniqueness, not null, etc
        - Longer retention

- How the work?
    - Think of Who, What, Where, When and How
        - Who - usually the IDs (this user clicked this button)
        - Where - most likely to bring in dimension (device_id)
        - How - very similar to where fields. He used iphone to make this click
        - What - nature of the fact (GENERATED, SENT, CLICKED, DELIVERED)
        - When - nature of the fact(event timestamp, event date, event time) - client site loggig for easier time
- Fact data should have quality guarantees
    - if didnt, analysis would just go to the raw logs
    - for example, what and when field shouldnt be null, coz the data doesnt make much sense if null.
- Fact data should generally be smaller than raw logs.
- Fact data should parse out hard-to-understand column
    - Could be some complex depending on the business
    - But generally should be easy to understand

## When you should model in dimension?
- Sometimes DENORMALIZATION is the way to go, not the cause

## How does logging fit into fact data?

