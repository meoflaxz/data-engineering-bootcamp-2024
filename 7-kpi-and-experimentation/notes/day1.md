# BUILDING GOOD METRICS FOR BUSINESS

#### Why Do Metrics Matter At All?
- Up to each company
    - Some data driven companies will focus more on metrics eg Facebook
- Metrics provide insight, visibility and storytelling

#### Key Points
- Metrics play a huge part in data modelling
    - Metrics can be a little clunky if starts to involve count and aggregation
- Make sure metrics can't be gamed
    - Metrics go up does not mean good??
- The inner workings of experimentation
- The difference between feature gates and experimentation
- How do you plug metrics into experimentation framework

#### Complex Metrics Means Complex Data Modelling
- Don't let data scientist dictate your data model
    - This is true on when they ask you to build a complex model eg rolling sum, percentile
        - Anything that goes beyond simple group by, sum, count.
    - Just need to consider, just give them raw aggregates, that part is their job, not you

#### Types of Metrics
- Aggregates / Counts
    - Your most do job as data engineer
    - Your performance review is based on you doing data engineering, not data science 
- Ratios
- Percentiles
    - Keep the metrics simple because if you (DE) define a metric, you also need to know the answer for the questions related

#### Aggregates / Counts
- Your swiss army knife
    - No of user, no of events
    - Bring some dimension that focuses on the business objective

#### Ratios
- Data engineers should supply numerators and denominators, NOT THE RATIOS ITSELF
    - This is data scientist job because of the statistical knowledge
- Examples
    - Conversion rate
    - Purchase rate
    - Cost to acquire a customer