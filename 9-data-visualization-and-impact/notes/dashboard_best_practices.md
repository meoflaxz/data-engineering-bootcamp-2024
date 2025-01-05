# PERFORMANT DASHBOARD BEST PRACTICES

#### Best Practices
- Preaggregate data
    - Do GROUPING SETS syntax in SQL
- Don't do JOINS on the fly
    - Just don't
    - You should't thinking about master data here, no downstream other than human user
    - preaggregate and denormalize
- Use a low-latency storage solution like Druid
    - Don't build dashboard around S3
    - Apache Pinot
- If dashboard are slow, people are not gonna use it
    - 85% of businesses dashboard are used 1 time only and less

#### How to Build Dashboard That Makes Sense
- Think about who is your customer?
    - Execs (very easy to understand immediately)
        - No interaction for the most part
        - The dashboard and the screenshot of the dashboard is the same thing
    - Analysts (more charts, more density, more exploration capabilities)
        - More interactive

#### What are The Type of Questions That Can Be Asked?
- Top line questions
    - How many users we have?
    - How much revenue?
    - How much user spend time?
    - Simple dimension
- Trend questions
    - This year vs last year
    - Time component
- Composition questions
    - Percentage questions

#### What Numbers Matter?
- Total aggregates
    - COUNT(*)
- Time-based aggregates
    - COUNT(*) GROUP BY Year
- Time & Entity-based aggregates
    - COUNT(*) Active Users GROUP BY Year
- Derivative metrics (week-over-week, month-over-month, year-over-year)
    - Sometimes stakeholders may care more about this than the total aggregates
    - Lot more sensitive to change compared to normal aggregates
    - Volatile and easier to pick up trend earlier
    - Not recommended for day-to-day cause of the noise, also weekdays vs weekends
- Dimensional mix (%US vs %India, %Android vs %iPhone)
    - Sometimes the mix can change but total aggregates stay the same
- Retention / Survivorship(% left after N number of days)

#### Why Do These Numbers Matter?
- Total aggregates
    - Represent how good or bad the business is doing overall
    - Easily be used in marketing material (XX hits 2 billion users)
- Time-based aggregates
    - Catch trends earlier than total (bad quarter is potential signal of bad year)
    - Identify trends and growth
- Time & Entity-based aggregates
    - Easily plug into AB testing frameworks
    - Used by Data Scientist to look cut aggregated fact data in a way that is performant
        - They look why some metrics are going down
    - Often times included in daily master data
- Derivative metrics
    - More sensitive to changes compared to normal aggregates
    - % increase is better indicator than absolute number
    - YoY growth is quite important for business
- Dimensional mix
    - Identify impact opportunities
    - Spot trends in populations, not just over time
    - Really great for root cause analysis, say that you see numbers down on total aggregates, dimensional mix helps you understand the root cause
- Retention / Survivorship (% left after N number of days)
    - Also called J-curves / churn rate
    - Great at predicting lifetime customer value (LTV)
    - Understanding stickiness of the app you're building