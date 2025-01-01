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

#### Percentile
- Useful to measure experience at the extremes
- Example
    - P99 latency
        - How fast is the worst experience of a server
    - P10 engagement of active user

#### Metrics can be 'gamed' and need balance
- Experiments can move metrics up short-term but down long-term
- Notifications (eg)
    - Send more notifications to get more users in the short term
    - In the long term, more people will turn off setting
    - Need to have another experimentation to test the long term effect
- Fiddle with numerator or the denominator
- Novelty effects of experiments
    - Add new things, user will get excited, then goes down after a while
    - So you need to be aware on how long you should do your experiments 
    - P-hacking
- Increased user retention at extreme cost
    - Try add cost metrics to experiment to understand the ROI and diminishing returns

#### How does an experiment work?
- Make a hypothesis!
- Group assignment (think test vs control)
- Collect data
- Look at the differences between the groups

#### Hypothesis Testing
- The Null Hypothesis (H-naught)
    - No difference between groups
- The Alternative Hypothesis (H-one)
    - There is a difference from changes

#### Group Testing
- Assign test and control to group members
    - Who is eligible?
        - Are these users in a long-term holdout?
            - A persistent group that remain unexposed to specific feature/changes to measure long-term effects
        - Percentage of users to be experimented on
            - Big tech have luxury to experiment on small percentage on users

#### Group Assignment (this was super specific by zach)
- Example
    - Logged in users vs logged out users for website

#### Collect Data
- Collect data until you get statistically significant results (usually 1 month for big tech)
- Make sure to use stable identifiers
    - Hash IP to minimize collection of PII
    - Leverage stable ID in [Statsig](https://www.statsig.com/)
- The smaller the effect, the longer you'll have to wait
    - The effect sometimes could be very small that it doesn't matter (keep this in mind)
- Keeping in mind some effects may be so small you'll never ge a statsig results
- Do not UNDERPOWER experiments
    - More test cells means more data to collect
- Collect all events and dimensions you want to measure differences
    - Setup all logging events before collecting data