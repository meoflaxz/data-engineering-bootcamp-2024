# TECHNICAL DEBT IN DATA ENGINEERING

#### Signals of Tech Debt in Data Engineering
- Painful pipelines that break or are delayed
- Large cloud bills
- Multiple sources of truth
- Unclear data sets that aren't documented

#### Path Forward for Painful Pipelines
- The only thing better than optimized is deprecated
    - Sometimes this is the right play
    - If the data is not being used, why maintain it?
- Is there anything that can be done?
    - New technology/framework
    - Better data modelling
    - Bucketing?
    - Sampling?
- Deprecating without a replacement can be a hard sell!

#### Technology Upgrades for Pipelines
- Hive to Spark migration?
    - Often sees an order of magnitude increase in efficiency
    - Esp pipelines that use high-cardinality GROUP BY and/or JOIN
    - Spark is more efficient but less reliable than Hive due to on-memory vs on-disk thingy

#### Sampling
- When should you sample?
    - Sometimes you don't need a full dataset
    - If directionality and concept are important, this is where sampling data is good enough
    - Consult with data scientists to guarantee you get the best sample of dataset
    - How? 2 ways of sampling (example)
        - Take a small percentage users of all requests
        - Take all requests for small percentage of users
- When not to sample?
    - If you need entire data set for auditing purposes, sampling is not a good idea

#### Bucketing
- Consider bucketing when
    - You have expensive high-cardinality GROUP BY or JOIN
        - Bucketing avoids shuffle which is expensive
- Not worth it if small data
    - Expensive i/o

#### Large cloud bills
- IO is usually the number one cloud cost!
    - Check your downstream usage
- Followed by compute and then storage
- Why IO?
    - Duplicative data models
        - Different terms across team, causing communication overhead
    - Inefficient pipelines (use cumulative design when possible)
        - Scan a lot of data when you only need a small chunk
    - Excessive backfills
        - Backfill 1 month of data, validate, only then you starts to backfill other month
        - Don't backfill all without validating them
    - Not sampling
        - Sometimes you dont need all data
    - Not subpartitioning your data correctly (predicate pushdown is your friend)
        - Similar to ENUM - subpartition where it ignores irrelevant records
        - Predicate pushdown - moving filter conditions ("predicates") as close as possible to the data source before processing begins.

#### Why Subpartitions Minimize IO?
- ![alt text](../assets/image4.png)
- Very good for low-cardinality data, it avoid scanning all data, only skip to needed WHERE clause filter
- This is where subpartitions come into play

#### Large Cloud Bills
- Large IO and compute costs are correlated by:
    - Scanning too much data (use cumulative tables please)
    - O(n^2) algorithms applied in UDFs (nested loops are usually bad)
- Large IO and storage costs are correlated by:
    - Not leveraging Parquet file format effectively
        - Data is not compressed enough
    - Duplicative data models
        - Different definition of the same thing

#### Multiple Sources of Truth
- Some of the hardest (most impactful) work for data engineers
- Steps
    - Document all the sources and the discrepancies
        - Get the know the term/data they used
    - Understand from stakeholders why they needed something different
        - Get them in a room to discuss their definition
    - Build a spec that outlines a new path forward that can be agreed upon
- Multiple sources of truth does not mean 1 of them is true, you could be a new one that is agreed from the discussion
- Fruit for promotions and performance review

#### Document All the Sources and The Discrepancies
- Talk with all relevant stakeholders
- If you can code search for similar names, that works great
- Lineage of the shared source data is a good place to start too
    - Find the most raw log data and the downstream of this is the definition that you want to find

#### Understand from Stakeholders Why They Needed Something Different
- There's usually a reason why there's multiple data sets
    - Put an end to the bleeding problem if not stopped
- Could be organizational, ownership, technical, skills, etc
- Work with stakeholders to find an ownership model that works

#### Build a Pipeline Spec
- Capture all the needs from stakeholders
- Get all the multiple sources of truth owners to sign off (this helps a lot with migrations later!!)

#### Models for getting ahead of tech debt
- Fix 'as you go'
- Allocate a portion of time each quarter (often called tech excellence week)
- Have the on-call person focus on tech debt during their shift

#### Tech Debt Models

- ![alt text](../assets/image5.png)

- Fix 'as you go'
    - Pros - Isn't much incremental burden
    - Cons - Tech debt rarely actually gets tacked because it's the last priority
- Allocate dedicated time each quarter
    - Pros - Fix things in big bursts. Fix a lot of problems
    - Cons - Tie up the team for a week. Tech debt builds up throughout the quarter
- Have the oncall person do tech debt
    - Pros - They fix bugs as they arise. They can see troubleshoot the most prioritize tech debt
    - Cons - Oncall person does nothing else that week

#### Data Migration Models

- The extremely cautious approach
    - Be careful not to break anything
    - Parallel pipelines for months. Paying 2x costs
    - Migration could takes 'forever'
- The bull in a china shop approach
    - Efficiency wins, minimize risk by migrating high-impact pipelines first
    - Kill the legacy pipeline as soon as you can

#### Proper oncall responsibilities
- Set proper expectations with your stakeholders
- DOCUMENT EVERY FAILURE AND BUG (huge pain short-term but great relief long-term)
- Document every failure and bug (huge pain short-term but great relief long-term)
    - Get notifications from failure? Document it
    - Your oncall will be smoother and smoother
- Oncall handoff
    - Should be a 20-30 minutes sync to pass context from one oncall to the next

#### Runbooks
- Complex pipelines need runbooks (which should be linked in the spec too)
    - Not every pipelines need runbooks though
- Most important pieces:
    - Primary and secondary owners
    - Upstream owners (teams not individuals)
        - Because team ownership fluctuates less than individual
    - Common issues (if any)
        - Be fill out over time
        - oncall will fill out whatever issues they will face
        - Not bug per say, but common issues that happened due to the nature of the data, like eg web scraping data 
    - Critical downstream owners (teams not individuals)
        - Ones that matters the most
    - SLAs and agreements
        - When data is expected to arrive

#### Upstream and Downstream Owner
- ![alt text](../assets/image6.png)
