# Unit/Integration Testing with Spark


#### Where can you catch quality bugs?

- In development (best case)
    - Then you fix it before it hits production
- In production, but not showing up in tables (still good)
    - A bit stressful cause you have to fix data in production that will causing delay for the pipeline
- In production, in production tables (terrible and destroys trust)
    - This is when you getting yelled at
    - Unnoticed until users start complaining

#### How do you catch bugs in development?

- Unit tests and integration tests of your pipeline
    - Especially really good if you need to call 3rd party library
    - That way it safely allows checking with dependencies from other party/library
    - Linter could also be useful
        - Make everything more readable so you can easily catch bugs
        - Forces everyone to code the same standard

#### How do you catch bugs in production?

- Use write-audit-publish pattern
    - Write your data to test/staging table that has the same schema as production table, then you run quality check, if pass, you move them to production table

#### Why software engineering has higher quality standards than data engineering?

- Why?
    - Risks
        - Server going down bigger impact than pipeline delay
        - Frontend being non-responsive stops the business too
        - Immediate consequence of bad quality
    - Maturation
        - Software engineering is a more mature field
        - Test-driven development and behaviour-driven development are new in data engineering
    - Talent
        - Data engineers come from a much more diverse background than SWEs

#### How will data engineering become riskier?

- Data delays impact machine learning
    - Everyday that notification ML was behind resulted in a ~10% drop in effectiveness and click through rates
- Data quality bugs impact experimentation
- As trust rises in data, risk rises data too
- Obviously this is not as riskier as SWEs

#### Why do most organizations miss the mark on data engineering quality?

- For most company, they just want to build the pipeline as quickly as possible
- "This chart looks weird" is enough for an alarming number of analytics organizations

#### Tradeoff between business velocity and sustainability

- Business wants answer fast
- Engineers don't want to to die from a mountain of tech debt
- SO WHO WINS?
- Depends on the strength of you engineering leaders and how much you push back!
- Don't cut corners to go fast