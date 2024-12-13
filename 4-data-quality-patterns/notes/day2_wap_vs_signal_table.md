# WRITE AUDIT PUBLISH vs SIGNAL TABLE PATTERN FOR DATA QUALITY

#### What Causes Bad Data?

- Logging errors
    - Upstream error from software engineer
    - Duplicate errors by mismatched logic from event
    - More of a fact data
- Snapshotting errors
    - What happen is you missing some dimension, too many users, etc
    - More for dimensional data
    - Rare event
- Production data quality issues
    - Actual bad data in production
    - Then you have to filter it out explicitly
    - Need to work with stakeholder/software engineers to figure out
- Schema evolution issues
    - Logging schema mismatch with staging schema, mismatch with production
- Pipeline mistakes making it into production
    - Error in WHERE, CASE WHEN, JOIN, etc
- Non-idempotent pipelines and backfill errors
    - A pipeline that produces different results
- Not thorough enough validation
    - Not validate when releasing dataset