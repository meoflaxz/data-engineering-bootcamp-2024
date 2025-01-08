#### DATA PIPELINE RUNBOOK

- Pipeline Name: Profit
- Primary owner: Meo
- Secondary owner: Atan
- Types of data processed: Revenues, expenses, aside from profit data
- Technology/Processing Steps:
    - Data ingestion: Using Airflow for orchestration.
    - Transformation: Performed with Spark and Python scripts.
    - Loading: Stored in a Snowflake data warehouse.
    - Monitoring: Alerts via PagerDuty for critical failures.
- Common issues:
    - New currency added - Solution: Add the currency in the unit tests and verify integration.
    - Duplicate transactions - Solution: Coordinate with the upstream source to validate the integrity of the source data.
- SLA: The data should land 2 hours after UTC midnight
    - Issue resolution: All critical issues should be resolved within 4 hours of detection
- Oncall schedule: Weekly rotation. Holidays as below:

| Date       | On-Call Engineer | Backup Engineer | Notes                  |
|------------|------------------|-----------------|------------------------|
| Dec 20-21  | Meo            | Atan             | Monitor for peak loads |
| Dec 22-23  | Atan              | Meo         | Adjust for new holidays|
| Dec 24-25  | Meo          | Atan           | Handle Christmas traffic |
| Dec 26-27  | Meo            | Atan             | Monitor post-holiday cleanup |
| Dec 28-29  | Atan              | Meo         | Final year-end checks |

- Suggestions
    - Escalation paths.
    - Specific dashboards or tools to consult during troubleshooting.