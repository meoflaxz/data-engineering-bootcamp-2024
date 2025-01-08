#### On call run book for XX Growth Pipeline

- Primary owner: Meoflaxz
- Secondary owner: Atan

#### Common Issues
- Upstream Datasets
    - Web site events
        - Common anomalies
            - Sometimes referrer is NULL too much, this is fixed downstream but we are alerted about because it messes with the metrics
    - User database exports
        - Export might fail to be extracted on a given day, when this happens, just use yesterday's export for today
- Downstream Datasets
    - Experimentation platform
    - Dashboards

#### SLAs 
- The data should land 4 hours after UTC midnight