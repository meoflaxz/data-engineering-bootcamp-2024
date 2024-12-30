# Growth Pipeline Specification

## XX User Growth Pipeline

#### This pipeline measure website traffic and user growth.

#### The goal of this pipeline is to answer following questions
- How many people are going to xx.com and xxx.com on daily basis?
    - What is the geographical and device break down of traffic?
    - Where are these people coming from? LinkedIn? Substack?
- How many people are signing up with an account on xx.com/signup each day?
    - What percentage of traffice is converting to signing up?
- How many people are purchasing boot camps and courses at xx.com/bootcamp?
    - What percentage of signups convert to paying customers?

## Business Metrics
|  **Metric Name** | **Definition**  | **is Guardrail** |
| :------ | :------  | :------: |
| **signup_conversion_rate** | COUNT(signups)/COUNT(website_hits) | YES |
| **purchase_conversion_rate** | COUNT(purchases)/COUNT(signups) | YES |
| **traffic_breakdown** | COUNT(website_hits) GROUP BY referrer | NO |

- Guardrail means if the metrics goes down, it will heavily impact the business

## Flow Diagram

## Schemas

- **core.fct_website_events** : this table is a list of all events for xx.com and includes IP enrichment and user agent enrinchment for country and device specific information

| **Column Name** | **Column Type** | **Column Comment** |
| :------ | :------  | :------ |
| *user_id* | BIGINT | This column is nullable for logged out events. This column indicates the user who generated this event. |
| logged_out_user_id | BIGINT | Hash of IP address and device information |
| dim_hostname | STRING | The host associated with event | 
| dim_country | STRING | The country associated with the IP address of this request |
| dim_device_brand | STRING | The device branch associated with this request |
| action_type | STRING | This is an enumerated list of actions that a user could take on this website (signup, watch video, go to landing page, etc) |
| event_timestamp | TIMESTAMP | The UTC timestamp for logged events |
| other_properties | MAP[STRING, STRING] | Any other valid properties that are part of this request |
| ds | STRING | This is the partition column for this table |
