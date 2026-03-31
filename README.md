# Healthcare_provider
<img width="1546" height="839" alt="Provider_efficiency" src="https://github.com/user-attachments/assets/71002a73-9b55-4a40-8eea-47a52b484c6f" />


## Overview
Built an end-to-end healthcare analytics pipeline to transform raw operational data into decision-ready productivity insights for providers and clinics.

This project simulates a real-world healthcare operations use case where appointment, patient, provider, and location data must be ingested, standardized, modeled, and aggregated to uncover workflow inefficiencies. The solution uses a modern **Azure + Databricks medallion architecture** to create analytics-ready datasets that support provider performance tracking, clinic benchmarking, and downstream BI reporting.

The pipeline demonstrates practical experience in:

- designing cloud-based data pipelines,
- building medallion data models,
- handling CDC/SCD patterns,
- deriving business KPIs from event timestamps,
- and preparing curated data products for reporting and analytics consumption.

---

## Business Problem
The organization currently relies on third-party vendors to manage and provide healthcare operational data, which limits direct access, flexibility, and control over how that data is used for internal analytics.

Because the data sits outside the organization’s core analytics ecosystem, it becomes difficult to build timely, standardized, and trustworthy reporting around provider and clinic performance. This creates challenges such as:

- limited visibility into appointment workflow efficiency,
- difficulty benchmarking providers across locations,
- delayed or inflexible reporting from external systems,
- reduced ability to define organization-specific KPIs,
- and a dependency on vendor-managed data pipelines for critical operational insights.

To improve control and unlock deeper performance analytics, the organization wants to bring this healthcare data in-house and build its own analytics pipeline for measuring provider productivity, clinic efficiency, and workflow bottlenecks.

---

## Solution
To solve this, I built a scalable healthcare productivity analytics pipeline that:

- ingests raw source data using **Azure Data Factory**,
- lands raw records into cloud storage,
- processes data through **Bronze, Silver, and Gold layers in Databricks**,
- applies cleansing, standardization, and SCD logic,
- derives time-based operational KPIs,
- and produces aggregated efficiency views and a denormalized analytical table for BI and dashboarding.

The final output enables stakeholders to monitor provider productivity, compare clinic performance, and identify operational improvement opportunities using trusted, analytics-ready data.

---
## Technical Design
### 1. Ingestion Layer

The ingestion pipeline is built in Azure Data Factory using a metadata-driven pattern:

- Lookup reads the list of input entities/files
- ForEach iterates through each source
- Copy Activity loads data into cloud storage as JSON
This design improves scalability and reusability by avoiding one-off pipelines for each data source.

### Value delivered
- Reduces pipeline duplication
- Simplifies onboarding of new entities
- Standardizes raw landing patterns
- Supports modular pipeline design

<img width="2378" height="1084" alt="image" src="https://github.com/user-attachments/assets/27e75ece-db6c-4ca8-9a92-7c9dc21d7b6a" />

### 2. Bronze Layer

The Bronze layer ingests raw healthcare datasets into Databricks tables, preserving source fidelity.

### Purpose
- Maintain a raw, replayable source of truth
- Separate ingestion from business logic
- Enable traceability and lineage across layers

### 3. Silver Layer

The Silver layer performs the core transformation and standardization work required to make the data analytically usable.
#### Key transformations
- Standardized date and timestamp fields
- Converted appointment lifecycle fields into full timestamps
- Cleaned and normalized dimensions
- Deduplicated records by business keys
- Standardized categorical values such as patient sex
- Applied CDC / SCD logic for entity management
- Productivity KPIs derived

From appointment workflow timestamps, the pipeline calculates operational metrics such as:
Waiting Process Time - Time between scheduled appointment and patient check-in
Rooming Process Time - Time between check-in and rooming
Check-out Process Time - Time between rooming and check-out
Chart Close Process Time - Time between check-out and chart closure
Total Processing Time - End-to-end cycle time across the visit workflow

### 4. Gold Layer

The Gold layer creates business-ready, aggregated views focused on efficiency and productivity measurement.

**Gold outputs**
Provider Efficiency View
Aggregates productivity metrics by provider to support benchmarking across clinicians.

Clinic Efficiency View
Aggregates the same metrics by clinic/location to identify operational gaps across sites.

**Example use cases**
- Compare providers by average wait time
- Identify clinics with slower chart closure
- Benchmark process efficiency across locations
- Surface workflow bottlenecks affecting throughput

### 5. Analytical Consumption Layer

To make downstream analytics easier, the project includes a **One Big Table (OBT)** that joins appointment, provider, patient, and location data into a denormalized analytical dataset.

**Why this matters**
This design reduces repeated joins for analysts and BI tools, making reporting faster and easier to maintain.

**Best suited for**
- Power BI dashboards
- Executive KPI reporting
- Self-service SQL analysis
- Ad hoc workflow investigations
  
<img width="1794" height="1166" alt="image" src="https://github.com/user-attachments/assets/eca0a95a-6674-43dc-8c86-a4296c4d7bf8" />

### Skills Demonstrated

This project highlights hands-on experience with:
- Azure Data Factory for orchestration and ingestion
- Azure Data Lake / Blob Storage for raw data landing
- Databricks for medallion architecture implementation
- PySpark for transformation and KPI derivation
- SQL for analytical modeling and OBT creation
- CDC / SCD handling for robust dimensional processing
- Data modeling for both aggregated views and denormalized BI tables
- Operational analytics for translating raw events into measurable business outcomes

### Impact

This solution provides a strong foundation for healthcare operations analytics by turning raw appointment flow data into structured, decision-ready insights.

In a real-world environment, a pipeline like this could help teams:
- reduce patient wait times,
- improve clinic throughput,
- identify provider workflow bottlenecks,
- improve chart closure discipline,
- and support leadership with standardized productivity reporting.
