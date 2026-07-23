# Data Product Contract

## 1. Product Information

Attribute				Value

Product Name 			University Chapters
Version 				v1
Owner 					Data Engineering Team
Source 					ArcGIS University Chapters REST API
Refresh 				Frequency Daily (Manual for Assignment)
Storage Layer 			Gold
Format 					Parquet
Classification 			Public Data
PII 					None

---

# 2. Purpose

The University Chapters data product provides a trusted, consumer-ready dataset
containing active university chapters for California (CA), Oregon (OR), and
Washington (WA).

The dataset is intended for:

- Reporting
- Business Intelligence
- GIS Analytics
- Dashboard Development
- Downstream Applications

---

# 3. Data Source

ArcGIS REST API

Source:

https://services2.arcgis.com/5I7u4SJE1vUr79JC/arcgis/rest/services/UniversityChapters_Public/FeatureServer/0/query

Filter:

State IN ('CA','OR','WA')

---

# 4. Data Flow


  ArcGIS API --> Bronze --> Silver    --> Gold

                          --> Quarantine

---

# 5. Grain

One row represents one university chapter.

Business Key

chapter_id

Example

CA-0355

---

# 6. Consumer Interface

Gold Path

/Volumes/dev_engineering_catalog/dev_engineering_schema/dev_engineering_volume/gold/university_chapters/v1/

Format

Parquet

---

# 7. Schema

  Column              Type          Description  
  chapter_id          String			  Business Key  
  chapter_name			  String			  University Chapter Name  
  city					      String			  City  
  state					      String			  State Code  
  longitude				    Double			  Longitude  
  latitude				    Double			  Latitude  
  dq_status				    String			  Data Quality Status  
  dq_warnings			    Array<String>	Warning Messages  
  published_timestamp	Timestamp		  Gold Publish Timestamp 

---

# 8. Data Quality Rules

## DQ-Q1 (Hard Rule)

Condition

  -Invalid Coordinates

Checks

  -Longitude NULL

  -Latitude NULL

  -Longitude outside [-180,180]

  -Latitude outside [-90,90]

Action

  -Move record to Quarantine

Reason Code

  -INVALID_COORDINATES

Published?

  -No

---

## DQ-W1 (Soft Rule)

Condition

  City

    -NULL

    -Blank

    -UNKNOWN

Action

  Publish to Gold

Status

  -WARNING

  -Warning

  -MISSING_OR_UNKNOWN_CITY

Published?

  Yes

---

# 9. Batch Validation

Pipeline fails when

Entire API returns zero records.

Pipeline raises an alert when

California records unexpectedly become zero.

Pipeline does NOT fail when

Oregon contains zero rows.

Washington contains zero rows.

This is considered valid source data.

---

# 10. Freshness SLA

Frequency

Daily

Expected Completion

06:00 UTC

---

# 11. Versioning

Current Version

v1

Breaking schema changes

New versions will be published using

v2

v3

without modifying previous versions.

---

# 12. Security

Classification

Public

Contains PII

No

Encryption

Platform Managed

---

# 13. Consumers

Power BI

Databricks

Azure Synapse

Data Scientists

Reporting Teams

---

# 14. Support

Owner

Data Engineering Team

Issue Response

Business Hours
