                                                                        ArcGIS REST API
                                                                (University Chapters FeatureServer)
                                                                                |
                                                                                |
                                                                                |
                                                                            ingestion.py
                                                                        (Data Ingestion to Bronze Layer)
                                                                        HTTP GET (Python Requests)
                                                                                -Call REST API
                                                                                -Filter CA, OR, WA
                                                                                -Generate Run ID
                                                                                -Save Raw JSON
                                                                                -Save Metadata
                                                                                |
                                                                                |
                                                                                |
                                                                        Bronze to Silver
                                                                                -Flatten JSON
                                                                                -Rename Columns
                                                                                -Validate Coordinates
                                                                                -Apply Warning Rules
                                                                                -Log Metrics 
                                                                                |                                                                                |
                                                                                |
                                                                                |
                                                        ---------------------------------------------------
                                                        |                                                  |
                                                Invalid Coordinates                                     Valid Records
                                                    (QUARANTINE)                                        (SILVER LAYER)
                                                    -INVALID_COORDINATES                                -Clean & Typed Data
                                                    -Raw Payload                                        -dq_status
                                                    -Run ID                                             -dq_warnings
                                                    -Reason Code
                                                                                                           |
                                                                                                        silver_to_gold.py
                                                                                                                -Read Silver
                                                                                                                -Remove Duplicates
                                                                                                                -Publish Gold 
                                                                                                                -Add Audit Columns
                                                                                                                -GOLD LAYER (Data Product)
                                                                                                                        -chapter_id 
                                                                                                                        -chapter_name
                                                                                                                        -city
                                                                                                                        -state
                                                                                                                        -longitude
                                                                                                                        -latitude
                                                                                                                        -dq_status
                                                                                                                        -dq_warnings
                                                                                                                        -published_timestamp

                                            