graph LR
    subgraph "On-Premises/Local"
        A["`**Python Application**
        Kafka Producer
        Weather Data Generator`"]
    end
    
    subgraph "Azure Cloud Platform"
        subgraph "Ingestion Layer"
            B["`**Azure Event Hubs**
            Standard Tier
            Weather Data Stream`"]
        end
        
        subgraph "Storage Layer"
            C["`**Azure Data Lake Storage Gen2**
            Hot Tier
            Raw Weather Data
            Partitioned by Date/Location`"]
        end
        
        subgraph "Processing Layer"
            D["`**Azure Synapse Analytics**
            Serverless Spark Pools
            Data Cleaning & Aggregation`"]
        end
        
        subgraph "Data Warehouse Layer"
            E["`**Azure SQL Database**
            Basic/Standard Tier
            Weather Facts & Dimensions`"]
        end
        
        subgraph "Management & Monitoring"
            F["`**Azure Data Factory**
            Pipeline Orchestration
            Scheduling & Triggers`"]
            
            G["`**Azure Monitor**
            Logging & Alerting
            Performance Metrics`"]
        end
    end
    
    %% Data Flow
    A -->|"Streaming Weather Data<br/>JSON Messages"| B
    B -->|"Real-time Events<br/>Capture & Store"| C
    C -->|"Raw Data Files<br/>Batch Processing"| D
    D -->|"Cleaned & Aggregated<br/>Structured Data"| E
    
    %% Orchestration & Monitoring
    F -.->|"Orchestrates"| D
    F -.->|"Schedules"| B
    G -.->|"Monitors"| B
    G -.->|"Monitors"| D
    G -.->|"Monitors"| E
    
    %% Styling - Azure Colors
    classDef onprem fill:#f9f9f9,stroke:#333,stroke-width:2px,color:#000
    classDef ingestion fill:#0078d4,stroke:#005a9e,stroke-width:2px,color:#fff
    classDef storage fill:#00bcf2,stroke:#0099cc,stroke-width:2px,color:#fff
    classDef processing fill:#ffb900,stroke:#cc9500,stroke-width:2px,color:#000
    classDef warehouse fill:#e81123,stroke:#c50e1f,stroke-width:2px,color:#fff
    classDef management fill:#5c2d91,stroke:#4a1e7a,stroke-width:2px,color:#fff
    
    class A onprem
    class B ingestion
    class C storage
    class D processing
    class E warehouse
    class F,G management