# Silver Layer Data Pipeline - Azure Databricks Delta Live Tables

A comprehensive Silver layer data pipeline implementation using Azure Databricks Delta Live Tables (DLT) following the Medallion architecture pattern.

## Overview

This pipeline processes Bronze layer data through three sequential Silver stages:
- **Silver_A**: Schema selection based on configurable business rules
- **Silver_B**: Data transformations with configurable business logic  
- **Silver_C**: Deduplication and filtering (placeholder for future enhancements)

## Architecture

```
Bronze Layer → Silver_A → Silver_B → Silver_C
     ↓           ↓         ↓         ↓
   Delta      Schema    Transform  Dedup/
   Tables     Rules     Rules      Filter
```

## Project Structure

```
claude-silver-pipeline/
├── config/
│   └── silver_pipeline_config.yaml    # Pipeline configuration
├── sql/
│   └── business_rules_schema.sql       # Business rules table definitions
├── src/
│   ├── silver_pipeline_dlt.py         # Main DLT pipeline implementation
│   ├── data_quality/
│   │   └── validators.py               # Data quality validation
│   └── utils/
│       ├── config_loader.py            # Configuration management
│       └── error_handling.py           # Error handling and logging
└── README.md
```

## Features

### ✅ Configurable Business Rules
- Schema selection rules stored in `business_rules.schema_definitions`
- Transformation rules stored in `business_rules.transform_definitions`
- No code changes required for rule updates

### ✅ Data Quality Validation
- Schema compliance validation
- Data completeness checks
- Data type validation
- Duplicate detection
- Configurable quality thresholds

### ✅ Error Handling & Monitoring
- Comprehensive error logging with structured output
- Performance metrics collection
- Circuit breaker pattern for resilience
- Data quality metrics tracking

### ✅ Performance Optimization
- Spark optimization settings
- Configurable batch processing
- Parallel processing support
- Delta Lake optimizations

## Quick Start

### 1. Setup Business Rules Tables

Execute the SQL script to create business rules tables:

```sql
-- Run this in Databricks SQL or notebook
%run sql/business_rules_schema.sql
```

### 2. Configure Pipeline

Update `config/silver_pipeline_config.yaml` with your specific:
- Source databases and schemas
- Lines of business (LOBs)
- Domains (pharmacy, medical, member)
- Processing parameters

### 3. Deploy DLT Pipeline

Create a new Delta Live Tables pipeline in Databricks:

1. Go to **Workflows** → **Delta Live Tables**
2. Click **Create Pipeline**
3. Configure pipeline settings:
   - **Pipeline Name**: `Silver_Layer_Pipeline`
   - **Source Code**: Upload `src/silver_pipeline_dlt.py`
   - **Target Schema**: `silver`
   - **Storage Location**: Your Delta Lake path
   - **Pipeline Mode**: Triggered (recommended for batch processing)

### 4. Run Pipeline

Start the pipeline from Databricks UI or via API:

```bash
# Using Databricks CLI
databricks pipelines start --pipeline-id <PIPELINE_ID>
```

## Configuration

### Pipeline Configuration (`config/silver_pipeline_config.yaml`)

```yaml
pipeline:
  bronze_schema: "bronze"
  silver_schema: "silver"
  
tables:
  schema_rules: "business_rules.schema_definitions"
  transform_rules: "business_rules.transform_definitions"
  
sources:
  - name: "source1"
    lobs: ["lob1", "lob2"]
    domains: ["pharmacy", "medical", "member"]

processing:
  batch_size: 1000000
  enable_optimization: true
  checkpoint_location: "/mnt/checkpoints/silver_pipeline"
```

### Business Rules Tables

#### Schema Rules (`business_rules.schema_definitions`)
Controls which fields are selected in Silver_A stage:

```sql
INSERT INTO business_rules.schema_definitions VALUES
('source1', 'lob1', 'pharmacy', 'patient_id', 'STRING', true, current_timestamp(), current_timestamp()),
('source1', 'lob1', 'pharmacy', 'prescription_date', 'DATE', true, current_timestamp(), current_timestamp());
```

#### Transform Rules (`business_rules.transform_definitions`)
Controls data transformations in Silver_B stage:

```sql
INSERT INTO business_rules.transform_definitions VALUES
('source1', 'lob1', 'pharmacy', 'patient_id', 'UPPER(patient_id)', 'FUNCTION', 1, current_timestamp(), current_timestamp());
```

## Data Flow

### Bronze → Silver_A (Schema Selection)
1. Read Bronze table: `bronze.source_lob_domain_yyyymm`
2. Query schema rules for source/lob/domain combination
3. Select only configured fields
4. Output: `silver.source_lob_domain_yyyy_silver_a`

### Silver_A → Silver_B (Transformations)
1. Read Silver_A table
2. Query transformation rules for source/lob/domain combination  
3. Apply transformations in priority order
4. Output: `silver.source_lob_domain_yyyy_silver_b`

### Silver_B → Silver_C (Future Enhancement)
1. Read Silver_B table
2. Apply deduplication logic (future)
3. Apply filtering rules (future)
4. Output: `silver.source_lob_domain_yyyy_silver_c`

## Data Quality

### Validation Types
- **Schema Validation**: Ensures required fields exist with correct types
- **Completeness**: Validates null percentages within tolerance
- **Duplicates**: Detects duplicate records based on key columns
- **Data Types**: Validates field types match business rules

### Quality Monitoring
Quality metrics are stored in `data_quality_metrics` table:

```sql
SELECT * FROM silver.data_quality_metrics 
WHERE source = 'source1' AND domain = 'pharmacy'
ORDER BY created_timestamp DESC;
```

## Error Handling

### Error Types
- **Configuration Errors**: Invalid YAML or missing required settings
- **Schema Errors**: Missing tables or incorrect table structure
- **Data Quality Errors**: Failed validation checks
- **Processing Errors**: Spark execution failures

### Error Monitoring
Errors are logged with structured JSON format:

```json
{
  "timestamp": "2025-07-22T10:30:00Z",
  "event_type": "ERROR", 
  "source": "source1",
  "lob": "lob1",
  "domain": "pharmacy",
  "stage": "silver_a",
  "error_type": "SchemaValidationError",
  "message": "Required field 'patient_id' missing from Bronze table"
}
```

## Performance Tuning

### Spark Configuration
Optimized settings in `config/silver_pipeline_config.yaml`:

```yaml
performance:
  spark_config:
    "spark.sql.adaptive.enabled": "true"
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
    "spark.sql.adaptive.skewJoin.enabled": "true"
```

### Delta Lake Optimizations
- Auto-optimization enabled on business rules tables
- Z-ordering on frequently queried columns
- Vacuum operations for maintenance

## Monitoring & Alerting

### Performance Metrics
- Processing time per stage
- Records per second throughput
- Memory and CPU utilization
- Data quality scores

### Alerting
Configure alerts for:
- Pipeline execution failures
- Data quality threshold violations
- Processing time SLA breaches
- Error rate increases

## Security

### Access Control
- Role-based access to pipeline configurations
- Secure credential management
- Service principal authentication

### Data Privacy
- PII handling compliance
- Data masking capabilities where required
- Audit logging for all operations

## Development

### Local Development
1. Install dependencies:
   ```bash
   pip install pyspark delta-spark pyyaml
   ```

2. Run unit tests:
   ```bash
   python -m pytest tests/
   ```

### CI/CD Integration
The pipeline supports automated deployment via:
- Azure DevOps Pipelines
- GitHub Actions
- Databricks REST API

## Troubleshooting

### Common Issues

1. **"Table not found" errors**
   - Verify Bronze tables exist with correct naming pattern
   - Check schema permissions

2. **Configuration validation errors**
   - Validate YAML syntax
   - Ensure all required sections are present

3. **Performance issues**
   - Check cluster sizing
   - Review Spark configuration
   - Monitor data skew

4. **Data quality failures**
   - Review business rules definitions
   - Check source data quality
   - Adjust tolerance thresholds

### Debug Mode
Enable debug logging:

```python
import logging
logging.getLogger('silver_pipeline').setLevel(logging.DEBUG)
```

## Future Enhancements

### Silver_C Stage Development
- Advanced deduplication algorithms
- Configurable filtering rules
- Data quality scoring integration

### Advanced Features
- Real-time processing capabilities
- ML-based anomaly detection
- Automated business rule discovery

## Support

For issues and feature requests, please refer to:
- Pipeline logs in Databricks workspace
- Data quality metrics dashboard
- Error tracking in monitoring system

## License

This project is licensed under the MIT License - see the LICENSE file for details.