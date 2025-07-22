"""
Sample Bronze Data Generator for Silver Pipeline Testing
Creates sample Bronze layer tables for testing the Silver pipeline
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType
from datetime import datetime, date
import random


def create_sample_bronze_data(spark: SparkSession, schema_name: str = "bronze"):
    """
    Create sample Bronze layer tables for testing
    
    Args:
        spark: SparkSession
        schema_name: Target schema name (default: bronze)
    """
    
    # Create schema if it doesn't exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    
    # Sample data for pharmacy domain
    pharmacy_schema = StructType([
        StructField("patient_id", StringType(), False),
        StructField("prescription_date", DateType(), False),
        StructField("drug_name", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("days_supply", IntegerType(), True),
        StructField("prescriber_id", StringType(), True),
        StructField("pharmacy_id", StringType(), False),
        StructField("cost", DoubleType(), True)
    ])
    
    pharmacy_data = [
        ("pat001", date(2024, 7, 1), "Metformin", 90, 30, "dr001", "pharm001", 25.50),
        ("pat002", date(2024, 7, 2), "Lisinopril", 30, 30, "dr001", "pharm001", 15.75),
        ("pat003", date(2024, 7, 3), "Atorvastatin", 30, 30, "dr002", "pharm002", 45.25),
        ("pat001", date(2024, 7, 15), "Insulin", 3, 30, "dr003", "pharm001", 125.00),
        ("pat004", date(2024, 7, 10), "Amlodipine", 30, 30, "dr001", "pharm003", 18.50),
        ("PAT005", date(2024, 7, 12), " Metoprolol ", 60, 30, "dr002", "pharm002", None),  # Test data quality issues
        (None, date(2024, 7, 8), "Omeprazole", 30, 30, "dr001", "pharm001", 22.75),  # Null patient_id
        ("pat006", date(2024, 7, 20), "Levothyroxine", 90, 90, "dr003", "pharm003", 12.25)
    ]
    
    pharmacy_df = spark.createDataFrame(pharmacy_data, pharmacy_schema)
    pharmacy_df.write.mode("overwrite").saveAsTable(f"{schema_name}.source1_lob1_pharmacy_202407")
    
    # Sample data for medical domain
    medical_schema = StructType([
        StructField("patient_id", StringType(), False),
        StructField("service_date", DateType(), False),
        StructField("procedure_code", StringType(), False),
        StructField("diagnosis_code", StringType(), True),
        StructField("provider_id", StringType(), False),
        StructField("facility_id", StringType(), True),
        StructField("charge_amount", DoubleType(), True),
        StructField("allowed_amount", DoubleType(), True)
    ])
    
    medical_data = [
        ("pat001", date(2024, 7, 5), "99213", "E11.9", "prov001", "fac001", 150.00, 120.00),
        ("pat002", date(2024, 7, 8), "99214", "I10", "prov002", "fac001", 200.00, 160.00),
        ("pat003", date(2024, 7, 12), "99396", "Z00.00", "prov001", "fac002", 250.00, 200.00),
        ("pat001", date(2024, 7, 18), "80053", "E11.9", "prov003", "fac003", 85.00, 68.00),
        ("pat004", date(2024, 7, 22), "99215", "M25.511", "prov002", "fac001", 275.00, 220.00),
        ("PAT005", date(2024, 7, 25), " 99213 ", "I10", "prov001", "fac002", 150.00, None),  # Data quality issues
        ("pat006", date(2024, 7, 28), "93000", "I49.9", "prov003", "fac003", 75.00, 60.00)
    ]
    
    medical_df = spark.createDataFrame(medical_data, medical_schema)
    medical_df.write.mode("overwrite").saveAsTable(f"{schema_name}.source1_lob1_medical_202407")
    
    # Sample data for member domain
    member_schema = StructType([
        StructField("member_id", StringType(), False),
        StructField("enrollment_date", DateType(), False),
        StructField("termination_date", DateType(), True),
        StructField("plan_code", StringType(), False),
        StructField("subscriber_id", StringType(), True),
        StructField("dependent_number", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("birth_date", DateType(), True)
    ])
    
    member_data = [
        ("mem001", date(2024, 1, 1), None, "HMO001", "sub001", 0, "M", date(1985, 3, 15)),
        ("mem002", date(2024, 1, 1), None, "PPO001", "sub002", 0, "F", date(1978, 8, 22)),
        ("mem003", date(2024, 2, 1), None, "HMO001", "sub001", 1, "F", date(2010, 5, 8)),
        ("mem004", date(2024, 3, 1), date(2024, 6, 30), "EPO001", "sub004", 0, "M", date(1992, 11, 3)),
        ("mem005", date(2024, 4, 1), None, "PPO001", "sub005", 0, "F", date(1965, 7, 18)),
        ("MEM006", date(2024, 5, 1), None, " HMO001 ", "sub001", 2, None, date(2015, 2, 28)),  # Data quality issues
        ("mem007", date(2024, 6, 1), None, "HMO002", "sub007", 0, "M", date(1988, 9, 12))
    ]
    
    member_df = spark.createDataFrame(member_data, member_schema)
    member_df.write.mode("overwrite").saveAsTable(f"{schema_name}.source1_lob1_member_202407")
    
    print(f"✅ Created sample Bronze tables in {schema_name} schema:")
    print(f"   - {schema_name}.source1_lob1_pharmacy_202407 ({pharmacy_df.count()} records)")
    print(f"   - {schema_name}.source1_lob1_medical_202407 ({medical_df.count()} records)")
    print(f"   - {schema_name}.source1_lob1_member_202407 ({member_df.count()} records)")


def create_additional_test_data(spark: SparkSession, schema_name: str = "bronze"):
    """
    Create additional test data with various data quality scenarios
    
    Args:
        spark: SparkSession
        schema_name: Target schema name
    """
    
    # Generate data for source2/lob3 combinations
    pharmacy_data_2 = [
        ("pat101", date(2024, 7, 1), "Aspirin", 30, 30, "dr101", "pharm101", 8.50),
        ("pat102", date(2024, 7, 2), "Ibuprofen", 60, 30, "dr102", "pharm101", 12.75),
        ("", date(2024, 7, 3), "Acetaminophen", 50, 30, "dr101", "pharm102", 6.25),  # Empty patient_id
        ("pat103", None, "Naproxen", 30, 30, "dr103", "pharm102", 15.50),  # Null date
    ]
    
    pharmacy_schema = StructType([
        StructField("patient_id", StringType(), False),
        StructField("prescription_date", DateType(), False),
        StructField("drug_name", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("days_supply", IntegerType(), True),
        StructField("prescriber_id", StringType(), True),
        StructField("pharmacy_id", StringType(), False),
        StructField("cost", DoubleType(), True)
    ])
    
    pharmacy_df_2 = spark.createDataFrame(pharmacy_data_2, pharmacy_schema)
    pharmacy_df_2.write.mode("overwrite").saveAsTable(f"{schema_name}.source2_lob3_pharmacy_202407")
    
    member_data_2 = [
        ("mem101", date(2024, 1, 1), None, "HMO101", "sub101", 0, "M", date(1980, 1, 15)),
        ("mem102", date(2024, 2, 1), None, "PPO101", "sub102", 0, "F", date(1975, 6, 20)),
        ("mem103", date(2024, 3, 1), None, "EPO101", "sub103", 0, "U", date(1990, 12, 8)),  # Unknown gender
        ("mem101", date(2024, 1, 1), None, "HMO101", "sub101", 0, "M", date(1980, 1, 15)),  # Duplicate
    ]
    
    member_schema = StructType([
        StructField("member_id", StringType(), False),
        StructField("enrollment_date", DateType(), False),
        StructField("termination_date", DateType(), True),
        StructField("plan_code", StringType(), False),
        StructField("subscriber_id", StringType(), True),
        StructField("dependent_number", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("birth_date", DateType(), True)
    ])
    
    member_df_2 = spark.createDataFrame(member_data_2, member_schema)
    member_df_2.write.mode("overwrite").saveAsTable(f"{schema_name}.source2_lob3_member_202407")
    
    print(f"✅ Created additional test tables:")
    print(f"   - {schema_name}.source2_lob3_pharmacy_202407 ({pharmacy_df_2.count()} records)")
    print(f"   - {schema_name}.source2_lob3_member_202407 ({member_df_2.count()} records)")


def validate_bronze_tables(spark: SparkSession, schema_name: str = "bronze"):
    """
    Validate that Bronze tables were created successfully
    
    Args:
        spark: SparkSession
        schema_name: Schema name to validate
    """
    
    expected_tables = [
        f"{schema_name}.source1_lob1_pharmacy_202407",
        f"{schema_name}.source1_lob1_medical_202407", 
        f"{schema_name}.source1_lob1_member_202407",
        f"{schema_name}.source2_lob3_pharmacy_202407",
        f"{schema_name}.source2_lob3_member_202407"
    ]
    
    print(f"\n📊 Bronze Table Validation Summary:")
    print("=" * 50)
    
    for table in expected_tables:
        try:
            df = spark.table(table)
            count = df.count()
            columns = len(df.columns)
            print(f"✅ {table}: {count} records, {columns} columns")
            
            # Show sample data
            print(f"   Sample data:")
            df.show(3, truncate=False)
            
        except Exception as e:
            print(f"❌ {table}: Failed to read - {e}")
    
    print("=" * 50)


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SilverPipelineBronzeDataGenerator") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        print("🚀 Generating sample Bronze layer data...")
        
        # Create main test data
        create_sample_bronze_data(spark)
        
        # Create additional test scenarios
        create_additional_test_data(spark)
        
        # Validate created tables
        validate_bronze_tables(spark)
        
        print("\n✅ Sample Bronze data generation completed successfully!")
        print("You can now run the Silver pipeline to test the implementation.")
        
    except Exception as e:
        print(f"❌ Error generating sample data: {e}")
        raise
    finally:
        spark.stop()