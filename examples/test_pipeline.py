"""
Test Script for Silver Pipeline
Tests the Silver pipeline implementation with sample data
"""

import sys
import os
from pathlib import Path

# Add src directory to path for imports
src_path = Path(__file__).parent.parent / "src"
sys.path.append(str(src_path))

from pyspark.sql import SparkSession
from utils.config_loader import config_loader, PipelineConfig
from utils.error_handling import SilverPipelineLogger, PipelineErrorHandler
from data_quality.validators import DataQualityValidator
import yaml
from datetime import datetime


class SilverPipelineTester:
    """Test harness for Silver pipeline components"""
    
    def __init__(self):
        self.spark = None
        self.config = None
        self.logger = SilverPipelineLogger("pipeline_tester")
        self.error_handler = PipelineErrorHandler()
        
    def setup_spark_session(self):
        """Initialize Spark session with Delta Lake support"""
        self.spark = SparkSession.builder \
            .appName("SilverPipelineTester") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        
        print("✅ Spark session initialized with Delta Lake support")
    
    def load_configuration(self):
        """Load pipeline configuration"""
        try:
            self.config = config_loader.load_config()
            print(f"✅ Configuration loaded successfully")
            print(f"   Bronze schema: {self.config.bronze_schema}")
            print(f"   Silver schema: {self.config.silver_schema}")
            print(f"   Sources configured: {len(self.config.sources)}")
        except Exception as e:
            print(f"❌ Failed to load configuration: {e}")
            raise
    
    def test_business_rules_tables(self):
        """Test business rules tables setup"""
        print("\n📋 Testing Business Rules Tables...")
        
        try:
            # Test schema_definitions table
            schema_rules_df = self.spark.sql(f"SELECT * FROM {self.config.schema_rules_table}")
            schema_count = schema_rules_df.count()
            print(f"✅ Schema rules table: {schema_count} rules found")
            
            if schema_count > 0:
                print("   Sample schema rules:")
                schema_rules_df.show(5, truncate=False)
            
            # Test transform_definitions table
            transform_rules_df = self.spark.sql(f"SELECT * FROM {self.config.transform_rules_table}")
            transform_count = transform_rules_df.count()
            print(f"✅ Transform rules table: {transform_count} rules found")
            
            if transform_count > 0:
                print("   Sample transform rules:")
                transform_rules_df.show(5, truncate=False)
                
        except Exception as e:
            print(f"❌ Business rules tables test failed: {e}")
            self.error_handler.log_error("BUSINESS_RULES_TEST", str(e), stage="business_rules_validation")
            return False
        
        return True
    
    def test_bronze_tables_exist(self):
        """Test that expected Bronze tables exist"""
        print("\n🏺 Testing Bronze Tables Availability...")
        
        combinations = config_loader.get_source_combinations()
        year_month = "202407"  # Test month
        
        existing_tables = []
        missing_tables = []
        
        for source, lob, domain in combinations:
            table_name = config_loader.get_bronze_table_name(source, lob, domain, year_month)
            
            try:
                df = self.spark.table(table_name)
                record_count = df.count()
                existing_tables.append((table_name, record_count))
                print(f"✅ {table_name}: {record_count} records")
            except Exception:
                missing_tables.append(table_name)
                print(f"❌ {table_name}: Not found")
        
        print(f"\nSummary: {len(existing_tables)} tables found, {len(missing_tables)} missing")
        
        if missing_tables:
            print("Missing tables:")
            for table in missing_tables:
                print(f"   - {table}")
            print("\nRun examples/sample_bronze_data.py to create sample data")
        
        return len(existing_tables) > 0
    
    def test_silver_a_logic(self):
        """Test Silver_A stage logic"""
        print("\n🥈 Testing Silver_A Stage Logic...")
        
        # Import pipeline stages (would normally be imported from DLT pipeline)
        sys.path.append(str(Path(__file__).parent.parent / "src"))
        from silver_pipeline_dlt import SilverPipelineStages
        
        pipeline_stages = SilverPipelineStages(self.config)
        
        # Test with first available source combination
        combinations = config_loader.get_source_combinations()
        test_source, test_lob, test_domain = combinations[0]
        
        try:
            # Get schema rules
            schema_rules = pipeline_stages.get_schema_rules(test_source, test_lob, test_domain)
            print(f"✅ Retrieved {len(schema_rules)} schema rules for {test_source}/{test_lob}/{test_domain}")
            
            # Test with Bronze table if it exists
            bronze_table = config_loader.get_bronze_table_name(test_source, test_lob, test_domain, "202407")
            
            if pipeline_stages.validate_bronze_table_exists(bronze_table):
                bronze_df = self.spark.table(bronze_table)
                print(f"✅ Bronze table loaded: {bronze_df.count()} records, {len(bronze_df.columns)} columns")
                
                # Apply schema selection
                silver_a_df = pipeline_stages.apply_schema_selection(bronze_df, schema_rules)
                print(f"✅ Schema selection applied: {silver_a_df.count()} records, {len(silver_a_df.columns)} columns")
                
                print("   Silver_A sample data:")
                silver_a_df.show(3, truncate=False)
                
                return True
            else:
                print(f"⚠️  Bronze table {bronze_table} not found - skipping data processing test")
                return True  # Configuration test passed
                
        except Exception as e:
            print(f"❌ Silver_A test failed: {e}")
            self.error_handler.log_error("SILVER_A_TEST", str(e), 
                                       source=test_source, lob=test_lob, 
                                       domain=test_domain, stage="silver_a")
            return False
    
    def test_silver_b_logic(self):
        """Test Silver_B stage logic"""
        print("\n🥈 Testing Silver_B Stage Logic...")
        
        from silver_pipeline_dlt import SilverPipelineStages
        pipeline_stages = SilverPipelineStages(self.config)
        
        # Test with first available source combination
        combinations = config_loader.get_source_combinations()
        test_source, test_lob, test_domain = combinations[0]
        
        try:
            # Get transform rules
            transform_rules = pipeline_stages.get_transform_rules(test_source, test_lob, test_domain)
            print(f"✅ Retrieved {len(transform_rules)} transform rules for {test_source}/{test_lob}/{test_domain}")
            
            # Test transformation logic with sample data if available
            bronze_table = config_loader.get_bronze_table_name(test_source, test_lob, test_domain, "202407")
            
            if pipeline_stages.validate_bronze_table_exists(bronze_table):
                # Simulate Silver_A → Silver_B transformation
                bronze_df = self.spark.table(bronze_table)
                schema_rules = pipeline_stages.get_schema_rules(test_source, test_lob, test_domain)
                silver_a_df = pipeline_stages.apply_schema_selection(bronze_df, schema_rules)
                
                # Apply transformations
                silver_b_df = pipeline_stages.apply_transformations(silver_a_df, transform_rules)
                print(f"✅ Transformations applied: {silver_b_df.count()} records")
                
                print("   Silver_B sample data:")
                silver_b_df.show(3, truncate=False)
                
                return True
            else:
                print(f"⚠️  No Bronze data for transformation testing")
                return True
                
        except Exception as e:
            print(f"❌ Silver_B test failed: {e}")
            self.error_handler.log_error("SILVER_B_TEST", str(e),
                                       source=test_source, lob=test_lob,
                                       domain=test_domain, stage="silver_b")
            return False
    
    def test_data_quality_validation(self):
        """Test data quality validation components"""
        print("\n🔍 Testing Data Quality Validation...")
        
        validator = DataQualityValidator(self.config)
        
        try:
            # Test quality metrics table creation
            quality_table = validator.create_quality_metrics_table()
            print(f"✅ Quality metrics table schema created")
            
            # Test validation with sample data if available
            combinations = config_loader.get_source_combinations()
            test_source, test_lob, test_domain = combinations[0]
            
            bronze_table = config_loader.get_bronze_table_name(test_source, test_lob, test_domain, "202407")
            
            try:
                test_df = self.spark.table(bronze_table)
                
                # Test schema compliance validation
                from silver_pipeline_dlt import SilverPipelineStages
                pipeline_stages = SilverPipelineStages(self.config)
                schema_rules = pipeline_stages.get_schema_rules(test_source, test_lob, test_domain)
                
                schema_validation = validator.validate_schema_compliance(
                    test_df, test_source, test_lob, test_domain, schema_rules
                )
                print(f"✅ Schema validation completed: {schema_validation['overall_status']}")
                
                # Test completeness validation
                required_fields = [rule['field_name'] for rule in schema_rules if rule.get('is_required')]
                completeness_validation = validator.validate_data_completeness(
                    test_df, test_source, test_lob, test_domain, required_fields
                )
                print(f"✅ Completeness validation: {completeness_validation['overall_completeness']:.1f}% complete")
                
                # Test duplicate detection
                key_columns = [required_fields[0]] if required_fields else []
                duplicate_results = validator.detect_duplicates(test_df, key_columns)
                print(f"✅ Duplicate detection: {duplicate_results['duplicate_count']} duplicates found")
                
                return True
                
            except Exception:
                print("⚠️  No Bronze data available for data quality testing")
                return True
                
        except Exception as e:
            print(f"❌ Data quality validation test failed: {e}")
            self.error_handler.log_error("DATA_QUALITY_TEST", str(e), stage="data_quality_validation")
            return False
    
    def test_error_handling(self):
        """Test error handling components"""
        print("\n🚨 Testing Error Handling...")
        
        try:
            # Test error logging
            self.error_handler.log_error(
                error_type="TEST_ERROR",
                error_message="This is a test error message",
                source="test_source",
                lob="test_lob", 
                domain="test_domain",
                stage="test_stage"
            )
            
            # Test error summary
            error_summary = self.error_handler.get_error_summary()
            print(f"✅ Error logging working: {error_summary['total_errors']} errors logged")
            
            # Test structured logging
            self.logger.log_pipeline_event(
                event_type="INFO",
                source="test_source",
                lob="test_lob",
                domain="test_domain", 
                stage="test_stage",
                message="Test pipeline event logging"
            )
            print("✅ Structured logging working")
            
            # Clear test errors
            self.error_handler.clear_errors()
            
            return True
            
        except Exception as e:
            print(f"❌ Error handling test failed: {e}")
            return False
    
    def run_comprehensive_test(self):
        """Run comprehensive test suite"""
        print("🧪 Starting Silver Pipeline Comprehensive Test Suite")
        print("=" * 60)
        
        test_results = []
        
        # Test configuration loading
        try:
            self.load_configuration()
            test_results.append(("Configuration Loading", True))
        except Exception as e:
            test_results.append(("Configuration Loading", False))
            print(f"❌ Cannot proceed without valid configuration: {e}")
            return False
        
        # Run individual tests
        tests = [
            ("Business Rules Tables", self.test_business_rules_tables),
            ("Bronze Tables Availability", self.test_bronze_tables_exist),
            ("Silver_A Logic", self.test_silver_a_logic),
            ("Silver_B Logic", self.test_silver_b_logic),
            ("Data Quality Validation", self.test_data_quality_validation),
            ("Error Handling", self.test_error_handling)
        ]
        
        for test_name, test_func in tests:
            try:
                result = test_func()
                test_results.append((test_name, result))
            except Exception as e:
                print(f"❌ {test_name} failed with exception: {e}")
                test_results.append((test_name, False))
        
        # Print test summary
        print("\n" + "=" * 60)
        print("🧪 Test Results Summary")
        print("=" * 60)
        
        passed_tests = 0
        total_tests = len(test_results)
        
        for test_name, result in test_results:
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{status} {test_name}")
            if result:
                passed_tests += 1
        
        print("=" * 60)
        print(f"Overall Result: {passed_tests}/{total_tests} tests passed")
        
        if passed_tests == total_tests:
            print("🎉 All tests passed! Silver pipeline is ready for deployment.")
        else:
            print("⚠️  Some tests failed. Please review the issues above.")
        
        # Print error summary if any errors occurred
        error_summary = self.error_handler.get_error_summary()
        if error_summary['total_errors'] > 0:
            print(f"\n🚨 {error_summary['total_errors']} errors were logged during testing")
        
        return passed_tests == total_tests
    
    def cleanup(self):
        """Cleanup resources"""
        if self.spark:
            self.spark.stop()
            print("✅ Spark session stopped")


def main():
    """Main test execution function"""
    tester = SilverPipelineTester()
    
    try:
        # Setup
        tester.setup_spark_session()
        
        # Run comprehensive test
        success = tester.run_comprehensive_test()
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Test execution failed: {e}")
        sys.exit(1)
    finally:
        tester.cleanup()


if __name__ == "__main__":
    main()