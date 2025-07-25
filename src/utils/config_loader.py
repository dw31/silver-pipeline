"""
Configuration loader utility for Silver Pipeline
Handles loading and validation of YAML configuration files
"""

import yaml
import os
from typing import Dict, Any, List
from dataclasses import dataclass
from pathlib import Path


@dataclass
class SourceConfig:
    """Configuration for a data source"""
    name: str
    lobs: List[str]
    domains: List[str]


@dataclass
class PipelineConfig:
    """Main pipeline configuration"""
    bronze_schema: str
    silver_schema: str
    schema_rules_table: str
    transform_rules_table: str
    sources: List[SourceConfig]
    batch_size: int
    enable_optimization: bool
    checkpoint_location: str
    validation_enabled: bool = True
    null_tolerance_percent: int = 5
    max_processing_time_hours: int = 2
    spark_config: Dict[str, str] = None

    def __post_init__(self):
        if self.spark_config is None:
            self.spark_config = {}


class ConfigLoader:
    """Loads and validates pipeline configuration from YAML files"""
    
    def __init__(self, config_path: str = None):
        """
        Initialize config loader
        
        Args:
            config_path: Path to configuration YAML file
        """
        if config_path is None:
            # Default to config directory relative to this file
            base_dir = Path(__file__).parent.parent.parent
            config_path = base_dir / "config" / "silver_pipeline_config.yaml"
        
        self.config_path = Path(config_path)
        self._config_data = None
        self._pipeline_config = None
    
    def load_config(self) -> PipelineConfig:
        """
        Load configuration from YAML file
        
        Returns:
            PipelineConfig object with loaded configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If YAML parsing fails
            ValueError: If configuration validation fails
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as file:
            self._config_data = yaml.safe_load(file)
        
        self._validate_config()
        self._pipeline_config = self._parse_config()
        
        return self._pipeline_config
    
    def _validate_config(self):
        """Validate the loaded configuration structure"""
        required_sections = ['pipeline', 'tables', 'sources', 'processing']
        
        for section in required_sections:
            if section not in self._config_data:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Validate pipeline section
        pipeline = self._config_data['pipeline']
        required_pipeline_keys = ['bronze_schema', 'silver_schema']
        for key in required_pipeline_keys:
            if key not in pipeline:
                raise ValueError(f"Missing required pipeline config: {key}")
        
        # Validate tables section
        tables = self._config_data['tables']
        required_table_keys = ['schema_rules', 'transform_rules']
        for key in required_table_keys:
            if key not in tables:
                raise ValueError(f"Missing required table config: {key}")
        
        # Validate sources
        sources = self._config_data['sources']
        if not isinstance(sources, list) or len(sources) == 0:
            raise ValueError("Sources must be a non-empty list")
        
        for source in sources:
            required_source_keys = ['name', 'lobs', 'domains']
            for key in required_source_keys:
                if key not in source:
                    raise ValueError(f"Missing required source config: {key}")
            
            # Validate domains
            valid_domains = {'pharmacy', 'medical', 'mbr'}
            for domain in source['domains']:
                if domain not in valid_domains:
                    raise ValueError(f"Invalid domain '{domain}'. Must be one of: {valid_domains}")
    
    def _parse_config(self) -> PipelineConfig:
        """Parse validated configuration into PipelineConfig object"""
        pipeline = self._config_data['pipeline']
        tables = self._config_data['tables']
        processing = self._config_data['processing']
        
        # Parse sources
        sources = []
        for source_data in self._config_data['sources']:
            sources.append(SourceConfig(
                name=source_data['name'],
                lobs=source_data['lobs'],
                domains=source_data['domains']
            ))
        
        # Get optional configurations
        data_quality = self._config_data.get('data_quality', {})
        performance = self._config_data.get('performance', {})
        spark_config = performance.get('spark_config', {})
        
        return PipelineConfig(
            bronze_schema=pipeline['bronze_schema'],
            silver_schema=pipeline['silver_schema'],
            schema_rules_table=tables['schema_rules'],
            transform_rules_table=tables['transform_rules'],
            sources=sources,
            batch_size=processing['batch_size'],
            enable_optimization=processing['enable_optimization'],
            checkpoint_location=processing['checkpoint_location'],
            validation_enabled=data_quality.get('validation_enabled', True),
            null_tolerance_percent=data_quality.get('null_tolerance_percent', 5),
            max_processing_time_hours=performance.get('max_processing_time_hours', 2),
            spark_config=spark_config
        )
    
    def get_source_combinations(self) -> List[tuple]:
        """
        Get all valid source/lob/domain combinations
        
        Returns:
            List of tuples (source, lob, domain)
        """
        if self._pipeline_config is None:
            self.load_config()
        
        combinations = []
        for source in self._pipeline_config.sources:
            for lob in source.lobs:
                for domain in source.domains:
                    combinations.append((source.name, lob, domain))
        
        return combinations
    
    def get_bronze_table_name(self, source: str, lob: str, domain: str, year_month: str) -> str:
        """
        Generate Bronze layer table name
        
        Args:
            source: Source identifier
            lob: Line of business
            domain: Domain (pharmacy, medical, member)
            year_month: YYYYMM format
            
        Returns:
            Fully qualified Bronze table name
        """
        if self._pipeline_config is None:
            self.load_config()
        
        return f"{self._pipeline_config.bronze_schema}.{source}_{lob}_{domain}_{year_month}_bronze"
    
    def get_silver_table_name(self, source: str, lob: str, domain: str, year: str, stage: str) -> str:
        """
        Generate Silver layer table name
        
        Args:
            source: Source identifier
            lob: Line of business  
            domain: Domain (pharmacy, medical, member)
            year: YYYY format
            stage: silver_a, silver_b, or silver_c
            
        Returns:
            Fully qualified Silver table name
        """
        if self._pipeline_config is None:
            self.load_config()
        
        return f"{self._pipeline_config.silver_schema}.{source}_{lob}_{domain}_{year}_{stage}"


# Singleton instance for easy import
config_loader = ConfigLoader()