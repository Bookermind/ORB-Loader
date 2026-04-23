import logging
from datetime import datetime, date
from typing import Any, Optional
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)

class MappingConfig:
    """
    Config class to wrap a versioned mapping configuration loaded from yaml.
    Seperates metadata from field mappings.
    """
    def __init__(self, source_name: str, raw: dict, version_file: str):
        self._raw = raw
        self.name = source_name
        self.version_file = version_file
        self._validate()
    
    def _validate(self):
        if 'metadata' not in self._raw:
            raise ValueError(
                f"Mapping '{self.version_file}': 'metadata' node is required"
            )
        if 'mappings' not in self._raw:
            raise ValueError(
                f"Mapping '{self.version_file}': 'mappings' node is required"
            )
        if self.startdate is None:
            raise ValueError(
                f"Mapping '{self.version_file}': metadata.startdate is required"
            )
    
    @property
    def startdate(self) -> Optional[date]:
        value = self._raw.get('metadata', {}).get('startdate')
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            return datetime.strptime(value, '%Y-%m-%d').date()
        return None
    @property
    def enddate(self) -> Optional[date]:
        value = self._raw.get('metadata', {}).get('enddate')
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            return datetime.strptime(value, '%Y-%m-%d').date()
        return None
    
    @property
    def field_mappings(self) -> dict:
        """
        Returns the field mapping entries for use by the SchemaMapper.
        """
        return self._raw.get('mappings', {})
    
    @property
    def source_fields(self) -> set[str]:
        """
        Returns a set of all source fields referenced in the mapping
        """
        fields = set()
        for target_field, config in self.field_mappings.items():
            if isinstance(config,str):
                fields.add(config)
            elif isinstance(config, dict):
                source = config.get('source')
                if source:
                    fields.add(source)
        return fields
    
    def covers_date(self, file_date: date) -> bool:
        """
        Returns True of the file_date falls between this mapping file's date range.
        NOTE: startdate is inclusive, enddate is exclusive.
        NOTE: A none enddate means that this is the current version of the mapping config
        Args:
            file_date: The date to check against the mapping config's date range.
        """
        if self.startdate > file_date:
            return False
        if self.enddate is not None and file_date >= self.enddate:
            return False
        return True
    
def load_mapping(source_name: str, file_date: date, mapping_dir: Path) -> MappingConfig:
    """
    Can mapping_dir for all yaml files matching the source_name.
    Load each and return the MappingConfig whose date range covers the file_date.
    Args:
        source_name: The name of the source to find a mapping config for.
        file_date: The date of the file to find a mapping config for.
        mapping_dir: The directory to search for mapping config files.
    Returns:
        MappingConfig for the matching version
       
    Raises:
        ValueError: If no mapping version covers the file_date or if multiple files cover the file date.
        FileNotFoundError: If no mapping files found for the source
    """
    candidates = sorted(mapping_dir.glob(f"{source_name}*.yaml"))
    if not candidates:
        raise FileNotFoundError(f"No mapping files for source '{source_name}' in {mapping_dir}")
    
    matched = []
    for filepath in candidates:
        with open(filepath) as f:
            raw = yaml.safe_load(f)
        if source_name not in raw:
            logger.warning(
                "Mapping file %s does not contain key '%s' - skipping", filepath.name, source_name
            )
            continue
        config = MappingConfig(source_name, raw[source_name], filepath.name)

        if config.covers_date(file_date):
            matched.append(config)
        
    if len(matched) == 0:
        raise ValueError(
            f"No mapping version for source '{source_name}' which covers the date {file_date}"
        )
        
    if len(matched) > 1:
        files = [m.version_file for m in matched]
        raise ValueError(
            f"Ambiguous mapping: date {file_date} matches multiple versions "
            f"for source '{source_name}': {files}"
        )
        
    logger.info(
        "Resolved mapping for '%s' date %s -> %s",
        source_name, file_date, matched[0].version_file
    )
    return matched[0]