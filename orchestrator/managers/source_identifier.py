import logging
import re
from pathlib import Path
from typing import Any, Optional

import yaml

_sources_cache = None
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
SOURCES_CONFIG_PATH = CONFIG_DIR / "sources.yaml"

logger = logging.getLogger(__name__)


class SourceConfig:
    """
    Stable configuation API for source configuration that abstracts YAML structure.
    Validates required fields on instantiation and provides property based access to all configuration elements.
    """

    def __init__(self, source_name: str, raw: dict):
        self._raw = raw
        self.name = source_name
        self._validate()

    def _get_nested(self, path: str, default: Any = None) -> Any:
        """
        Safely traverse nested yaml dict using dot seperated path.
        Returns a default value if any key in the path is missing
        """
        keys = path.split(".")
        value = self._raw
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default
        return value

    def _validate(self):
        """
        Validate required configuration fields.
        """
        if not self.validation_strategy:
            raise ValueError(
                f"Source '{self.name}': validation.strategy.type is required"
            )
        if not self.file_type:
            raise ValueError(f"Source '{self.name}': metadata.file_type is required")
        if not self.filename_pattern:
            raise ValueError(
                f"Source '{self.name}': metadata.filename_pattern is required"
            )
        # For footer completion strategy the config file must define a count_pattern
        if self.validation_strategy == "footer":
            c_pattern = self.count_pattern is not None
            if not c_pattern:
                raise ValueError(
                    f"Source '{self.name}' has footer validation strategy but has not defined validation.count_pattern"
                )
        # For footer completion strategy the config file must define an amount_pattern
        if self.validation_strategy == "footer":
            a_pattern = self.amount_pattern is not None
            if not a_pattern:
                raise ValueError(
                    f"Source '{self.name}' has footer validation strategy but has not defined validation.amount_pattern"
                )
        # Configuration file must have either an amount_column_name or an amount_column_position
        a_name = self.amount_column_name is not None
        a_pos = self.amount_column_position is not None
        if not a_name and not a_pos:
            raise ValueError(
                f"Source '{self.name}' must define a validation.amount_column.name or a validation.amount_column.position"
            )
        # For validation_strategy = 'file' the configuration file miust define either a pattern or a suffix
        if self.validation_strategy == "file":
            f_pattern = self.validation_file_pattern is not None
            f_suffix = self.validation_file_suffix is not None
            if not f_pattern and not f_suffix:
                raise ValueError(
                    f"Source '{self.name}: has a validation strategy of 'file' and must therefore define either a validation.strategy.pattern or a validation.strategy.suffix"
                )

        # For validation strategy = "file" the configuration file must define a key_pattern
        if self.validation_strategy == "file":
            if not self.validation_key_pattern:
                raise ValueError(
                    f"Source '{self.name}': has a validation strategy of 'file' and must therefore define a validation.strategy.key_pattern"
                )

    # --- Metadata properties ---
    @property
    def file_type(self) -> str:
        return self._get_nested("metadata.file_type")

    @property
    def encoding(self) -> str:
        return self._get_nested("metadata.encoding", "utf-8-sig")

    @property
    def filename_pattern(self) -> str:
        return self._get_nested("metadata.filename_pattern")

    @property
    def timeout_seconds(self) -> int:
        return self._get_nested("metadata.timeout_seconds", 3000)

    @property
    def stable_seconds(self) -> int:
        return self._get_nested("metadata.stable_seconds", 3)

    # --- Validation properties ---
    @property
    def count_pattern(self) -> Optional[str]:
        return self._get_nested("validation.count_pattern")

    @property
    def validation_strategy(self) -> str:
        return self._get_nested("validation.strategy.type")

    @property
    def validation_file_pattern(self) -> Optional[str]:
        return self._get_nested("validation.strategy.pattern")

    @property
    def validation_key_pattern(self) -> Optional[str]:
        return self._get_nested("validation.strategy.key_pattern")

    @property
    def validation_file_suffix(self) -> Optional[str]:
        return self._get_nested("validation.strategy.suffix")

    @property
    def amount_pattern(self) -> Optional[str]:
        return self._get_nested("validation.amount_pattern")

    @property
    def amount_column_name(self) -> Optional[str]:
        return self._get_nested("validation.amount_column.name")

    @property
    def amount_column_position(self) -> Optional[int]:
        pos = self._get_nested("validation.amount_column.position")
        return int(pos) if pos is not None else None

    # --- Padding properties ---
    @property
    def header_size(self) -> int:
        h_size = self._get_nested("padding.header_size", 0)
        return int(h_size) if h_size is not None else 0

    @property
    def footer_size(self) -> int:
        f_size = self._get_nested("padding.footer_size", 0)
        return int(f_size) if f_size is not None else 0

    # --- Helper Methods ---
    def get_amount_column_index(self, header_columns: list[str]) -> int:
        """
        Returns a 0 based index for the amount column.
        Prefers name over position.
        Raises a value error if neither are configured
        """
        # TODO: The value error here may be overkill given validation in class.....
        if self.amount_column_name:
            try:
                return header_columns.index(self.amount_column_name)
            except ValueError:
                raise ValueError(
                    f"Amount Column '{self.amount_column_name}' not found in file header"
                )
        if self.amount_column_position is not None:
            return (
                self.amount_column_position - 1
            )  # Convert 1 based in the yaml file to 0 based python counting
        raise ValueError("No amount column configured (name of position required)")

    def is_control_file(self, filename: str) -> bool:
        """
        Check if the filename matchs this source's control file spec
        """
        if self.validation_file_pattern:
            return bool(re.match(self.validation_file_pattern, filename))
        if self.validation_file_suffix:
            return filename.lower().endswith(self.validation_file_suffix.lower())
        return False

    def get_data_filename_from_control(self, control_filename: str) -> str:
        """
        Extract data filename from control filename
        """
        if self.validation_file_suffix:
            suffix = self.validation_file_suffix
            if control_filename.lower().endswith(suffix.lower()):
                return control_filename[: -len(suffix)]
        if self.validation_file_pattern:
            return control_filename
        return control_filename


class SourceRegistry:
    """
    Registry of SourceConfig instances with filename matching
    """

    def __init__(self, sources: dict[str, SourceConfig]):
        self._sources = sources
        self._compiled = {
            name: re.compile(source.filename_pattern)
            for name, source in sources.items()
        }

    @property
    def sources(self) -> dict[str, SourceConfig]:
        return self._sources

    def match(self, filename: str) -> Optional[SourceConfig]:
        """
        Match filename against all source patterns using re.fullmatch
        Returns:
            SourceConfig is exactly one pattern matches
            None if 0 match (with a warning logged)
            None if 2+ matches are found (with an error logged)
        """
        matches = [
            self._sources[name]
            for name, pattern in self._compiled.items()
            if pattern.fullmatch(filename)
        ]
        if len(matches) == 0:
            logger.warning(
                "No source pattern found for file: %s. Assuming Control File", filename
            )
            return None
        if len(matches) > 1:
            names = [s.name for s in matches]
            logger.error(
                "Ambiguous config - file %s matches multiple sources: %s",
                filename,
                names,
            )
            return None
        return matches[0]


def load_sources(config_path: Path = SOURCES_CONFIG_PATH) -> dict[str, SourceConfig]:
    """
    Load and cache all source configuration files from yaml config
    """
    global _sources_cache
    if _sources_cache is not None:
        return _sources_cache

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    _sources_cache = {
        name: SourceConfig(name, section) for name, section in raw.items()
    }
    return _sources_cache
