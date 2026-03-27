import re
import yaml
from pathlib import Path
from typing import Optional, Tuple

_sources_cache = None
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
SOURCES_CONFIG_PATH = CONFIG_DIR / "sources.yaml"


def load_sources_config(config_path: Path = Path(SOURCES_CONFIG_PATH)) -> dict:
    """
    Loads and caches sources.yaml configuration
    """
    global _sources_cache
    if _sources_cache is None:
        with open(config_path, 'r', encoding='utf-8') as f:
            _sources_cache = yaml.safe_load(f)
    return _sources_cache

def identify_source(filename: str, config_path: Path = Path(SOURCES_CONFIG_PATH)) -> Tuple[Optional[str], Optional[dict]]:
    """
    Identifies the source for a file based on it's filename convention
    """
    sources = load_sources_config(config_path)

    for source_name, source_config in sources.items():
        pattern = source_config.get('filename_pattern')

        if not pattern:
            # Skip sources without a configured file name pattern
            continue
        # Match the pattern against the filename
        if re.match(pattern, filename):
            return source_name, source_config  
    return None, None  # No matching source found

def is_control_file(filename: str, config_path: Path = Path(SOURCES_CONFIG_PATH)) -> bool:
    """
    Determine if the file is a control file based on it's extension
    Accepted control file extensions are .ctl, .done, .ready, .complete
    """
    sources = load_sources_config(config_path)
    for source_name, source_config in sources.items():
        control_suffix = source_config.get('control_file_suffix', '').lower()
        if control_suffix and filename.lower().endswith(control_suffix):
            return True
        control_pattern = source_config.get('control_file_pattern')
        if control_pattern and re.match(control_pattern, filename):
            return True
    return False

#def get_data_filename_from_control(control_filename: str, config_path: Path = Path(SOURCES_CONFIG_PATH)) -> str:
#    """
#    Extracts the data filename from a control file based on the source configuration
#    """
#    sources = load_sources_config(config_path)
#    for source_name, source_config in sources.items():
#        control_suffix = source_config.get('control_file_suffix')
#        if control_suffix and control_filename.lower().endswith(control_suffix):
#            return control_filename[:-len(control_suffix)]
#        control_pattern = source_config.get('control_file_pattern')
#        if control_pattern and re.match(control_pattern, control_filename):
#            return control