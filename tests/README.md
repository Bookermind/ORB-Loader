# ORB-Loader Unit Tests

This directory contains unit tests for the ORB-Loader project using pytest.

## Structure
tests/
├── conftest.py # Shared fixtures
├── fixtures/ # Test data
│ ├── sources.yaml # Valid test configuration
│ └── invalid_sources.yaml # Invalid configs for error testing
├── test_source_identifier.py # SourceConfig & SourceRegistry tests
├── test_companion_tracker.py # File pairing logic tests
├── test_utilities.py # Utility function tests
└── test_logging_config.py # Logging setup tests


## Running Tests

### Run all tests
```bash
pytest tests/
```

### Run with coverage report
```bash
pytest tests/ --cov=orchestrator --cov-report=term --cov-report=html
```
### Run specific test file
```bash
pytest tests/test_source_identifier.py -v
```
### Run a specific test function 
```bash
pytest tests/test_source_identifier.py::test_source_config_valid_initialization -v
```
### Run tests by marker
```bash
pytest -m unit          # Only unit tests
pytest -m integration   # Only integration tests
pytest -m "not slow"    # Skip slow tests
```

## Writing tests   
### Test naming conventions   
- Test Files: ```test_<module_name>.py```
- Test Functions: ```test_<what_is_being_tested>```
- Test Classes: ```test_<ClassName>```

### Using fixtures   
Fixtures are defined in ```test/conftest.py``` and are automatically available to all tests:
```python
def test_something(valid_source_dict, tmp_path):
    """Test description in docstring."""
    # valid_source_dict is automatically injected
    # tmp_path is a pytest built-in for temporary directories
    pass
```

### Common Fixtures
- ```valid_source_dict``` - An example valid source configuration dictionary object
- ```footer_validation_source_dict``` - A valid source configuration dictionay for a file with footer completion
- ```minimal_source_dict``` - A minimally valid source configuration dictionary
- ```temp_dir_structure``` - A complete directory structure in temp space
- ```sample_yaml_file``` - A tempory valid yaml file in temp space
- ```mock_logger``` - Mocked logging logger for use testing log calls
- ```multiple_source_dict``` - A source configuration containing multiple sources for registry testing

### Mocking external dependencies
Use ```pytest-mock``` for mocking:
```python
def test_with_mock(mocker):
    # Mock a function
    mock_open = mocker.patch('builtins.open', mocker.mock_open(read_data='test'))
    
    # Mock a class method
    mock_method = mocker.patch('module.Class.method')
    mock_method.return_value = 'mocked value'
```

### Testing file operations
Use ```tmp_path``` fixture for isolated file operations:   
```python
def test_file_operation(tmp_path):
    test_file = tmp_path / "test.csv"
    test_file.write_text("data")
    assert test_file.read_text() == "data"
    # Automatic cleanup after test
```

### Testing time-dependent code   
Use ```freezegun``` to control time:   
```python
from freezegun import freeze_time
from datetime import timedelta

@freeze_time("2026-04-10 12:00:00")
def test_timeout(freezer):
    # Time is frozen at 2026-04-10 12:00:00
    freezer.tick(delta=timedelta(seconds=60))
    # Time is now 2026-04-10 12:01:00
```

### Testing exceptions   
Use ```pytest.raises```:   
```python
import pytest

def test_validation_error():
    with pytest.raises(ValueError, match="required field"):
        # Code that should raise ValueError
        raise ValueError("required field is missing")
```

## Coverage Goals   
- Critical Components (config, file-pairing for examples): **90%+**   
- Utilities and Helpers: **80%**
- Overall project: **80%**   

## Test Markers   
Mark tests with decorators:   
```python
import pytest

@pytest.mark.unit
def test_something():
    pass

@pytest.mark.slow
def test_long_running():
    pass

@pytest.mark.requires_db
def test_database_operation():
    pass
```

## Type checking
Run ```mypy``` on tests:  
```bash
mypy orchestrator/ tests/
```

## Troubleshooting   
### Tests passing locally but failing in CI  
- Check for hardcoded paths   
- Ensure tests don't depend on execution order   
- Verify that there is no shared state between tests   
### Import Errors   
- Ensure orchestrator/ is in PYTHONPATH
- Run tests from the project root: ```pytest tests/```   
### Fixture not found   
- Check fixture is defined in ```conftest.py```
- Verify fixture name matches exactly   
### Cache polution   
- Global caches are reset automatically via ```reset_sources_cache``` fixture
- If issue persist, check for any other global state.