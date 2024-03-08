import pytest
import os
from src.commons.file_utils import read_yaml
import yaml


@pytest.fixture(scope="module")
def test_yaml_path():
    # Create a sample YAML file for testing
    test_yaml_path = "test_config.yaml"
    with open(test_yaml_path, "w") as test_file:
        test_file.write("key1: value1\nkey2: value2\nkey3: value3\n")
    yield test_yaml_path
    # Delete the tests YAML file after testing
    if os.path.exists(test_yaml_path):
        os.remove(test_yaml_path)


def test_read_yaml_valid_file(test_yaml_path):
    yaml_content = read_yaml(test_yaml_path)
    assert isinstance(yaml_content, dict)
    assert len(yaml_content) == 3
    assert yaml_content["key1"] == "value1"
    assert yaml_content["key2"] == "value2"
    assert yaml_content["key3"] == "value3"


def test_read_yaml_invalid_file():
    with pytest.raises(FileNotFoundError) as exc_info:
        read_yaml("nonexistent.yaml")
    assert "The file 'nonexistent.yaml' was not found." in str(exc_info.value)
