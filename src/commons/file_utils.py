import yaml

def read_yaml(file_path):
    """
        Read YAML content from a file and return the parsed content as a dictionary.

        Parameters:
        - file_path (str): The path to the YAML file.

        Returns:
        - dict: A dictionary representing the parsed YAML content.

        Example:
        yaml_content = read_yaml("config.yaml")
    """
    try:
        with open(file_path, 'r') as f:
            return yaml.safe_load(f)

    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{file_path}' was not found.")

    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing YAML in '{file_path}': {str(e)}")
