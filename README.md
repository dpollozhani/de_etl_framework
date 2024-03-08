# Data Engineer ETL Framework

This PySpark project provides modular code for reading, writing, and transforming data in ETL pipelines.

## Local Environment Setup

### Prerequisites

- Python (3.6 or higher)
- PySpark (3.x recommended)

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your_username/de_etl_framework.git
   cd de_etl_framework
   ```
2. Install dependencies:
    ``` 
    pip install -r requirements.txt
    ```
   **Note:** Enable venv for this project before running the pip install command.
## Project Structure

- **src/**: Contains the source code for the project.
  - **io/**: Modules for input/output operations.
    - **reader/**: Modules for reading data.
    - **writer/**: Modules for writing data.
  - **transform/**: Modules for data transformations.
  - **commons/**: Common utility modules.
- **tests/**: Unit tests for the project modules.
- **resources/**: Contains required configuration and data files.
- **examples/**: Example scripts demonstrating the usage of the project modules.

## Testing
1. Navigate to project root directory
2. run pytest command in terminal
``` 
python -m pytest
```
**Note:** Enable venv for this project before running the pip install command.
3. Check the coverage report in **htmlcov/index.html** file

## Packaging

Run the below command to package the code

``` 
python setup.py sdist bdist_wheel
```
This command generates a **dist/** directory containing the distribution files.
- source distribution (tar.gz)
- wheel distribution (.whl)

