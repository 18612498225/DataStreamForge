# Big Data Project with Python, PySpark, and PyFlink

This project is a template for developing big data applications using Python with PySpark and PyFlink.

## Project Structure

- `src/`: Contains all Python source code.
  - `jobs/`: Houses PySpark and PyFlink job scripts.
    - `spark/`: PySpark specific jobs.
    - `flink/`: PyFlink specific jobs.
  - `utils/`: Common utility functions.
- `tests/`: Contains unit and integration tests.
  - `jobs/spark/`: Tests for PySpark jobs.
  - `jobs/flink/`: Tests for PyFlink jobs.
- `conf/`: Stores configuration files (e.g., `spark-defaults.conf`, `flink-conf.yaml`).
- `scripts/`: Includes helper scripts for tasks like running jobs or deployment.
- `notebooks/`: For Jupyter notebooks used for data exploration and analysis.
- `requirements.txt`: Lists Python dependencies.
- `.gitignore`: Specifies intentionally untracked files that Git should ignore.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd <your-repo-name>
    ```

2.  **Create a Python virtual environment:**
    It's highly recommended to use a virtual environment to manage project dependencies.
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Environment Variables (Optional but Recommended):**
    For PySpark and PyFlink to work correctly, you might need `JAVA_HOME` set to your Java installation directory and Spark/Flink home directories if not using them solely as libraries.
    
    Ensure Spark and Flink binaries are in your PATH if you plan to use `spark-submit` or `flink` CLI tools directly without full paths.
    Alternatively, jobs can be run purely via Python execution if cluster managers are not involved or if using embedded modes.

## Running Jobs

You can run the example jobs directly using Python. For more complex deployments or cluster execution, you would typically use `spark-submit` for PySpark jobs and `flink run` for PyFlink jobs.

### PySpark Example (`word_count.py`)

To run the PySpark word count example directly:
```bash
# Ensure your virtual environment is activated
# Make sure src/ is accessible by PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$(pwd)/src 
python src/jobs/spark/word_count.py
```
This script runs a self-contained SparkSession. Output will be printed to the console.

For cluster submission (example):
```bash
# spark-submit --master <your-spark-master> src/jobs/spark/word_count.py
```

### PyFlink Example (`word_count.py`)

To run the PyFlink word count example directly:
```bash
# Ensure your virtual environment is activated
# Make sure src/ is accessible by PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$(pwd)/src
python src/jobs/flink/word_count.py
```
This script runs a self-contained Flink job. Output will be printed to the console.

For cluster submission (example for a JAR, Python files can also be submitted):
```bash
# flink run -py src/jobs/flink/word_count.py
# or for a packaged application:
# flink run -c com.example.MainClass your-application.jar
```
*Note: Submitting Python Flink jobs might require packaging or specifying Python files with `-py` or `-pyfs` options depending on your Flink version and setup.*


## Running Tests

This project uses `pytest` for running tests.

1.  **Ensure `pytest` is installed** (it's in `requirements.txt`).
2.  **Run tests:**
    From the project root directory:
    ```bash
    # Ensure src is in PYTHONPATH for imports to work
    export PYTHONPATH=$(pwd)/src:$PYTHONPATH 
    pytest
    ```
    This will discover and run all tests in the `tests/` directory.

## Configuration

-   Spark default configurations can be placed in `conf/spark-defaults.conf`. Copy the template and modify.
-   Flink configurations can be placed in `conf/flink-conf.yaml`. Copy the template and modify.
-   Application-specific configurations can be managed in files like `conf/app.conf`.

These configuration files are typically used when submitting jobs to a cluster or if applications are built to load them explicitly. The provided examples run in local mode and may not extensively use these files without modification.
