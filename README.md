# Python、PySpark 与 PyFlink 大数据项目

本项目是一个使用 Python、PySpark 和 PyFlink 开发大数据应用程序的模板。

## 项目结构

- `src/`: 包含所有 Python 源代码。
  - `jobs/`: 存放 PySpark 和 PyFlink 作业脚本。
    - `spark/`: PySpark 相关作业。
    - `flink/`: PyFlink 相关作业。
  - `utils/`: 通用工具函数。
- `tests/`: 包含单元测试和集成测试。
  - `jobs/spark/`: PySpark 作业的测试。
  - `jobs/flink/`: PyFlink 作业的测试。
- `conf/`: 存储配置文件（例如 `spark-defaults.conf`, `flink-conf.yaml`）。
- `scripts/`: 包含辅助脚本，用于运行作业或部署等任务。
- `notebooks/`: 用于数据探索和分析的 Jupyter Notebook。
- `requirements.txt`: 列出 Python 依赖项。
- `.gitignore`: 指定 Git 应忽略的特意未跟踪的文件。

## 安装步骤

1.  **克隆代码仓库:**
    ```bash
    git clone <your-repo-url>
    cd <your-repo-name>
    ```

2.  **创建 Python 虚拟环境:**
    强烈建议使用虚拟环境来管理项目依赖。
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # Windows 系统请使用 `venv\Scripts\activate`
    ```

3.  **安装依赖:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **环境变量 (可选但推荐):**
    为确保 PySpark 和 PyFlink 正常工作，您可能需要设置 `JAVA_HOME` 指向您的 Java 安装目录，如果不是仅作为库使用它们，还可能需要设置 Spark/Flink 的主目录。
    
    如果您计划直接使用 `spark-submit` 或 `flink` 命令行工具（而不是完整路径），请确保 Spark 和 Flink 的二进制文件路径已添加到您的 PATH 环境变量中。
    或者，如果不涉及集群管理器或使用嵌入式模式，作业也可以纯粹通过 Python 执行。

## 运行作业

您可以直接使用 Python 运行示例作业。对于更复杂的部署或集群执行，通常 PySpark 作业使用 `spark-submit`，PyFlink 作业使用 `flink run`。

### PySpark 示例 (`word_count.py`)

直接运行 PySpark 词频统计示例：
```bash
# 确保您的虚拟环境已激活
# 确保 src/ 目录在 PYTHONPATH 中以便访问
export PYTHONPATH=$PYTHONPATH:$(pwd)/src 
python src/jobs/spark/word_count.py
```
此脚本运行一个独立的 SparkSession。输出将打印到控制台。

集群提交 (示例):
```bash
# spark-submit --master <your-spark-master> src/jobs/spark/word_count.py
```

### PyFlink 示例 (`word_count.py`)

直接运行 PyFlink 词频统计示例：
```bash
# 确保您的虚拟环境已激活
# 确保 src/ 目录在 PYTHONPATH 中以便访问
export PYTHONPATH=$PYTHONPATH:$(pwd)/src
python src/jobs/flink/word_count.py
```
此脚本运行一个独立的 Flink 作业。输出将打印到控制台。

集群提交 (JAR 包示例, Python 文件也可以提交):
```bash
# flink run -py src/jobs/flink/word_count.py
# 或者对于已打包的应用程序:
# flink run -c com.example.MainClass your-application.jar
```
*注意: 提交 Python Flink 作业可能需要打包或使用 `-py` 或 `-pyfs` 选项指定 Python 文件，具体取决于您的 Flink 版本和设置。*


## 运行测试

本项目使用 `pytest` 进行测试。

1.  **确保 `pytest` 已安装** (它在 `requirements.txt` 文件中)。
2.  **运行测试:**
    在项目根目录下执行：
    ```bash
    # 确保 src 目录在 PYTHONPATH 中以便导入正常工作
    export PYTHONPATH=$(pwd)/src:$PYTHONPATH 
    pytest
    ```
    这将发现并运行 `tests/` 目录下的所有测试。

## 配置

-   Spark 默认配置可以放在 `conf/spark-defaults.conf`。复制模板并修改。
-   Flink 配置可以放在 `conf/flink-conf.yaml`。复制模板并修改。
-   应用程序特定的配置可以在类似 `conf/app.conf` 的文件中管理。

这些配置文件通常在向集群提交作业时使用，或者如果应用程序被构建为显式加载它们。提供的示例在本地模式下运行，可能不会广泛使用这些文件而无需修改。
