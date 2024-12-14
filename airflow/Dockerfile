# Use the official Apache Airflow image as the base
FROM apache/airflow:2.7.1-python3.9

# Switch to root to install system dependencies and DuckDB CLI
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \
    software-properties-common \
    build-essential \
    curl \
    ca-certificates \
    unzip \
    python3-venv \
    python3-distutils && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*



# Install DuckDB CLI based on architecture
RUN arch=$(dpkg --print-architecture) && \
    if [ "$arch" = "amd64" ]; then \
        curl -L https://github.com/duckdb/duckdb/releases/download/v1.1.1/duckdb_cli-linux-amd64.zip -o duckdb.zip; \
    elif [ "$arch" = "arm64" ]; then \
        curl -L https://github.com/duckdb/duckdb/releases/download/v1.1.1/duckdb_cli-linux-aarch64.zip -o duckdb.zip; \
    else \
        echo "Unsupported architecture: $arch"; exit 1; \
    fi && \
    unzip duckdb.zip && \
    mv duckdb /usr/local/bin/ && \
    chmod +x /usr/local/bin/duckdb && \
    rm duckdb.zip

USER airflow

RUN python -m ensurepip && pip install --upgrade pip && \
    pip install --no-cache-dir --index-url=https://pypi.org/simple \
    duckdb 'pyarrow<10.1.0,>=10.0.1' azure-storage-common apache-airflow-providers-google[all]


# Install DuckDB Python package
 RUN pip install --no-cache-dir duckdb
# RUN pip install duckdb

RUN pip install --no-cache-dir redis

RUN pip install --no-cache-dir \
    pandas \
    numpy \
    matplotlib

# Use the default Airflow entrypoint
ENTRYPOINT ["/entrypoint"]

# Set the default command to run Airflow's webserver (adjust if needed)
CMD ["webserver"]
