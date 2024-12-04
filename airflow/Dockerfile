# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.7.1

# Set metadata about the authors
LABEL authors="joelo"

# Switch to root to install DuckDB and any additional dependencies
USER root

# Install DuckDB Python package
RUN pip install duckdb

# Switch back to airflow user to avoid running as root
USER airflow

# Use the default Airflow entrypoint
ENTRYPOINT ["/entrypoint"]

# Set default command to run Airflow's webserver (you can adjust if needed)
CMD ["webserver"]
