# Airflow ETL Docker Image
FROM apache/airflow:2.7.2-python3.10

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:///airflow.db
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
ENV AIRFLOW__WEBSERVER__DEFAULT_USER_USERNAME=admin
ENV AIRFLOW__WEBSERVER__DEFAULT_USER_PASSWORD=admin
ENV _AIRFLOW_WWW_USER_USERNAME=admin
ENV _AIRFLOW_WWW_USER_PASSWORD=admin

# Install Python dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install additional Airflow providers
RUN pip install --no-cache-dir apache-airflow-providers-google

# Copy project files
COPY src/ /opt/airflow/src/
COPY dags/ /opt/airflow/dags/
COPY data/ /opt/airflow/data/

# Create necessary directories
RUN mkdir -p /opt/airflow/data/raw /opt/airflow/data/transformed

# Set ownership
RUN chown -R airflow:airflow /opt/airflow

# Switch to airflow user
USER airflow

# Initialize Airflow database
RUN airflow db init

# Create Airflow user
RUN airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Expose ports
EXPOSE 8080

# Default command
CMD ["airflow", "webserver"]
