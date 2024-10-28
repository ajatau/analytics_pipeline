FROM apache/airflow:latest

# Install any additional dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy wait-for-it script into the container
COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh  # Ensure the script has execute permission

# Copy your DAGs into the Airflow container
COPY dags /opt/airflow/dags

# Set Airflow environment
ENV AIRFLOW_HOME=/opt/airflow
WORKDIR /opt/airflow