FROM apache/airflow:2.7.3-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
   build-essential \
   && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /opt/airflow
ENV PYTHONPATH="/opt/airflow"

# Install dependencies
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy necessary files into the image
COPY ./dags dags
COPY ./src src
COPY ./configs configs
COPY ./utils utils
COPY ./data data

# Give ownership of the data folder to airflow user
USER root
RUN chown -R airflow: /opt/airflow/data
USER airflow
