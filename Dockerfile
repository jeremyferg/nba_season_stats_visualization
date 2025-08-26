FROM apache/superset:latest

USER root

# Install system dependencies for MySQL driver
RUN apt-get update && \
    apt-get install -y default-libmysqlclient-dev build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to superset user
USER superset

# Install Python packages inside Superset's virtualenv
RUN . .venv/bin/activate && pip install --no-cache-dir pymysql pillow

# Copy your Superset config
COPY superset_config.py /app/pythonpath/

EXPOSE 8088

# Start Superset
CMD superset db upgrade && \
    superset init && \
    superset fab create-admin \
        --username admin \
        --firstname Superset \
        --lastname Admin \
        --email admin@example.com \
        --password admin && \
    superset run -h 0.0.0.0 -p 8088
