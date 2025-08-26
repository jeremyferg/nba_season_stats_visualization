# Use official Superset image
FROM apache/superset:latest

# Switch to root to install system packages
USER root

# Install MySQL client libraries
RUN apt-get update && \
    apt-get install -y default-libmysqlclient-dev build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to superset user
USER superset

# Install Python packages inside Superset's venv
RUN . .venv/bin/activate && pip install --no-cache-dir pymysql pillow

# Copy your local Superset configuration (optional)
COPY superset_config.py /app/pythonpath/

# Expose default Superset port
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


