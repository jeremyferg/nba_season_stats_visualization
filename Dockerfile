# Use official Superset image
FROM apache/superset:latest

# Install PyMySQL and Pillow inside the virtualenv
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        libjpeg-dev \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Switch back to superset user
USER superset

# Activate virtualenv and install Python packages
RUN . /app/.venv/bin/activate && \
    pip install --no-cache-dir pymysql pillow

# Copy your Superset config
COPY superset_config.py /app/pythonpath/

# Expose default port
EXPOSE 8088

# Start Superset
CMD ["/bin/bash", "-c", ". /app/.venv/bin/activate && superset db upgrade && superset init && superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@example.com --password admin && superset run -h 0.0.0.0 -p 8088"]

