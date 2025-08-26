# Use official Superset image
FROM apache/superset:latest

# Install MySQL driver (for querying your Aiven MySQL database) and Pillow (optional, for screenshots)
USER root
RUN apt-get update && \
    apt-get install -y default-libmysqlclient-dev build-essential && \
    pip install --no-cache-dir pymysql pillow && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the superset user
USER superset

# Copy your local Superset configuration (optional)
COPY superset_config.py /app/pythonpath/

# Expose default Superset port
EXPOSE 8088

# Start Superset (metadata DB defaults to Postgres on Render)
CMD superset db upgrade && \
    superset init && \
    superset fab create-admin \
        --username admin \
        --firstname Superset \
        --lastname Admin \
        --email admin@example.com \
        --password admin && \
    superset run -h 0.0.0.0 -p 8088

