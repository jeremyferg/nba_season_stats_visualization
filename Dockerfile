# Use official Superset image
FROM apache/superset:latest

# Install MySQL driver (for Aiven metadata) and Pillow (optional, for screenshots)
RUN pip install pymysql pillow

# Copy your local Superset configuration
COPY superset_config.py /app/pythonpath/

# Expose the default Superset port
EXPOSE 8088

# Start Superset: upgrade DB, init, create admin, run server
CMD superset db upgrade && \
    superset init && \
    superset fab create-admin \
        --username admin \
        --firstname Superset \
        --lastname Admin \
        --email admin@example.com \
        --password admin && \
    superset run -h 0.0.0.0 -p 8088
