FROM apache/superset:latest

# Install MySQL driver and pillow
RUN pip install pymysql pillow

# Copy Superset config
COPY superset_config.py /app/pythonpath/

EXPOSE 8088

# Initialize Superset and create admin if not exists
CMD superset db upgrade && \
    superset init && \
    # Create admin user only if it doesn't exist
    superset fab create-admin \
        --username admin \
        --firstname Superset \
        --lastname Admin \
        --email admin@example.com \
        --password admin \
        --role Admin || true && \
    superset run -h 0.0.0.0 -p 8088
