FROM apache/superset:latest

# Install MySQL driver and Pillow for screenshots (optional)
RUN pip install pymysql pillow

# Copy your Superset config
COPY superset_config.py /app/pythonpath/

EXPOSE 8088

# Proper startup: upgrade DB, init Superset, create admin, run server
CMD superset db upgrade && \
    superset init && \
    superset fab create-admin \
        --username admin \
        --firstname Superset \
        --lastname Admin \
        --email admin@example.com \
        --password admin \
        --role Admin && \
    superset run -h 0.0.0.0 -p 8088

