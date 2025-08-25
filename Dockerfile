FROM apache/superset:latest

RUN pip install pymysql

EXPOSE 8088

CMD superset db upgrade && \
    superset fab create-admin \
        --username admin \
        --firstname Superset \
        --lastname Admin \
        --email admin@example.com \
        --password admin \
        --role Admin || true && \
    superset init && \
    superset run -h 0.0.0.0 -p 8088
