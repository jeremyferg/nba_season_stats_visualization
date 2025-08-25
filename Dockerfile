FROM apache/superset:latest

# Optional: pre-install Python packages you need
# RUN pip install psycopg2-binary mysqlclient

# Superset runs on port 8088 by default
EXPOSE 8088

# Start Superset
CMD ["superset", "run", "-p", "8088", "-h", "0.0.0.0"]
