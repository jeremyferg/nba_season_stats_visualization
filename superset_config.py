import os

# Generate a secure secret key (example only — replace with your own)
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "KFe68ZNu5Bw4PiRoIIRP2a5Pt2M")

FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": True
}

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_DATABASE_URI",
    "mysql+pymysql://avnadmin:AVNS_-a_5vz1AV9Ppt95Sozh@mysql-3203b068-nba-stats.k.aivencloud.com:22355/defaultdb?ssl_mode=REQUIRED"
)
