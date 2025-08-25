import os

# Generate a secure secret key (example only — replace with your own)
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "KFe68ZNu5Bw4PiRoIIRP2a5Pt2M")

FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": True
}
