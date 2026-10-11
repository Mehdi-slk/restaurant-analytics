import os

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://admin:admin@postgres:5432/superset"
)

SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY",
    "change-this-to-a-random-secret-key",
)