from decouple import config
import pytz
import boto3
import os

PAUSE = config("PAUSE", cast=bool, default=False)

# Number of rows to process from each queue in each round of the round-robin processing
ROWS_PER_ROUND = config("ROWS_PER_ROUND", cast=int, default=1)

# S3 bucket name
S3_BUCKET_NAME = config("S3_BUCKET_NAME")

# Polling interval (in seconds) for checking the S3 bucket for new files, pinging uptime monitor, etc.
POLLING_INTERVAL = config("POLLING_INTERVAL", cast=int)

# Uptime monitor address
UPTIME_MONITOR = config("UPTIME_MONITOR")

# Database connection
DATABASE_CONNECTION_STRING = config("DATABASE_CONNECTION_STRING")

# RabbitMQ connection
RABBITMQ_HOST = config("RABBITMQ_HOST")
RABBITMQ_DEFAULT_VHOSTS = config("RABBITMQ_DEFAULT_VHOSTS", default="/").split(",")
RABBITMQ_USERNAME = config("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = config("RABBITMQ_PASSWORD")

# Logging to Loki
LOKI_USER = config("LOKI_USER")
LOKI_PASSWORD = config("LOKI_PASSWORD")
LOKI_HOST = config("LOKI_HOST")
SERVICE_NAME = config("SERVICE_NAME")

# Timezone used in this app
appTimezoneStr = config("TIMEZONE")
appTimezone = pytz.timezone(appTimezoneStr)

# S3 object
S3_ENDPOINT = config("S3_ENDPOINT")
S3_KEY = config("S3_KEY")
S3_SECRET = config("S3_SECRET")
s3 = boto3.resource(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET,
)

VALIDATION_WORKERS = config("VALIDATION_WORKERS", default="").split(",")
VALIDATOR_API_KEY = config("VALIDATOR_API_KEY", default="")
if not VALIDATION_WORKERS or VALIDATION_WORKERS == [""]:
    raise ValueError("No VALIDATION_WORKERS defined in environment variables.")
if not VALIDATOR_API_KEY:
    raise ValueError("No VALIDATOR_API_KEY defined in environment variables.")

# Task slot to identify the instance logs are coming from during parallel execution (default is '0' for single instance)
HOSTNAME = config("HOSTNAME", default="0")
