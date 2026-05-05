# Automatically load .env file so DATABASE_URL / PG_* variables are available
# to PGMQConfig even when tests are run without `uv run --env-file`.
from dotenv import load_dotenv

load_dotenv()
