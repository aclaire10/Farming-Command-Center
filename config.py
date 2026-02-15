"""Centralize configuration and environment variables for the ingestion pipeline."""

from dotenv import load_dotenv
import os


def load_config() -> dict:
    """Load and validate configuration from environment.

    Loads variables from .env and ensures OPENAI_API_KEY is present.
    Raises if required configuration is missing.

    Returns:
        dict: Configuration with key 'openai_api_key'.

    Raises:
        ValueError: If OPENAI_API_KEY is missing or empty.
    """
    load_dotenv()
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise ValueError(
            "OPENAI_API_KEY is required. Set it in .env or your environment."
        )
    return {"openai_api_key": api_key}
