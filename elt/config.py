"""Env-driven configuration.

Phase 2: a frozen ``Settings`` dataclass loaded from ``.env`` via python-dotenv.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

from elt.errors import ELTError

_REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    # repr=False: the key must never land in logs, tracebacks, or print(settings).
    api_football_key: str = field(repr=False)
    api_football_host: str = ""
    api_rate_limit_rpm: int = 250
    gcp_project: str = ""
    bq_location: str = "US"
    bq_raw_dataset: str = "football_raw"
    google_application_credentials: str = ""
    leagues: list[int] = field(default_factory=list)
    seasons: list[int] = field(default_factory=list)
    log_level: str = "INFO"


def _split_ints(raw: str, var: str) -> list[int]:
    try:
        return [int(x.strip()) for x in raw.split(",") if x.strip()]
    except ValueError as exc:
        raise ELTError(f"{var} must be a comma-separated list of ints, got {raw!r}") from exc


def _resolve_credentials(raw_creds: str) -> str:
    """Resolve GOOGLE_APPLICATION_CREDENTIALS to an absolute path against the repo
    root -- a relative path resolves against the process CWD, which for dbt and
    the BQ client is not the repo root. Write it back so libraries that read the
    env var directly get the absolute form too."""
    if not raw_creds:
        return ""
    creds = Path(raw_creds)
    if not creds.is_absolute():
        creds = (_REPO_ROOT / creds).resolve()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(creds)
    return str(creds)


def _validate(settings: Settings) -> None:
    """Raise a single ELTError listing every configuration problem."""
    errors: list[str] = []

    required = {
        "API_FOOTBALL_KEY": settings.api_football_key,
        "API_FOOTBALL_HOST": settings.api_football_host,
        "GCP_PROJECT": settings.gcp_project,
        "GOOGLE_APPLICATION_CREDENTIALS": settings.google_application_credentials,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        errors.append(f"missing required config: {', '.join(missing)}")
    if settings.api_rate_limit_rpm <= 0:
        errors.append(f"API_RATE_LIMIT_RPM must be positive, got {settings.api_rate_limit_rpm}")
    if not settings.leagues:
        errors.append("LEAGUES is empty")
    if not settings.seasons:
        errors.append("SEASONS is empty")
    creds = settings.google_application_credentials
    if creds and not Path(creds).is_file():
        errors.append(f"credentials file not found: {creds}")

    if errors:
        raise ELTError("invalid configuration:\n  - " + "\n  - ".join(errors))


def load_settings() -> Settings:
    """Load, coerce, and validate pipeline configuration from the environment."""

    load_dotenv(_REPO_ROOT / ".env", override=False)

    api_football_key = os.getenv("API_FOOTBALL_KEY", "")
    api_football_host = os.getenv("API_FOOTBALL_HOST", "v3.football.api-sports.io")
    gcp_project = os.getenv("GCP_PROJECT", "")
    bq_location = os.getenv("BQ_LOCATION", "US")
    bq_raw_dataset = os.getenv("BQ_RAW_DATASET", "football_raw")
    raw_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    log_level = os.getenv("LOG_LEVEL", "INFO")

    try:
        api_rate_limit_rpm = int(os.getenv("API_RATE_LIMIT_RPM", "250"))
    except ValueError as exc:
        raise ELTError(
            f"API_RATE_LIMIT_RPM must be an int, got {os.getenv('API_RATE_LIMIT_RPM')!r}"
        ) from exc

    leagues = _split_ints(os.getenv("LEAGUES", ""), "LEAGUES")
    seasons = _split_ints(os.getenv("SEASONS", ""), "SEASONS")

    settings = Settings(
        api_football_key=api_football_key,
        api_football_host=api_football_host,
        api_rate_limit_rpm=api_rate_limit_rpm,
        gcp_project=gcp_project,
        bq_location=bq_location,
        bq_raw_dataset=bq_raw_dataset,
        google_application_credentials=_resolve_credentials(raw_creds),
        leagues=leagues,
        seasons=seasons,
        log_level=log_level,
    )
    _validate(settings)
    return settings
