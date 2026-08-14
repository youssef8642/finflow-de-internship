"""Configuration shared by every pipeline stage."""

from dataclasses import dataclass


@dataclass
class PipelineConfig:
    """Paths, credentials, and tuning knobs for the FinFlow pipeline.

    Paths are relative to the project root; each module resolves them against
    its own PROJECT_ROOT so the scripts work from any directory.
    """

    raw_dir: str = "data/raw"
    processed_dir: str = "data/processed"
    db_path: str = "data/finflow.duckdb"
    fred_api_key: str = "cfaf9d1c396769e0e721088fee194288"
    max_workers: int = 4
    chunk_size: int = 500_000
