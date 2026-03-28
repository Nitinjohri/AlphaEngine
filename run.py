"""
ML Engineering Internship - Task 0
Rolling mean signal pipeline for OHLCV data.
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


def setup_logging(log_file: str) -> logging.Logger:
    logger = logging.getLogger("mlops_pipeline")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
    # File handler
    fh = logging.FileHandler(log_file, mode="w")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


REQUIRED_CONFIG_KEYS = {"seed", "window", "version"}


def load_config(config_path: str, logger: logging.Logger) -> dict:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, "r") as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise ValueError("Config file is empty or not a valid YAML mapping.")

    missing = REQUIRED_CONFIG_KEYS - config.keys()
    if missing:
        raise ValueError(f"Config is missing required keys: {missing}")

    # Type checks
    if not isinstance(config["seed"], int):
        raise ValueError(f"Config 'seed' must be an integer, got: {type(config['seed'])}")
    if not isinstance(config["window"], int) or config["window"] < 1:
        raise ValueError(f"Config 'window' must be a positive integer, got: {config['window']}")
    if not isinstance(config["version"], str):
        raise ValueError(f"Config 'version' must be a string, got: {type(config['version'])}")

    logger.info(
        "Config loaded and validated — seed=%s, window=%s, version=%s",
        config["seed"], config["window"], config["version"]
    )
    return config

def load_dataset(input_path: str, logger: logging.Logger) -> pd.DataFrame:
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    try:
        import csv
        df = pd.read_csv(path, quoting=csv.QUOTE_NONE)
        df.columns = df.columns.str.strip('"')
        if "close" not in df.columns and '"close"' in df.columns:
            df.rename(columns={'"close"': "close"}, inplace=True)
        for col in df.select_dtypes(['object']).columns:
            df[col] = df[col].astype(str).str.strip('"')

    except Exception as exc:
        raise ValueError(f"Failed to parse CSV: {exc}") from exc

    if df.empty:
        raise ValueError("Input CSV is empty.")

    if "close" not in df.columns:
        raise ValueError(
            f"Required column 'close' not found. Columns present: {list(df.columns)}"
        )
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    n_invalid = df["close"].isna().sum()
    if n_invalid > 0:
        logger.warning("%d rows have non-numeric 'close' values and will be dropped.", n_invalid)
        df = df.dropna(subset=["close"]).reset_index(drop=True)

    logger.info("Dataset loaded — %d rows, columns: %s", len(df), list(df.columns))
    return df


def compute_rolling_mean(df: pd.DataFrame, window: int, logger: logging.Logger) -> pd.DataFrame:
    """
    Compute rolling mean on 'close'. The first (window-1) rows will have NaN
    rolling_mean values; they are excluded from signal computation.
    """
    df = df.copy()
    df["rolling_mean"] = df["close"].rolling(window=window, min_periods=window).mean()
    nan_count = df["rolling_mean"].isna().sum()
    logger.info(
        "Rolling mean computed (window=%d) — %d rows have NaN (warm-up period, excluded from signal).",
        window, nan_count
    )
    return df


def compute_signal(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    signal = 1 if close > rolling_mean, else 0.
    Rows where rolling_mean is NaN are excluded (signal stays NaN, not counted).
    """
    df = df.copy()
    valid = df["rolling_mean"].notna()
    df.loc[valid, "signal"] = (df.loc[valid, "close"] > df.loc[valid, "rolling_mean"]).astype(int)
    n_valid = valid.sum()
    n_signal = int(df.loc[valid, "signal"].sum())
    logger.info(
        "Signal generated — %d valid rows, %d with signal=1, %d with signal=0.",
        n_valid, n_signal, n_valid - n_signal
    )
    return df


def compute_metrics(df: pd.DataFrame, version: str, seed: int, latency_ms: float) -> dict:
    valid = df["signal"].notna()
    rows_processed = int(valid.sum())
    signal_rate = round(float(df.loc[valid, "signal"].mean()), 4)
    return {
        "version": version,
        "rows_processed": rows_processed,
        "metric": "signal_rate",
        "value": signal_rate,
        "latency_ms": round(latency_ms, 2),
        "seed": seed,
        "status": "success",
    }


def write_metrics(metrics: dict, output_path: str) -> None:
    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2)


def parse_args():
    parser = argparse.ArgumentParser(description="MLOps rolling-mean signal pipeline")
    parser.add_argument("--input",    required=True, help="Path to input CSV")
    parser.add_argument("--config",   required=True, help="Path to YAML config")
    parser.add_argument("--output",   required=True, help="Path for output metrics JSON")
    parser.add_argument("--log-file", required=True, help="Path for log file")
    return parser.parse_args()


def main():
    args = parse_args()
    logger = setup_logging(args.log_file)

    start_time = time.time()
    logger.info("=== Job started ===")

    version = "unknown"

    try:
        # 1. Load config
        config = load_config(args.config, logger)
        version = config["version"]
        seed    = config["seed"]
        window  = config["window"]

        # Set random seed for reproducibility
        np.random.seed(seed)
        logger.info("Random seed set: %d", seed)

        # 2. Load dataset
        df = load_dataset(args.input, logger)

        # 3. Rolling mean
        df = compute_rolling_mean(df, window, logger)

        # 4. Signal
        df = compute_signal(df, logger)

        # 5. Metrics
        latency_ms = (time.time() - start_time) * 1000
        metrics = compute_metrics(df, version, seed, latency_ms)

        write_metrics(metrics, args.output)
        logger.info(
            "Metrics written to %s — rows_processed=%d, signal_rate=%.4f, latency_ms=%.2f",
            args.output, metrics["rows_processed"], metrics["value"], metrics["latency_ms"]
        )

        logger.info("=== Job finished: SUCCESS ===")
        print(json.dumps(metrics, indent=2))
        sys.exit(0)

    except Exception as exc:
        latency_ms = (time.time() - start_time) * 1000
        logger.exception("Pipeline failed: %s", exc)

        error_metrics = {
            "version": version,
            "status": "error",
            "error_message": str(exc),
        }
        write_metrics(error_metrics, args.output)
        logger.info("Error metrics written to %s", args.output)
        logger.info("=== Job finished: FAILURE ===")

        print(json.dumps(error_metrics, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()