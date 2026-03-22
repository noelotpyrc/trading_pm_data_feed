from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class VolSignalBuildConfig:
    feature_cols: tuple[str, ...] = (
        "parkinson_10",
        "parkinson_15",
        "parkinson_30",
        "parkinson_ratio_15_1440",
        "parkinson_ratio_30_1440",
    )
    vol_windows: tuple[int, ...] = (10, 15, 30, 45, 60, 1440)
    mar_horizons: tuple[int, ...] = (1, 3, 5)
    mar_blend_weights: tuple[float, float, float] = (0.2, 0.3, 0.5)
    forward_return_horizons: tuple[int, ...] = tuple(range(1, 16))
    training_lookback_days: int = 7
    z_pool_lookback_days: int = 7
    group_minutes: int = 15
    min_train_rows: int = 100
    min_z_pool_size: int = 100

    @property
    def max_forward_horizon(self) -> int:
        return max(self.forward_return_horizons)

    @property
    def min_required_history_days(self) -> int:
        # Need:
        # - 1 day for parkinson_1440 warmup
        # - 7 days to train the earliest day used in the z-pool lookback
        # - 7 days of rows whose scored z-values seed the score-day z-pool
        return 1 + self.training_lookback_days + self.z_pool_lookback_days


@dataclass(frozen=True)
class HorizonModel:
    horizon: int
    intercept: float
    coefficients: tuple[float, ...]
    train_start: str
    train_end_exclusive: str
    train_rows: int

    def to_dict(self, feature_cols: tuple[str, ...]) -> dict[str, Any]:
        return {
            "horizon": self.horizon,
            "intercept": self.intercept,
            "coefficients": dict(zip(feature_cols, self.coefficients, strict=True)),
            "train_start": self.train_start,
            "train_end_exclusive": self.train_end_exclusive,
            "train_rows": self.train_rows,
        }


@dataclass(frozen=True)
class DailySignalArtifact:
    score_date: str
    config: VolSignalBuildConfig
    models: dict[int, HorizonModel]
    z_pool: np.ndarray
    metadata: dict[str, Any] = field(default_factory=dict)

    def save(self, artifact_dir: Path) -> Path:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        np.save(artifact_dir / "z_pool.npy", self.z_pool)

        model_payload = {
            "score_date": self.score_date,
            "feature_cols": list(self.config.feature_cols),
            "mar_horizons": list(self.config.mar_horizons),
            "mar_blend_weights": list(self.config.mar_blend_weights),
            "group_minutes": self.config.group_minutes,
            "models": {
                str(h): model.to_dict(self.config.feature_cols)
                for h, model in sorted(self.models.items())
            },
        }
        (artifact_dir / "model.json").write_text(
            json.dumps(model_payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        metadata_payload = {
            "score_date": self.score_date,
            "z_pool_size": int(self.z_pool.size),
            **self.metadata,
        }
        (artifact_dir / "metadata.json").write_text(
            json.dumps(metadata_payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return artifact_dir

    @classmethod
    def load(cls, artifact_dir: Path) -> "DailySignalArtifact":
        model_payload = json.loads((artifact_dir / "model.json").read_text(encoding="utf-8"))
        metadata = json.loads((artifact_dir / "metadata.json").read_text(encoding="utf-8"))
        z_pool = np.load(artifact_dir / "z_pool.npy")

        config = VolSignalBuildConfig(
            feature_cols=tuple(model_payload["feature_cols"]),
            mar_horizons=tuple(model_payload["mar_horizons"]),
            mar_blend_weights=tuple(model_payload["mar_blend_weights"]),
            group_minutes=int(model_payload["group_minutes"]),
        )

        models: dict[int, HorizonModel] = {}
        for key, raw_model in model_payload["models"].items():
            horizon = int(key)
            coeff_map = raw_model["coefficients"]
            models[horizon] = HorizonModel(
                horizon=horizon,
                intercept=float(raw_model["intercept"]),
                coefficients=tuple(float(coeff_map[col]) for col in config.feature_cols),
                train_start=str(raw_model["train_start"]),
                train_end_exclusive=str(raw_model["train_end_exclusive"]),
                train_rows=int(raw_model["train_rows"]),
            )

        return cls(
            score_date=str(model_payload["score_date"]),
            config=config,
            models=models,
            z_pool=z_pool,
            metadata=metadata,
        )


def normalize_score_date(score_date: str | pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(score_date).normalize()


def validate_ohlcv_frame(df: pd.DataFrame) -> pd.DataFrame:
    required = {"datetime_utc", "open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"missing required OHLCV columns: {sorted(missing)}")

    out = df.copy()
    out["datetime_utc"] = pd.to_datetime(out["datetime_utc"])
    out = out.sort_values("datetime_utc").reset_index(drop=True)

    if out["datetime_utc"].duplicated().any():
        raise ValueError("OHLCV input contains duplicate datetime_utc rows")

    return out


def parkinson_volatility(high: pd.Series, low: pd.Series, window: int) -> pd.Series:
    log_hl = np.log(high / low)
    return np.sqrt((1.0 / (4.0 * np.log(2.0))) * (log_hl.pow(2).rolling(window).mean()))


def forward_sum(values: pd.Series, n: int, offset: int = 1) -> pd.Series:
    return values.shift(-offset).iloc[::-1].rolling(n, min_periods=n).sum().iloc[::-1]


def prepare_feature_frame(df: pd.DataFrame, config: VolSignalBuildConfig | None = None) -> pd.DataFrame:
    config = config or VolSignalBuildConfig()
    out = validate_ohlcv_frame(df)

    for window in config.vol_windows:
        out[f"parkinson_{window}"] = parkinson_volatility(out["high"], out["low"], window=window)

    out["parkinson_ratio_30_1440"] = out["parkinson_30"] / out["parkinson_1440"].replace(0.0, np.nan)
    out["parkinson_ratio_15_1440"] = out["parkinson_15"] / out["parkinson_1440"].replace(0.0, np.nan)

    for n in config.forward_return_horizons:
        out[f"target_fwd_ret_{n}"] = np.log(out["close"].shift(-n) / out["close"])

    log_ret_abs = np.log(out["close"] / out["close"].shift(1)).abs()
    for n in config.mar_horizons:
        out[f"target_mar_next{n}_close"] = forward_sum(log_ret_abs, n=n, offset=1) / float(n)

    return out


def _train_models_for_score_date(
    feature_df: pd.DataFrame,
    score_date: pd.Timestamp,
    config: VolSignalBuildConfig,
) -> dict[int, HorizonModel]:
    models: dict[int, HorizonModel] = {}
    for horizon in config.mar_horizons:
        train_start = score_date - timedelta(days=config.training_lookback_days)
        train_end = score_date - timedelta(minutes=horizon)
        target_col = f"target_mar_next{horizon}_close"

        mask = (feature_df["datetime_utc"] >= train_start) & (feature_df["datetime_utc"] < train_end)
        train = feature_df.loc[mask, list(config.feature_cols) + [target_col]].dropna()
        if len(train) < config.min_train_rows:
            raise ValueError(
                f"insufficient training rows for score_date={score_date.date()} "
                f"horizon={horizon}: {len(train)} < {config.min_train_rows}"
            )

        x_train = train[list(config.feature_cols)].to_numpy(dtype=float)
        y_train = train[target_col].to_numpy(dtype=float)
        intercept, coefficients = fit_linear_regression(x_train, y_train)

        models[horizon] = HorizonModel(
            horizon=horizon,
            intercept=intercept,
            coefficients=coefficients,
            train_start=train_start.isoformat(),
            train_end_exclusive=train_end.isoformat(),
            train_rows=int(len(train)),
        )
    return models


def _predict_with_models(
    frame: pd.DataFrame,
    models: dict[int, HorizonModel],
    config: VolSignalBuildConfig,
) -> pd.DataFrame:
    out = frame.copy()
    feature_block = out.loc[:, config.feature_cols]
    valid_mask = feature_block.notna().all(axis=1)
    if not valid_mask.any():
        for horizon in config.mar_horizons:
            out[f"pred_mar_{horizon}"] = np.nan
        return out

    x_valid = feature_block.loc[valid_mask].to_numpy()
    for horizon in config.mar_horizons:
        model = models[horizon]
        coeffs = np.asarray(model.coefficients, dtype=float)
        preds = x_valid @ coeffs + float(model.intercept)
        out[f"pred_mar_{horizon}"] = np.nan
        out.loc[valid_mask, f"pred_mar_{horizon}"] = preds
    return out


def score_date_range_for_history(
    feature_df: pd.DataFrame,
    start_date: str | pd.Timestamp,
    end_date: str | pd.Timestamp,
    config: VolSignalBuildConfig | None = None,
) -> pd.DataFrame:
    config = config or VolSignalBuildConfig()
    start = normalize_score_date(start_date)
    end = normalize_score_date(end_date)
    if end < start:
        raise ValueError("end_date must be on or after start_date")

    out_frames: list[pd.DataFrame] = []
    for score_date in pd.date_range(start=start, end=end, freq="D"):
        day_end = score_date + timedelta(days=1)
        day_rows = feature_df.loc[
            (feature_df["datetime_utc"] >= score_date) & (feature_df["datetime_utc"] < day_end)
        ]
        if day_rows.empty:
            continue
        models = _train_models_for_score_date(feature_df, score_date, config)
        out_frames.append(_predict_with_models(day_rows, models, config))

    if not out_frames:
        raise ValueError("no scoreable rows found in requested date range")
    return pd.concat(out_frames, ignore_index=True)


def blend_mar(
    pred_mar_1: pd.Series,
    pred_mar_3: pd.Series,
    pred_mar_5: pd.Series,
    weights: tuple[float, float, float] = (0.2, 0.3, 0.5),
) -> pd.Series:
    w1, w3, w5 = weights
    return w1 * pred_mar_1 + w3 * pred_mar_3 + w5 * pred_mar_5


def time_to_strike(df: pd.DataFrame, n_minutes: int = 15, dt_col: str = "datetime_utc") -> pd.Series:
    group_start = df[dt_col].dt.floor(f"{n_minutes}min")
    elapsed = ((df[dt_col] - group_start).dt.total_seconds() / 60.0).astype(int)
    return ((n_minutes - 2 - elapsed) % n_minutes) + 1


def assign_group_strike(df: pd.DataFrame, n_minutes: int = 15) -> pd.Series:
    ttl = time_to_strike(df, n_minutes=n_minutes)
    k_series = pd.Series(np.nan, index=df.index, dtype=float)
    setter_mask = ttl == n_minutes
    k_series.loc[setter_mask] = df.loc[setter_mask, "close"].to_numpy()
    k_series = k_series.ffill()

    if setter_mask.any():
        first_setter_index = setter_mask.idxmax()
        if first_setter_index > 0:
            k_series.loc[: first_setter_index - 1] = float(df["open"].iloc[0])
    else:
        k_series.loc[:] = float(df["open"].iloc[0])

    return k_series


def sigma_w_from_mar(mar_blend: pd.Series, ttl: pd.Series) -> pd.Series:
    mar_clean = pd.to_numeric(mar_blend, errors="coerce").clip(lower=0.0)
    sigma_1m = math.sqrt(math.pi / 2.0) * mar_clean
    return sigma_1m * np.sqrt(pd.to_numeric(ttl, errors="coerce"))


def attach_signal_columns(df: pd.DataFrame, config: VolSignalBuildConfig | None = None) -> pd.DataFrame:
    config = config or VolSignalBuildConfig()
    out = df.copy()
    out["mar_blend"] = blend_mar(
        out["pred_mar_1"],
        out["pred_mar_3"],
        out["pred_mar_5"],
        weights=config.mar_blend_weights,
    )
    out["ttl"] = time_to_strike(out, n_minutes=config.group_minutes)
    out["strike_K"] = assign_group_strike(out, n_minutes=config.group_minutes)
    out["sigma_W_ttl"] = sigma_w_from_mar(out["mar_blend"], out["ttl"])
    return out


def build_z_pool(
    scored_history_df: pd.DataFrame,
    score_date: str | pd.Timestamp,
    config: VolSignalBuildConfig | None = None,
) -> np.ndarray:
    config = config or VolSignalBuildConfig()
    score_date_ts = normalize_score_date(score_date)
    pool_start = score_date_ts - timedelta(days=config.z_pool_lookback_days)
    pool_end = score_date_ts - timedelta(minutes=config.group_minutes)

    history = scored_history_df.loc[
        (scored_history_df["datetime_utc"] >= pool_start)
        & (scored_history_df["datetime_utc"] < pool_end)
    ].copy()
    if history.empty:
        raise ValueError("no scored rows available in z-pool lookback window")

    if "sigma_W_ttl" not in history.columns or "ttl" not in history.columns:
        history = attach_signal_columns(history, config)

    z_parts: list[np.ndarray] = []
    for ttl in range(1, config.group_minutes + 1):
        ttl_rows = history.loc[history["ttl"] == ttl]
        if ttl_rows.empty:
            continue
        forward_col = f"target_fwd_ret_{ttl}"
        if forward_col not in ttl_rows.columns:
            raise ValueError(f"missing required forward-return column: {forward_col}")
        z = ttl_rows[forward_col] / ttl_rows["sigma_W_ttl"]
        z = z.replace([np.inf, -np.inf], np.nan).dropna()
        if not z.empty:
            z_parts.append(z.to_numpy(dtype=float))

    if not z_parts:
        raise ValueError("z-pool build produced zero valid z-scores")

    z_pool = np.sort(np.concatenate(z_parts))
    if z_pool.size < config.min_z_pool_size:
        raise ValueError(
            f"z-pool too small for score_date={score_date_ts.date()}: "
            f"{z_pool.size} < {config.min_z_pool_size}"
        )
    return z_pool


def build_daily_signal_artifact(
    ohlcv_df: pd.DataFrame,
    score_date: str | pd.Timestamp,
    config: VolSignalBuildConfig | None = None,
    extra_metadata: dict | None = None,
) -> DailySignalArtifact:
    config = config or VolSignalBuildConfig()
    score_date_ts = normalize_score_date(score_date)

    raw_df = validate_ohlcv_frame(ohlcv_df)
    if raw_df["datetime_utc"].min() > score_date_ts - timedelta(days=config.min_required_history_days):
        raise ValueError(
            "insufficient raw history for artifact build; "
            f"need data starting by {(score_date_ts - timedelta(days=config.min_required_history_days)).isoformat()}"
        )

    feature_df = prepare_feature_frame(raw_df, config)

    z_pool_start_day = score_date_ts - timedelta(days=config.z_pool_lookback_days)
    history_scored = score_date_range_for_history(
        feature_df,
        start_date=z_pool_start_day,
        end_date=score_date_ts - timedelta(days=1),
        config=config,
    )
    history_scored = attach_signal_columns(history_scored, config)
    z_pool = build_z_pool(history_scored, score_date=score_date_ts, config=config)
    score_day_models = _train_models_for_score_date(feature_df, score_date_ts, config)

    metadata = {
        "score_date": score_date_ts.date().isoformat(),
        "raw_min_datetime": raw_df["datetime_utc"].min().isoformat(),
        "raw_max_datetime": raw_df["datetime_utc"].max().isoformat(),
        "feature_rows": int(len(feature_df)),
        "history_scored_rows": int(len(history_scored)),
        "training_lookback_days": config.training_lookback_days,
        "z_pool_lookback_days": config.z_pool_lookback_days,
        "min_required_history_days": config.min_required_history_days,
        "artifact_version": 1,
    }
    if extra_metadata:
        metadata.update(extra_metadata)

    return DailySignalArtifact(
        score_date=score_date_ts.date().isoformat(),
        config=config,
        models=score_day_models,
        z_pool=z_pool,
        metadata=metadata,
    )


def fit_linear_regression(x: np.ndarray, y: np.ndarray) -> tuple[float, tuple[float, ...]]:
    if x.ndim != 2:
        raise ValueError("x must be a 2D array")
    if y.ndim != 1:
        raise ValueError("y must be a 1D array")
    if len(x) != len(y):
        raise ValueError("x and y must have matching row counts")

    x_design = np.column_stack([np.ones(len(x)), x])
    beta, *_ = np.linalg.lstsq(x_design, y, rcond=None)
    intercept = float(beta[0])
    coefficients = tuple(float(value) for value in beta[1:])
    return intercept, coefficients
