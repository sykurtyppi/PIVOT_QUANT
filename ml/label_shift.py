"""Rolling prior label-shift correction for inference-time probabilities.

Formula (Bayes odds-ratio):
    P_corrected = P_raw * r / (P_raw * r + (1 - P_raw))
    where r = (pi_current / pi_train) / ((1 - pi_current) / (1 - pi_train))

This corrects for the mismatch between the class prior seen during training
(pi_train) and the empirically-observed rolling prior at inference time
(pi_current).  When pi_current < pi_train (fewer rejects than the model
expects), the correction pushes predicted probabilities down toward the true
base rate, reducing false-reject signals.

References:
  Saerens et al. (2002); Lipton et al. (2018) ICML;
  Flores et al. arXiv:2506.14540.
"""
from __future__ import annotations

import math


def correct_prior_shift(
    p_raw: float,
    pi_train: float,
    pi_current: float,
) -> float:
    """Return the prior-corrected probability for a single prediction.

    Args:
        p_raw:      Raw model probability for class=1 (0 < p_raw < 1).
        pi_train:   Class-1 prevalence in the training set (from manifest stats).
        pi_current: Rolling empirical class-1 prevalence from recent labeled events.

    Returns:
        Corrected probability clamped to [1e-6, 1-1e-6].
    """
    eps = 1e-6
    p = max(eps, min(1.0 - eps, float(p_raw)))
    pi_t = max(eps, min(1.0 - eps, float(pi_train)))
    pi_c = max(eps, min(1.0 - eps, float(pi_current)))

    if math.isclose(pi_t, pi_c, rel_tol=1e-4):
        return p

    r = (pi_c / pi_t) / ((1.0 - pi_c) / (1.0 - pi_t))
    p_corrected = p * r / (p * r + (1.0 - p))
    return max(eps, min(1.0 - eps, p_corrected))


def rolling_class_rate(
    db_path: str,
    *,
    target: str,
    horizon: int,
    window_days: int,
    min_rows: int = 20,
) -> float | None:
    """Query SQLite for the empirical class-1 rate for `target` over the last
    `window_days` days, restricted to *resolved* events only.

    Only events with a non-NULL ``resolution_min`` in ``event_labels`` are
    counted.  This ensures pi_current is measured on the same population as
    pi_train (which also filters unresolved events via RF_FILTER_UNRESOLVED_EVENTS),
    preventing over-correction when unresolved/timeout events are present.

    `target` must be either "reject" or "break".
    Returns None when there are fewer than `min_rows` resolved labeled rows,
    to avoid noisy corrections from tiny samples.
    """
    import sqlite3
    import time

    if target not in ("reject", "break"):
        return None

    cutoff_ms = int((time.time() - window_days * 86400) * 1000)
    col = target  # "reject" or "break" — both are columns in event_labels

    try:
        con = sqlite3.connect(db_path, timeout=5)
        try:
            # Join prediction_log (for ts_prediction timestamp) with
            # event_labels (for labels and resolution_min).
            # prediction_log has one row per event (not per horizon), so we
            # join on event_id only and filter horizon_min from event_labels.
            # Only include resolved events so pi_current matches the training
            # distribution (which filters unresolved via RF_FILTER_UNRESOLVED_EVENTS).
            cur = con.execute(
                f"""
                SELECT COUNT(*)                                           AS total,
                       SUM(CASE WHEN el.{col} = 1 THEN 1 ELSE 0 END)    AS pos_count
                FROM   prediction_log   AS pl
                JOIN   event_labels     AS el
                       ON  el.event_id    = pl.event_id
                WHERE  el.horizon_min           = ?
                  AND  el.{col}                 IS NOT NULL
                  AND  el.resolution_min        IS NOT NULL
                  AND  pl.ts_prediction         >= ?
                """,
                (horizon, cutoff_ms),
            )
            row = cur.fetchone()
        finally:
            con.close()
    except Exception:
        return None

    if row is None:
        return None
    total, pos_count = row
    if total is None or int(total) < min_rows:
        return None
    return float(int(pos_count)) / float(int(total))
