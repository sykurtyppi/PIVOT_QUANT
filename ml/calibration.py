class ProbabilityCalibrator:
    def __init__(self, base_model, method: str):
        self.base_model = base_model
        self.method = method
        self.calibrator = None

    @staticmethod
    def _positive_class_proba(probs):
        """Return the P(class=1) column with a clear error if the base model is
        not a proper binary classifier. A single-class base model returns a
        1-column ``predict_proba``; indexing ``[:, 1]`` would otherwise raise an
        opaque ``IndexError`` mid-training (and, with no try/except around the
        train loop, abort the whole run). Fail with a diagnosable message so the
        caller can skip the degenerate horizon instead."""
        import numpy as np

        arr = np.asarray(probs)
        if arr.ndim != 2 or arr.shape[1] < 2:
            raise ValueError(
                "ProbabilityCalibrator requires a binary base model: predict_proba "
                f"returned shape {arr.shape} (need 2 columns). The base model "
                "appears to be single-class and cannot be calibrated."
            )
        return arr[:, 1]

    def fit(self, X_calib, y_calib):
        import numpy as np

        probs = self._positive_class_proba(self.base_model.predict_proba(X_calib))
        if self.method == "isotonic":
            from sklearn.isotonic import IsotonicRegression

            iso = IsotonicRegression(out_of_bounds="clip")
            iso.fit(probs, y_calib)
            self.calibrator = iso
        elif self.method == "sigmoid":
            from sklearn.linear_model import LogisticRegression

            lr = LogisticRegression(solver="lbfgs")
            lr.fit(probs.reshape(-1, 1), y_calib)
            self.calibrator = lr
        else:
            self.calibrator = None
        return self

    def predict_proba(self, X):
        import numpy as np

        probs = self.base_model.predict_proba(X)
        if self.calibrator is None:
            return probs
        p1 = self._positive_class_proba(probs)
        if self.method == "isotonic":
            p1c = self.calibrator.transform(p1)
        else:
            p1c = self.calibrator.predict_proba(p1.reshape(-1, 1))[:, 1]
        return np.vstack([1 - p1c, p1c]).T

    def predict(self, X, threshold: float = 0.5):
        import numpy as np

        return (self.predict_proba(X)[:, 1] >= threshold).astype(int)
