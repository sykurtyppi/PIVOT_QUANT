class ProbabilityCalibrator:
    def __init__(self, base_model, method: str):
        self.base_model = base_model
        self.method = method
        self.calibrator = None

    def fit(self, X_calib, y_calib):
        import numpy as np

        probs = self.base_model.predict_proba(X_calib)[:, 1]
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
        p1 = probs[:, 1]
        if self.method == "isotonic":
            p1c = self.calibrator.transform(p1)
        else:
            p1c = self.calibrator.predict_proba(p1.reshape(-1, 1))[:, 1]
        return np.vstack([1 - p1c, p1c]).T

    def predict(self, X):
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)
