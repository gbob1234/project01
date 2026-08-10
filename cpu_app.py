import os
import platform
import sys
import time

import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI(
    title="CPU Base Image Test",
    version="1.0.0",
)


class PredictRequest(BaseModel):
    size: int = 1000


@app.get("/")
def root():
    return {
        "service": "cpu-test",
        "status": "ok",
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "python_version": sys.version,
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "numpy_version": np.__version__,
    }


@app.post("/predict")
def predict(request: PredictRequest):
    size = request.size

    start = time.perf_counter()

    a = np.random.rand(size, size).astype(np.float32)
    b = np.random.rand(size, size).astype(np.float32)

    result = np.matmul(a, b)

    elapsed = time.perf_counter() - start

    return {
        "device": "cpu",
        "matrix_size": size,
        "result_shape": list(result.shape),
        "sample_result": float(result[0][0]),
        "elapsed_seconds": elapsed,
    }
