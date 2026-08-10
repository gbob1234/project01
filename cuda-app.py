import platform
import sys
import time

import cupy as cp
from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI(
    title="CUDA Base Image Test",
    version="1.0.0",
)


class PredictRequest(BaseModel):
    size: int = 2000


@app.get("/")
def root():
    return {
        "service": "cuda-test",
        "status": "ok",
    }


@app.get("/health")
def health():
    device_count = cp.cuda.runtime.getDeviceCount()

    devices = []

    for device_id in range(device_count):
        props = cp.cuda.runtime.getDeviceProperties(device_id)

        devices.append(
            {
                "device_id": device_id,
                "name": props["name"].decode(),
                "total_global_mem": props["totalGlobalMem"],
                "major": props["major"],
                "minor": props["minor"],
            }
        )

    return {
        "status": "healthy",
        "python_version": sys.version,
        "platform": platform.platform(),
        "cupy_version": cp.__version__,
        "cuda_runtime_version": cp.cuda.runtime.runtimeGetVersion(),
        "gpu_count": device_count,
        "devices": devices,
    }


@app.post("/predict")
def predict(request: PredictRequest):
    size = request.size

    start = time.perf_counter()

    a = cp.random.random((size, size), dtype=cp.float32)
    b = cp.random.random((size, size), dtype=cp.float32)

    result = cp.matmul(a, b)

    # GPU 비동기 작업 완료 대기
    cp.cuda.Stream.null.synchronize()

    elapsed = time.perf_counter() - start

    return {
        "device": "cuda",
        "matrix_size": size,
        "result_shape": list(result.shape),
        "sample_result": float(result[0, 0].get()),
        "elapsed_seconds": elapsed,
    }
