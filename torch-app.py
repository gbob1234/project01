import platform
import sys
import time

import torch
import torch.nn as nn
from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI(
    title="PyTorch GPU Base Image Test",
    version="1.0.0",
)


class PredictRequest(BaseModel):
    batch_size: int = 32
    input_size: int = 1024


class TestModel(nn.Module):
    def __init__(self, input_size: int):
        super().__init__()

        self.model = nn.Sequential(
            nn.Linear(input_size, 2048),
            nn.ReLU(),
            nn.Linear(2048, 1024),
            nn.ReLU(),
            nn.Linear(1024, 10),
        )

    def forward(self, x):
        return self.model(x)


@app.get("/")
def root():
    return {
        "service": "pytorch-gpu-test",
        "status": "ok",
    }


@app.get("/health")
def health():
    cuda_available = torch.cuda.is_available()

    response = {
        "status": "healthy",
        "python_version": sys.version,
        "platform": platform.platform(),
        "torch_version": torch.__version__,
        "cuda_available": cuda_available,
        "torch_cuda_version": torch.version.cuda,
        "cudnn_available": torch.backends.cudnn.is_available(),
        "cudnn_version": torch.backends.cudnn.version(),
    }

    if cuda_available:
        response.update(
            {
                "gpu_count": torch.cuda.device_count(),
                "gpu_name": torch.cuda.get_device_name(0),
                "gpu_capability": torch.cuda.get_device_capability(0),
            }
        )

    return response


@app.post("/predict")
def predict(request: PredictRequest):
    if not torch.cuda.is_available():
        return {
            "status": "error",
            "message": "CUDA is not available",
        }

    device = torch.device("cuda:0")

    model = TestModel(request.input_size)
    model = model.to(device)
    model.eval()

    x = torch.randn(
        request.batch_size,
        request.input_size,
        device=device,
    )

    torch.cuda.synchronize()

    start = time.perf_counter()

    with torch.no_grad():
        output = model(x)

    torch.cuda.synchronize()

    elapsed = time.perf_counter() - start

    return {
        "device": str(device),
        "gpu_name": torch.cuda.get_device_name(0),
        "batch_size": request.batch_size,
        "input_size": request.input_size,
        "output_shape": list(output.shape),
        "sample_output": output[0].cpu().tolist(),
        "elapsed_seconds": elapsed,
        "allocated_memory_mb": (
            torch.cuda.memory_allocated() / 1024 / 1024
        ),
        "reserved_memory_mb": (
            torch.cuda.memory_reserved() / 1024 / 1024
        ),
    }
