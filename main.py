from fastapi import FastAPI, Request
from datetime import datetime
import json

app = FastAPI()

@app.get("/")
def root():
    return {"status": "running"}

@app.post("/telemetry")
async def receive_telemetry(request: Request):
    body = await request.json()
    print("Received:", body)
    return {"status": "ok"}