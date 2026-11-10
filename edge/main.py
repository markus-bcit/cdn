from fastapi import FastAPI

app = FastAPI(title="Edge Cache Proxy")

@app.get("/health")
def health():
    return {"status": "healthy"}
