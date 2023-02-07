from fastapi import FastAPI
import uvicorn
from routes import router

app = FastAPI()
app.include_router(router)


if __name__ == "__main__":
    uvicorn.run("endpoint:app", host='0.0.0.0', port=5000)
