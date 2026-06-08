from fastapi import FastAPI

app = FastAPI(title="Internal Video Service")

@app.get("/api/v1/videos")
def get_videos():
    return [
        {"id": 1, "title": "Squid Game - Season 3"},
        {"id": 2, "title": "The Witcher"}
    ]
