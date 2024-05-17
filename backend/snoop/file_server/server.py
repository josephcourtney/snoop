from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

app = FastAPI()


@app.get("/media/{file_path:path}")
def get_media(file_path: str) -> FileResponse:
    full_path = Path("media") / file_path
    if full_path.exists():
        return FileResponse(full_path)
    raise HTTPException(status_code=404, detail="File not found")
