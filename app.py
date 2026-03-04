import os
import uuid
import glob
import time
from pathlib import Path

import httpx
from fastapi import FastAPI, UploadFile, File, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

load_dotenv()

RECAPTCHA_SITE_KEY = os.getenv("RECAPTCHA_SITE_KEY", "")
RECAPTCHA_SECRET_KEY = os.getenv("RECAPTCHA_SECRET_KEY", "")
RECAPTCHA_THRESHOLD = 0.5
RECAPTCHA_VERIFY_URL = "https://www.google.com/recaptcha/api/siteverify"

from services.import_master import import_master_data, MASTER_CONFIGS
from services.generate_excel import generate_opening_so
from services.validate_headers import validate_sap_headers

app = FastAPI(title="SAP SO to NetSuite Converter")
templates = Jinja2Templates(directory="templates")

TEMP_DIR = Path(__file__).parent / "temp"
TEMP_DIR.mkdir(exist_ok=True)

MAX_TEMP_AGE_SECONDS = 3600


def _cleanup_temp():
    """Remove temp files older than MAX_TEMP_AGE_SECONDS."""
    now = time.time()
    for f in TEMP_DIR.glob("*"):
        if f.name == ".gitkeep":
            continue
        if now - f.stat().st_mtime > MAX_TEMP_AGE_SECONDS:
            f.unlink(missing_ok=True)


async def _verify_recaptcha(token: str) -> tuple[bool, float]:
    """Verify reCAPTCHA v3 token with Google. Returns (passed, score)."""
    if not RECAPTCHA_SECRET_KEY:
        return True, 1.0
    async with httpx.AsyncClient(timeout=5) as client:
        resp = await client.post(RECAPTCHA_VERIFY_URL, data={
            "secret": RECAPTCHA_SECRET_KEY,
            "response": token,
        })
        result = resp.json()
    success = result.get("success", False)
    score = result.get("score", 0.0)
    return success and score >= RECAPTCHA_THRESHOLD, score


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    master_options = {k: cfg["sheet"] for k, cfg in MASTER_CONFIGS.items()}
    return templates.TemplateResponse("index.html", {
        "request": request,
        "master_options": master_options,
        "recaptcha_site_key": RECAPTCHA_SITE_KEY,
    })


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...), recaptcha_token: str = Form("")):
    """Upload Excel file and return a temp file ID."""
    passed, score = await _verify_recaptcha(recaptcha_token)
    if not passed:
        return JSONResponse(
            {"status": "error", "message": f"reCAPTCHA verification failed (score={score:.2f}). Please try again."},
            status_code=403,
        )
    _cleanup_temp()
    file_id = str(uuid.uuid4())
    ext = Path(file.filename).suffix or ".xlsx"
    dest = TEMP_DIR / f"{file_id}{ext}"
    content = await file.read()
    dest.write_bytes(content)
    return {"file_id": file_id, "filename": file.filename, "ext": ext}


@app.post("/api/validate")
async def validate_headers(
    file_id: str = Form(...),
    ext: str = Form(".xlsx"),
):
    """Validate SAP report column headers and detect discount columns."""
    source = TEMP_DIR / f"{file_id}{ext}"
    if not source.exists():
        return JSONResponse({"status": "error", "message": "Uploaded file not found. Please upload again."}, status_code=404)
    try:
        result = validate_sap_headers(str(source))
    except Exception as e:
        result = {"status": "error", "message": str(e), "columns_valid": False, "column_errors": [], "discounts": []}
    return result


@app.post("/api/import-master")
async def import_master(
    file_id: str = Form(...),
    ext: str = Form(".xlsx"),
    master_type: str = Form(...),
):
    """Import selected master data from the uploaded file."""
    source = TEMP_DIR / f"{file_id}{ext}"
    if not source.exists():
        return JSONResponse({"status": "error", "message": "Uploaded file not found. Please upload again."}, status_code=404)
    try:
        result = import_master_data(str(source), master_type)
    except Exception as e:
        result = {"status": "error", "message": str(e)}
    return result


@app.post("/api/generate")
async def generate(
    file_id: str = Form(...),
    ext: str = Form(".xlsx"),
):
    """Generate Opening SO Excel from the uploaded file."""
    source = TEMP_DIR / f"{file_id}{ext}"
    if not source.exists():
        return JSONResponse({"status": "error", "message": "Uploaded file not found. Please upload again."}, status_code=404)

    output_id = str(uuid.uuid4())
    output_path = TEMP_DIR / f"{output_id}.xlsx"
    try:
        result = generate_opening_so(str(source), str(output_path))
        result["output_id"] = output_id
    except Exception as e:
        result = {"status": "error", "message": str(e)}
    return result


@app.get("/api/download/{output_id}")
async def download(output_id: str):
    """Download the generated Excel file."""
    path = TEMP_DIR / f"{output_id}.xlsx"
    if not path.exists():
        return JSONResponse({"status": "error", "message": "File not found."}, status_code=404)
    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="output_opening_so.xlsx",
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=True)
