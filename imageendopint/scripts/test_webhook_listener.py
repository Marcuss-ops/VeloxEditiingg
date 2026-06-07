from fastapi import FastAPI, Request, File, UploadFile, Form
import uvicorn
import os
import json
from pathlib import Path

app = FastAPI()
received_dir = Path("outputs/webhook-received")
received_dir.mkdir(parents=True, exist_ok=True)

@app.post("/webhook")
async def receive_webhook(request: Request):
    form = await request.form()
    print(f"\n[WEBHOOK] Received form keys: {list(form.keys())}")
    job_json = form.get("job_json")
    print(f"[WEBHOOK] Job ID: {json.loads(job_json)['id'] if job_json else 'N/A'}")
    
    # Save files
    for key in form:
        value = form[key]
        if isinstance(value, UploadFile):
            content = await value.read()
            target_path = received_dir / value.filename
            target_path.write_bytes(content)
            print(f"[WEBHOOK] Saved file: {value.filename} ({len(content)} bytes)")
        else:
            print(f"[WEBHOOK] Form field: {key}")
            
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=9000)
