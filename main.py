# === main.py ===
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn
from config.settings import PORT
from routes.cron_routes import router as cron_routes
from routes.admin_routes import router as admin_routes
from routes.user_routes import router as user_routes

app = FastAPI(title="Prediction App")

app.include_router(cron_routes, prefix="/cron", tags=["Prediction App Cron Service"])
app.include_router(admin_routes, prefix="/admin", tags=["Prediction App Admin Service"])
app.include_router(user_routes, prefix="/user", tags=["Prediction App User Service"])

@app.get("/", response_class=HTMLResponse)
def root():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Prediction App</title>
        <style>
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: #f8f9fa;
                color: #333;
                padding: 40px;
                text-align: center;
            }
            .container {
                background: white;
                max-width: 600px;
                margin: auto;
                padding: 30px;
                border-radius: 12px;
                box-shadow: 0 8px 16px rgba(0,0,0,0.1);
            }
            h1 {
                font-size: 28px;
                margin-bottom: 20px;
                color: #2c3e50;
            }
            p {
                font-size: 16px;
                margin-bottom: 10px;
            }
            code {
                background: #eef;
                padding: 4px 8px;
                border-radius: 4px;
                font-family: monospace;
                color: #005;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🚀 Prediction App is Running!</h1>
            <p>Server URL:</p>
            <code>http://0.0.0.0:8088/</code>
        </div>
    </body>
    </html>
    """

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=True)