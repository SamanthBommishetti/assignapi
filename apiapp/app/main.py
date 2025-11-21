from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
import os
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from app.auth.routes.auth_router import auth_router
from app.user.routes.user_router import user_router
from app.analysis.routes.analysis_router import analysis_router
from app.dashboard.routes.dashboard_router import dashboard_router
from app.query.routes.query_router import query_router
from app.queryresult.routes.queryresult_router import queryresult_router
from app.context_table.routes.context_table_router import context_table_router
from app.entity.router import entity_router
from app.navigation.router import navigation_router
from app.general_query.router import general_query_router
from app.genai_tools.tools import tools_router
from app.core.config_loader import settings
from datetime import datetime
from datetime import timezone

app = FastAPI(
    title="FreddieMac FastAPI 🚀",
    description="A FastAPI application for managing FreddieMac data and analytics.",
    version="1.0.0",
    debug=True
)

# Add SessionMiddleware
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.JWT_SECRET_KEY,
    max_age=60 * 60 * 24 * 30,  # 30 days
    same_site="lax",
)

# CORS Middleware to allow cross-origin requests (adjust origins as needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update with specific origins in production (e.g., ["http://localhost:3000"])
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers with consistent prefix
app.include_router(auth_router, prefix="/api", tags=["Authentication"])
app.include_router(user_router, prefix="/api", tags=["Users"])
app.include_router(analysis_router, prefix="/api", tags=["Analysis"])
app.include_router(dashboard_router, prefix="/api", tags=["Dashboards"])
app.include_router(query_router, prefix="/api", tags=["Queries"])
app.include_router(queryresult_router, prefix="/api", tags=["Query Results"])
app.include_router(context_table_router, prefix="/api", tags=["Context Tables"])
app.include_router(entity_router, prefix="/api", tags=["Entities"])
app.include_router(navigation_router, prefix="/api", tags=["Navigations"])
app.include_router(general_query_router, prefix="/api", tags=["General Queries"])
app.include_router(tools_router, prefix="/api", tags=["Utils"])

# Get the absolute path to the directory where this script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to static files directories
STATIC_DIR = os.path.join(BASE_DIR, "out_csvfiles")
GENERAL_STATIC_DIR = os.path.join(BASE_DIR, "general_out_csvfiles")

# Mount static file directories with error handling
try:
    app.mount("/out_csvfiles", StaticFiles(directory=STATIC_DIR), name="out_csvfiles")
    if not os.path.exists(STATIC_DIR):
        print(f"Warning: Static directory {STATIC_DIR} does not exist.")
except Exception as e:
    print(f"Error mounting /out_csvfiles: {e}")

try:
    app.mount("/general_out_csvfiles", StaticFiles(directory=GENERAL_STATIC_DIR), name="general_out_csvfiles")
    if not os.path.exists(GENERAL_STATIC_DIR):
        print(f"Warning: Static directory {GENERAL_STATIC_DIR} does not exist.")
except Exception as e:
    print(f"Error mounting /general_out_csvfiles: {e}")

# Root endpoint
@app.get("/", summary="Welcome Endpoint")
def root():
    return {"message": "Welcome to FreddieMac FastAPI 🚀", "status": "running", "version": "1.0.0"}

# Health check endpoint
@app.get("/health", summary="Health Check")
def health_check():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat(), "message": "Service is operational"}

# Error handling for 404
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return {"error": exc.detail, "status_code": exc.status_code}, exc.status_code

#if __name__ == "__main__":
#    import uvicorn
#    uvicorn.run(app, host="0.0.0.0", port=8000)
