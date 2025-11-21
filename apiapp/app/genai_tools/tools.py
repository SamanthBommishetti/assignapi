from fastapi import APIRouter, HTTPException
import requests
import os
from dotenv import load_dotenv
from pydantic import BaseModel
import re
from app.core.database import engine
from app.data_loader import dummy_data_loader as data_loader

# Load environment variables
load_dotenv()

tools_router = APIRouter(prefix="/utils", tags=["Utils"])

# class PredictionInput(BaseModel):
#     question: str
#     context: str

# @tools_router.post("/predict")
# def predict(input_data: PredictionInput):
#     # Debug environment variables
#     host = os.getenv("DATABRICKS_HOST")
#     endpoint_name = os.getenv("DATABRICKS_ENDPOINT_NAME")
#     token = os.getenv("DATABRICKS_TOKEN")
#     # print("DATABRICKS_HOST (raw):", host)
#     # print("DATABRICKS_ENDPOINT_NAME (raw):", endpoint_name)
#     # print("DATABRICKS_TOKEN:", token[:5] + "..." if token else "None")

#     if not host or not endpoint_name:
#         raise HTTPException(status_code=500, detail="Databricks host or endpoint name not configured.")

#     # Clean host to remove any extra paths
#     host = re.sub(r'/serving-endpoints/.*$', '', host).rstrip('/')
#     if not host.startswith(('http://', 'https://')):
#         host = f"https://{host}"
#     # print("DATABRICKS_HOST (cleaned):", host)

#     # Clean endpoint_name to ensure it's just the endpoint name, not a full URL
#     endpoint_name = re.sub(r'^https?://[^/]+/serving-endpoints/|/invocations.*$', '', endpoint_name).strip()
#     if not endpoint_name:
#         raise HTTPException(status_code=500, detail="Invalid Databricks endpoint name.")
#     # print("DATABRICKS_ENDPOINT_NAME (cleaned):", endpoint_name)

#     # Construct URL
#     url = f"{host}/serving-endpoints/{endpoint_name}/invocations"
#     # print("Constructed URL:", url)

#     headers = {
#         "Authorization": f"Bearer {token}",
#         "Content-Type": "application/json",
#     }
#     # Construct payload matching the new schema with question and context
#     payload = {
#         "inputs": [{
#             "question": input_data.question,
#             "context": input_data.context
#         }]
#     }
#     # print("Sent Payload:", payload)  # Debug the exact payload sent

#     try:
#         response = requests.post(url, headers=headers, json=payload)
#         response.raise_for_status()
#         result = response.json()
#         if "predictions" in result and result["predictions"]:
#             return {"abfs_path": result["predictions"][0]["output_path"]}
#         raise HTTPException(status_code=500, detail="No predictions found in Databricks response")
#     except requests.exceptions.RequestException as e:
#         raise HTTPException(status_code=500, detail=f"Error connecting to Databricks endpoint: {str(e)}")
    
@tools_router.get("/load_dummy_data")
def load_dummy_data():
    data_loader.load_dummy_data(engine)
    return {"message": "Data Loaded Successfully"}
