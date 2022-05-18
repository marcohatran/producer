import logging
import os
from fastapi import FastAPI, Body
from typing import Dict, Any, List
from mangum import Mangum
from app.glue_api_provider import *
from app.parser import *
from app.utils import *
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

API_STAGE = os.environ["API_STAGE"]

app = FastAPI(root_path=f"/{API_STAGE}")



@app.head("/status")
def status():
    return {}

@app.get("/health/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}

@app.post("/execution-plans")
def execution_plans(body: Dict[str, Any] = Body(...)):
    logger.info(f"Received execution plan:{json.dumps(body)}")
    execution_id = body["id"]
    save_to_s3('execution-plans_' + str(execution_id), body)
    return execution_id

@app.post("/execution-events")
def execution_events(body: List[Dict[str, Any]] = Body(...)):
    logger.info(f"Received execution event: {json.dumps(body)}")
    executionPlanID = body[0]["planId"]
    save_to_s3('execution-events_' + str(executionPlanID), body)
    metrics = parseExecutionPlanFromExecutionEvents(body[0])
    logger.info("Finished execution_events")
    return executionPlanID

@app.post("/execution-failure")
def execution_failure(body: Dict[str, Any] = Body(...)):
    fail_json = {"status" : "failure",
                 "time": datetime.now()}
    save_to_s3('failure_' + str(datetime.now()), fail_json)
    return {}


def lambda_handler(event, context):
    handler = Mangum(app, api_gateway_base_path=f"/{API_STAGE}")
    return handler(event, context)
