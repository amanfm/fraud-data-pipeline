import asyncio
import logging
import uvicorn
from sys import stderr
from fastapi import FastAPI, HTTPException
from pipeline import run_acm_daily_pipeline,run_acm_monthly_pipeline,load_and_concat_files
from contextlib import asynccontextmanager
from helper import get_latest_log_file, read_log_file, log_decorator


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(stream=stderr, level="INFO")
    yield
    models.clear()

app = FastAPI(lifespan=lifespan)



@app.get("/view_daily")
@log_decorator
async def get_view_daily(v_order_details_daily: str, v_payment_details_daily: str):
    message = run_acm_daily_pipeline(
        v_order_details_daily, v_payment_details_daily
    )
    if message is None:
        raise HTTPException(status_code=404, detail="WARNING: No new data uploaded!")
    return {"s3_path": message}


@app.get("/view_monthly")
@log_decorator
async def get_view_monthly(v_order_details_monthly: str, v_payment_details_monthly: str):
    message = run_acm_monthly_pipeline(
        v_order_details_monthly, v_payment_details_monthly
    )
    if message is None:
        raise HTTPException(status_code=404, detail="WARNING: No new data uploaded!")
    return {"s3_path": message}

@app.get("/acm_concat")
@log_decorator
async def get_acm(end_date):
    message = load_and_concat_files(end_date)
    if message is None:
        raise HTTPException(status_code=404, detail="WARNING: No new data uploaded!")
    return {"s3_path": message}

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=9000, reload=True)
