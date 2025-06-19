from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from lakehouse.flows.ingest_klines_flow import ingest_binance_klines_flow # Ensure this path is correct

# Define the deployment for the klines ingestion flow
klines_ingestion_deployment = Deployment.build_from_flow(
    flow=ingest_binance_klines_flow,
    name="Binance Klines Ingestion - Daily",
    # description="Scheduled daily ingestion of Binance klines data for BTCUSDT.",
    version="1.0",
    tags=["binance", "klines", "ingestion", "data-lakehouse"],

    # Define a schedule for the deployment
    # This example runs the flow daily at 1:00 AM UTC for BTCUSDT 1m data from the previous day.
    # Prefect CronSchedule uses standard cron syntax.
    schedule=(CronSchedule(cron="0 1 * * *", timezone="UTC")), # Runs daily at 1:00 AM UTC

    # Define default parameters for this scheduled deployment
    # These can be overridden when triggering manual runs.
    parameters={
        "symbol": "BTCUSDT",
        "interval": "1m",
        # For a daily schedule, you'd typically calculate `start_date` and `end_date`
        # dynamically, e.g., to fetch "yesterday's" data.
        # Prefect allows using Jinja templating for dynamic parameters if needed,
        # or this logic can be embedded within the flow itself.
        # For simplicity here, we're using fixed dates, but in a real scenario,
        # the flow itself might be designed to default to "yesterday" if no dates are passed.
        # Or, a small wrapper script could generate these dates before triggering the flow.
        "start_date": "2024-01-01", # Placeholder: In practice, make this dynamic e.g. yesterday
        "end_date": "2024-01-02",   # Placeholder: In practice, make this dynamic e.g. today
        "asset_type": "spot"
    },

    # Specify the entrypoint for the flow, if different from the current file or if the flow is in a module
    # entrypoint="lakehouse/flows/ingest_klines_flow.py:ingest_binance_klines_flow", # path-to-file.py:flow-function-name

    # Work pool and queue configuration (optional, defaults usually work for local testing)
    # work_pool_name="my-work-pool",
    # work_queue_name="my-work-queue",

    # Path to the directory containing the flow code.
    # If None, Prefect tries to infer it. It's good practice to set it if your
    # deployment definition is separate from your flow code directory.
    # path="." # Assuming this script is run from the repo root, and flows are in ./lakehouse/flows
    # Alternatively, if your flows are installed as part of a Python package:
    # from lakehouse import flows
    # entrypoint = "lakehouse.flows.ingest_klines_flow.ingest_binance_klines_flow"

    # If your flow has dependencies specified in a requirements.txt or environment.yml,
    # you can specify infrastructure overrides here for execution environments like Docker.
    # infra_overrides={"env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}},
)

if __name__ == "__main__":
    # This will define and apply the deployment to your Prefect backend (local or cloud).
    # You need to be logged into a Prefect backend for this to work.
    # Example: `prefect server start` (for local) or `prefect cloud login`

    # To apply the deployment (i.e., register it with the Prefect API):
    klines_ingestion_deployment.apply()

    print(f"Deployment '{klines_ingestion_deployment.name}' for flow '{klines_ingestion_deployment.flow_name}' created/updated.")
    print("To see this deployment in the UI, ensure your Prefect server or agent is running and connected.")
    print("The agent will pick up scheduled runs based on the work pool and queue configuration.")

    # You can also build and apply deployments for other flows (e.g., trades flow) similarly.
    # from lakehouse.flows.ingest_trades_flow import ingest_binance_trades_flow
    # trades_ingestion_deployment = Deployment.build_from_flow(...)
    # trades_ingestion_deployment.apply()
