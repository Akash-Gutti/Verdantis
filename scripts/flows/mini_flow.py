from prefect import flow, task

from services.common.bus import publish


@task
def emit():
    publish("FlowStarted", {"flow": "mini"})
    return "ok"


@flow(log_prints=True)
def mini_flow():
    print("running mini flow…")
    return emit()


if __name__ == "__main__":
    mini_flow()
