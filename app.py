import os
import json
import requests
from flask import Flask, request, jsonify
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

app = Flask(__name__)

def validate_configuration(cfg):
    required = ["host", "client_id", "client_secret", "tenant_id"]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        raise ValueError(f"Missing configuration keys: {missing}")

def get_access_token(cfg):
    url = f"{cfg['host']}/v1/auth/token"
    resp = requests.post(url, json={
        "clientId": cfg["client_id"],
        "clientSecret": cfg["client_secret"]
    })
    resp.raise_for_status()
    return resp.json()["access_token"]

def get_customers(cfg, state):
    token = get_access_token(cfg)
    headers = {
        "Authorization": f"Bearer {token}",
        "tenantId": cfg["tenant_id"]
    }
    resp = requests.get(f"{cfg['host']}/v1/customers", headers=headers)
    resp.raise_for_status()
    return resp.json().get("items", [])

def schema(cfg):
    """
    Define your schema here. Example for a single table:
    """
    return [
        {
            "name": "customers",
            "columns": [
                {"name": "id",      "type": "string", "nullable": False},
                {"name": "name",    "type": "string"},
                {"name": "status",  "type": "string"}
            ],
            "primary_key": ["id"]
        }
    ]

def update(cfg, state):
    """
    Called on each sync. Yield Operations for upsert/checkpoint.
    """
    log.info("Starting sync for BuildOps customers")
    validate_configuration(cfg)
    last_sync = state.get("last_sync_time")
    data = get_customers(cfg, state)
    new_sync_time = int(__import__("time").time())
    for record in data:
        yield op.upsert(table="customers", data=record)
    yield op.checkpoint({"last_sync_time": new_sync_time})

connector = Connector(update=update, schema=schema)

@app.route("/sync", methods=["POST"])
def sync():
    body = request.get_json(force=True)
    # You can verify the incoming secret or signature here if desired
    cfg = {
        "host":            os.getenv("BUILDOPS_HOST", "https://api.buildops.com"),
        "client_id":       os.environ["BUILDOPS_CLIENT_ID"],
        "client_secret":   os.environ["BUILDOPS_CLIENT_SECRET"],
        "tenant_id":       os.environ["BUILDOPS_TENANT_ID"]
    }
    method = body.get("method")
    state  = body.get("state", {})
    if method == "update":
        gen = connector.update(configuration=cfg, state=state)
    else:
        gen = connector.schema(configuration=cfg)
    result = connector.handle(gen)
    return jsonify(result)

if __name__ == "__main__":
    # Local debug
    with open("configuration.json") as f:
        cfg = json.load(f)
    connector.debug(configuration=cfg)
