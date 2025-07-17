# BuildOps‐Fivetran Connector

This repo contains a custom connector to sync BuildOps data into Fivetran.

## Setup

1. **Fork or clone** this repo to GitHub.
2. Copy `.env-example` → `.env` and fill in your credentials.
3. (Optional) Edit `configuration.json` for local debug.

## Local Testing

```bash
pip install -r requirements.txt
export $(grep -v '^#' .env | xargs)
python app.py
# or for SDK debug:
python app.py --debug
