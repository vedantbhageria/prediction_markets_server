"""HTTP API over the prediction_markets database.

    uvicorn api.main:app --host 127.0.0.1 --port 8000

Settings come from the project's .env (see api/config.py); keys are made with
`python -m api.keys`. The client for Python is pmapi.py at the project root.
"""

__version__ = "1.0.0"
