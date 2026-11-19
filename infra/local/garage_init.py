"""Prépare Garage : layout du nœud, clé d'accès, bucket du lac. Idempotent.

Utilise l'API d'administration HTTP de Garage, sans dépendance externe.
"""

import json
import os
import time
import urllib.error
import urllib.request

ADMIN = os.getenv("GARAGE_ADMIN", "http://garage:3903")
TOKEN = os.environ["GARAGE_ADMIN_TOKEN"]
KEY = os.environ["S3_ACCESS_KEY"]
SECRET = os.environ["S3_SECRET_KEY"]
BUCKET = os.getenv("BUCKET", "lake")


def call(method: str, path: str, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{ADMIN}{path}", data=data, method=method, headers={
        "Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def wait() -> dict:
    for _ in range(60):
        try:
            return call("GET", "/v1/status")
        except (urllib.error.URLError, ConnectionError):
            time.sleep(1)
    raise SystemExit("Garage ne répond pas")


def main() -> None:
    status = wait()
    layout = call("GET", "/v1/layout")
    if not layout["roles"]:
        call("POST", "/v1/layout", [{"id": status["node"], "zone": "local",
                                     "capacity": 20 * 1024**3, "tags": []}])
        call("POST", "/v1/layout/apply", {"version": layout["version"] + 1})
        print("layout appliqué")

    if KEY not in {k["id"] for k in call("GET", "/v1/key?list")}:
        call("POST", "/v1/key/import", {"accessKeyId": KEY, "secretAccessKey": SECRET,
                                         "name": "lake"})
        print("clé importée")

    bucket = next((b for b in call("GET", "/v1/bucket?list")
                   if BUCKET in b.get("globalAliases", [])), None)
    if bucket is None:
        bucket = call("POST", "/v1/bucket", {"globalAlias": BUCKET})
        print(f"bucket {BUCKET} créé")
    call("POST", "/v1/bucket/allow", {"bucketId": bucket["id"], "accessKeyId": KEY,
                                      "permissions": {"read": True, "write": True,
                                                      "owner": True}})
    print("prêt")


if __name__ == "__main__":
    main()
