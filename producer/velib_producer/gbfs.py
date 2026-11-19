"""Lecture du flux GBFS de Vélib' Métropole et mise en forme des messages Kafka.

GBFS est le format ouvert des systèmes de vélos en libre-service. Deux flux
nous intéressent : station_status (vélos et bornes disponibles, mis à jour en
continu) et station_information (nom, position, capacité, quasi statique).
Le serveur répond 403 à un client qui ne s'identifie pas : on envoie un
User-Agent explicite.
"""

from dataclasses import dataclass, field

import httpx

BASE = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"
STATUS_URL = f"{BASE}/station_status.json"
INFO_URL = f"{BASE}/station_information.json"
USER_AGENT = "velib-data-pipeline/1.0 (demo; +https://github.com/SassiBen/velib-data-pipeline)"
SCHEMA_VERSION = 1


def fetch(url: str, client: httpx.Client) -> dict:
    r = client.get(url, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return r.json()


def bike_types(types: list[dict] | None) -> tuple[int, int]:
    """Le flux donne [{"mechanical": 3}, {"ebike": 8}] : on l'aplatit."""
    counts = {"mechanical": 0, "ebike": 0}
    for entry in types or []:
        for kind, n in entry.items():
            if kind in counts:
                counts[kind] += int(n or 0)
    return counts["mechanical"], counts["ebike"]


def status_messages(payload: dict, polled_at: int) -> list[dict]:
    out = []
    for s in payload["data"]["stations"]:
        mechanical, ebike = bike_types(s.get("num_bikes_available_types"))
        out.append({
            "schema": SCHEMA_VERSION,
            "station_id": str(s["station_id"]),
            "station_code": str(s.get("stationCode", "")),
            "bikes": int(s.get("num_bikes_available", 0)),
            "mechanical": mechanical,
            "ebike": ebike,
            "docks": int(s.get("num_docks_available", 0)),
            "is_installed": bool(s.get("is_installed")),
            "is_renting": bool(s.get("is_renting")),
            "is_returning": bool(s.get("is_returning")),
            "last_reported": int(s.get("last_reported", 0)),
            "polled_at": polled_at,
        })
    return out


def info_messages(payload: dict, polled_at: int) -> list[dict]:
    return [{
        "schema": SCHEMA_VERSION,
        "station_id": str(s["station_id"]),
        "station_code": str(s.get("stationCode", "")),
        "name": s.get("name", ""),
        "lat": float(s["lat"]),
        "lon": float(s["lon"]),
        "capacity": int(s.get("capacity", 0)),
        "polled_at": polled_at,
    } for s in payload["data"]["stations"]]


@dataclass
class ChangeFilter:
    """Ne laisse passer une station que si elle a émis un nouveau relevé.

    Environ deux tiers des stations n'ont pas bougé d'une minute à l'autre :
    les republier gonflerait Kafka et le lac sans rien apprendre de plus.
    """
    seen: dict[str, int] = field(default_factory=dict)

    def __call__(self, messages: list[dict]) -> list[dict]:
        fresh = [m for m in messages if self.seen.get(m["station_id"]) != m["last_reported"]]
        for m in fresh:
            self.seen[m["station_id"]] = m["last_reported"]
        return fresh
