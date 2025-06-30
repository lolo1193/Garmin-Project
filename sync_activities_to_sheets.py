#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script : sync_activities_to_sheets.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Synchronise automatiquement les fichiers JSON Garmin avec une feuille Google Sheets
(« Global activity details »). Pour chaque activité, la paire
activity_<ID>.json + activity_details_<ID>.json est fusionnée et importée.
Ajout de logs de debug, vérification d'existence de répertoire, et pause finale.
"""

import os
import glob
import json
from datetime import datetime
from typing import List, Any, Dict

import gspread

# ===========================
# Configuration à adapter
# ===========================
CREDENTIALS_FILE = "credentials.json"
SPREADSHEET_ID   = "1tf1UNvptLpjt8gBq-9g9NsabxGWMFmQGoapLO3ryHEg"
WORKSHEET_NAME   = "Global activity details"
# Chemin corrigé : utiliser slash ou raw string avec une seule paire de backslashes
ACTIVITIES_DIR   = "C:/Users/loys_/HealthData/FitFiles/Activities"
CHUNK_SIZE       = 100
MAX_CELL_LENGTH  = 50000  # sécurité pour Google Sheets

HEADERS: List[str] = [
    "activityId", "activityName", "activityTypeId", "parentActivityTypeId",
    "eventTypeId", "manualActivity", "favorite", "personalRecord",
    "startTimeLocal", "startTimeGMT", "activityTimestampMs", "duration",
    "movingDuration", "elapsedDuration", "timezoneOffset",
    "distance", "averageSpeed", "maxSpeed", "averageStrideLength",
    "averagePaceMinPerKm", "totalElevationGain", "totalElevationLoss",
    "minElevation", "maxElevation", "startLatitude", "startLongitude",
    "endLatitude", "endLongitude", "averageHeartRate", "maxHeartRate",
    "averageCadence", "maxCadence", "totalSteps", "vo2MaxValue",
    "activeKilocalories", "bmrKilocalories", "bodyBatteryDelta",
    "hrZone1Seconds", "hrZone2Seconds", "hrZone3Seconds", "hrZone4Seconds",
    "hrZone5Seconds", "moderateIntensityMinutes", "vigorousIntensityMinutes",
    "hydrationConsumedMl", "splitsJSON"
]

# ---------------------------------------------------------------------------
# Calculs de champs dérivés
# ---------------------------------------------------------------------------
def compute_stride_length(distance_m: float, total_steps: int) -> Any:
    return round(distance_m / total_steps, 3) if total_steps else ""

def compute_pace_min_per_km(avg_speed_m_s: float) -> Any:
    if avg_speed_m_s:
        pace_sec = 1000 / avg_speed_m_s
        return round(pace_sec / 60, 2)
    return ""

# ---------------------------------------------------------------------------
# Fusion des deux JSON pour extraire tous les champs
# ---------------------------------------------------------------------------
def merge_data(details: dict, flat: dict) -> dict:
    start_local = details['summaryDTO']['startTimeLocal']
    start_gmt   = details['summaryDTO']['startTimeGMT']
    tz_offset_h = (
        datetime.fromisoformat(start_local)
        - datetime.fromisoformat(start_gmt)
    ).total_seconds() / 3600

    return {
        "activityId":           details["activityId"],
        "activityName":         details["activityName"],
        "activityTypeId":       details["activityTypeDTO"]["typeId"],
        "parentActivityTypeId": details["activityTypeDTO"]["parentTypeId"],
        "eventTypeId":          details["eventTypeDTO"]["typeId"],
        "manualActivity":       details["metadataDTO"]["manualActivity"],
        "favorite":             details["metadataDTO"]["favorite"],
        "personalRecord":       details["metadataDTO"]["personalRecord"],
        "startTimeLocal":       start_local,
        "startTimeGMT":         start_gmt,
        "activityTimestampMs":  flat.get("beginTimestamp"),
        "timezoneOffset":       tz_offset_h,
        "duration":             details["summaryDTO"]["duration"],
        "movingDuration":       details["summaryDTO"]["movingDuration"],
        "elapsedDuration":      details["summaryDTO"]["elapsedDuration"],
        "distance":             details["summaryDTO"]["distance"],
        "averageSpeed":         details["summaryDTO"]["averageSpeed"],
        "maxSpeed":             details["summaryDTO"]["maxSpeed"],
        "totalSteps":           details["summaryDTO"]["steps"],
        "totalElevationGain":   details["summaryDTO"]["elevationGain"],
        "totalElevationLoss":   details["summaryDTO"]["elevationLoss"],
        "minElevation":         details["summaryDTO"]["minElevation"],
        "maxElevation":         details["summaryDTO"]["maxElevation"],
        "startLatitude":        details["summaryDTO"]["startLatitude"],
        "startLongitude":       details["summaryDTO"]["startLongitude"],
        "endLatitude":          details["summaryDTO"]["endLatitude"],
        "endLongitude":         details["summaryDTO"]["endLongitude"],
        "averageHeartRate":     details["summaryDTO"]["averageHR"],
        "maxHeartRate":         details["summaryDTO"]["maxHR"],
        "averageCadence":       details["summaryDTO"]["averageRunCadence"],
        "maxCadence":           details["summaryDTO"]["maxRunCadence"],
        "vo2MaxValue":          flat.get("vO2MaxValue"),
        "activeKilocalories":   details["summaryDTO"]["calories"],
        "bmrKilocalories":      details["summaryDTO"]["bmrCalories"],
        "bodyBatteryDelta":     details["summaryDTO"]["differenceBodyBattery"],
        "hrZone1Seconds":       flat.get("hrTimeInZone_1"),
        "hrZone2Seconds":       flat.get("hrTimeInZone_2"),
        "hrZone3Seconds":       flat.get("hrTimeInZone_3"),
        "hrZone4Seconds":       flat.get("hrTimeInZone_4"),
        "hrZone5Seconds":       flat.get("hrTimeInZone_5"),
        "moderateIntensityMinutes": details["summaryDTO"]["moderateIntensityMinutes"],
        "vigorousIntensityMinutes": details["summaryDTO"]["vigorousIntensityMinutes"],
        "hydrationConsumedMl":      details["summaryDTO"]["waterEstimated"],
        "splitsJSON":               details.get("splitSummaries", [])
    }

# ---------------------------------------------------------------------------
# Construction de la ligne à injecter dans Sheets
# ---------------------------------------------------------------------------
def build_row(data: Dict[str, Any]) -> List[Any]:
    """Prépare une ligne en appliquant les conversions nécessaires."""
    stride = compute_stride_length(data.get("distance", 0), data.get("totalSteps", 0))
    pace = compute_pace_min_per_km(data.get("averageSpeed", 0))

    enriched = {
        **data,
        "averageStrideLength": stride,
        "averagePaceMinPerKm": pace,
    }

    row: List[Any] = []
    for col in HEADERS:
        val = enriched.get(col, "")
        if isinstance(val, (dict, list)):
            val = json.dumps(val, ensure_ascii=False)[:MAX_CELL_LENGTH]
        row.append(val)
    return row

# ---------------------------------------------------------------------------
# Google Sheets : chargement et gestion des en-têtes
# ---------------------------------------------------------------------------
def load_worksheet():
    gc = gspread.service_account(filename=CREDENTIALS_FILE)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows="2000", cols=str(len(HEADERS)))
    if not ws.row_values(1):
        ws.append_row(HEADERS, value_input_option="USER_ENTERED")
    return ws


def get_existing_ids(ws) -> set:
    return set(ws.col_values(1)[1:])

# ---------------------------------------------------------------------------
# Exécution principale
# ---------------------------------------------------------------------------
def main() -> None:
    try:
        ws = load_worksheet()
        known_ids = get_existing_ids(ws)
        print(f"✅ {len(known_ids)} activités déjà présentes dans la feuille.")

        # Debug: existence et contenu du répertoire
        print("DEBUG ▶ ACTIVITIES_DIR exists:", os.path.isdir(ACTIVITIES_DIR))
        flat_files   = glob.glob(os.path.join(ACTIVITIES_DIR, "activity_*.json"))
        detail_files = glob.glob(os.path.join(ACTIVITIES_DIR, "activity_details_*.json"))
        print("DEBUG ▶ flat files   =", flat_files)
        print("DEBUG ▶ detail files =", detail_files)

        # IDs communs
        get_id     = lambda p: os.path.splitext(os.path.basename(p))[0].rsplit("_", 1)[1]
        common_ids = sorted({get_id(f) for f in flat_files} & {get_id(f) for f in detail_files})
        print("DEBUG ▶ common activity IDs =", common_ids)

        if not common_ids:
            print("⚠️ Aucun fichier JSON pair trouvé. Vérifiez ACTIVITIES_DIR et le nommage.")
            return

        new_rows, added = [], 0
        for aid in common_ids:
            if aid in known_ids:
                continue
            print(f"Traitement de l'activité {aid}...")
            try:
                with open(os.path.join(ACTIVITIES_DIR, f"activity_{aid}.json"), 'r', encoding='utf-8') as f:
                    flat    = json.load(f)
                with open(os.path.join(ACTIVITIES_DIR, f"activity_details_{aid}.json"), 'r', encoding='utf-8') as f:
                    details = json.load(f)
                merged = merge_data(details, flat)
                new_rows.append(build_row(merged))
                added += 1
            except Exception as e:
                print(f"⚠️ Erreur activité {aid} : {e}")

            if len(new_rows) >= CHUNK_SIZE:
                ws.append_rows(new_rows, value_input_option="USER_ENTERED")
                print(f"➕ Ajout de {len(new_rows)} activités…")
                new_rows = []

        if new_rows:
            ws.append_rows(new_rows, value_input_option="USER_ENTERED")
            print(f"➕ Ajout final de {len(new_rows)} activités…")

        print(f"🎉 Terminé : {added} nouvelles activités ajoutées.")
    except Exception as ex:
        print(f"💥 Erreur inattendue : {ex}")
    finally:
        input("Appuyez sur Entrée pour quitter...")

if __name__ == "__main__":
    main()
