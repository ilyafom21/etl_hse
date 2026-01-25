from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import xml.etree.ElementTree as ET

from airflow.decorators import dag, task

JSON_URL = "https://raw.githubusercontent.com/LearnWebCode/json-example/master/pets-data.json"
XML_URL = "https://gist.githubusercontent.com/pamelafox/3000322/raw/6cc03bccf04ede0e16564926956675794efe5191/nutrition.xml"

OUT_DIR = Path("/opt/airflow/output")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({k for r in rows for k in r.keys()}) if rows else ["empty"]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for r in rows:
            cleaned = {k: ("" if r.get(k) is None else r.get(k)) for k in fieldnames}
            w.writerow(cleaned)
    return str(path)


def _flatten_json(obj: Any, prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}_{k}" if prefix else str(k)
            out.update(_flatten_json(v, key))
        return out
    if isinstance(obj, list):
        out[prefix] = " | ".join(str(x) for x in obj)
        out[prefix + "_json"] = json.dumps(obj, ensure_ascii=False)
        return out
    out[prefix] = obj
    return out


def _json_to_rows(data: Any) -> List[Dict[str, Any]]:
    records: Optional[List[Any]] = None
    if isinstance(data, dict):
        if isinstance(data.get("pets"), list):
            records = data["pets"]
        else:
            for k in ("items", "results", "data", "records", "rows"):
                if isinstance(data.get(k), list):
                    records = data[k]
                    break
        if records is None:
            records = [data]
    elif isinstance(data, list):
        records = data
    else:
        records = [{"value": data}]
    return [_flatten_json(r) for r in records]


def _get_text(parent: ET.Element, tag: str) -> str:
    el = parent.find(tag)
    return (el.text or "").strip() if el is not None and el.text else ""


def _xml_to_rows(xml_text: str) -> List[Dict[str, Any]]:
    root = ET.fromstring(xml_text)

    daily = root.find("daily-values")
    daily_vals: Dict[str, Any] = {}
    if daily is not None:
        for child in list(daily):
            daily_vals[f"daily_{child.tag}"] = (child.text or "").strip() if child.text else ""
            if child.attrib.get("units"):
                daily_vals[f"daily_{child.tag}_units"] = child.attrib["units"]

    rows: List[Dict[str, Any]] = []
    for food in root.findall("food"):
        row: Dict[str, Any] = {}
        row["name"] = _get_text(food, "name")
        row["mfr"] = _get_text(food, "mfr")

        serving = food.find("serving")
        if serving is not None:
            row["serving"] = (serving.text or "").strip() if serving.text else ""
            row["serving_units"] = serving.attrib.get("units", "")

        calories = food.find("calories")
        if calories is not None:
            row["calories_total"] = calories.attrib.get("total", "")
            row["calories_fat"] = calories.attrib.get("fat", "")

        for t in ["total-fat", "saturated-fat", "cholesterol", "sodium", "carb", "fiber", "protein"]:
            row[t] = _get_text(food, t)

        vitamins = food.find("vitamins")
        if vitamins is not None:
            row["vitamins_a"] = _get_text(vitamins, "a")
            row["vitamins_c"] = _get_text(vitamins, "c")

        minerals = food.find("minerals")
        if minerals is not None:
            row["minerals_ca"] = _get_text(minerals, "ca")
            row["minerals_fe"] = _get_text(minerals, "fe")

        row.update(daily_vals)
        rows.append(row)

    return rows if rows else [{"empty": ""}]


@dag(
    dag_id="hw2_etl",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
    tags=["hw", "etl", "json", "xml"],
)
def hw2_etl():
    @task
    def extract_json() -> Any:
        r = requests.get(JSON_URL, timeout=60)
        r.raise_for_status()
        return r.json()

    @task
    def transform_json(data: Any) -> List[Dict[str, Any]]:
        return _json_to_rows(data)

    @task
    def load_json(rows: List[Dict[str, Any]]) -> str:
        return _write_csv(OUT_DIR / "json_flat.csv", rows)

    @task
    def extract_xml() -> str:
        r = requests.get(XML_URL, timeout=60)
        r.raise_for_status()
        return r.text

    @task
    def transform_xml(xml_text: str) -> List[Dict[str, Any]]:
        return _xml_to_rows(xml_text)

    @task
    def load_xml(rows: List[Dict[str, Any]]) -> str:
        return _write_csv(OUT_DIR / "xml_flat.csv", rows)

    load_json(transform_json(extract_json()))
    load_xml(transform_xml(extract_xml()))


hw2_etl()
