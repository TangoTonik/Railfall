"""A small SQLite warehouse with clean `stations` and `weather_monthly` raw tables.

Shared by the checkpoint and canary tests (SQLite has no schemas: tables are
unqualified and the code under test runs with `schema=None`).
"""

from __future__ import annotations

import sqlite3

STATIONS = [
    ("87686006", "Paris Gare de Lyon", "PARIS GARE DE LYON", "75", 48.844, 2.374),
    ("87722025", "Lyon Part Dieu", "LYON PART DIEU", "69", 45.760, 4.859),
    ("87751008", "Marseille St Charles", "MARSEILLE ST CHARLES", "13", 43.302, 5.380),
    ("87756056", "Nice Ville", "NICE VILLE", "06", 43.704, 7.262),
]
WEATHER_MONTHLY = [
    # dept, month, year, stations, precip, rain, heavy, tmean, tmin, frost, wmean, wmax, strong
    ("75", "2024-01-01", 2024, 3, 61.2, 12.0, 1.0, 5.1, -3.2, 4.0, 4.1, 17.0, 2.0),
    ("69", "2024-01-01", 2024, 5, 48.0, 9.0, 0.0, 3.9, -6.0, 9.0, 3.2, 21.0, 3.0),
    ("2A", "2024-01-01", 2024, 2, 92.5, 11.0, 3.0, 9.4, 0.5, 0.0, 5.0, 25.0, 6.0),
]


def sqlite_db(tmp_path, stations=STATIONS, weather=WEATHER_MONTHLY) -> str:
    path = tmp_path / "warehouse.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "create table stations (uic_code text, station_name text, name_key text,"
            " department text, latitude real, longitude real)"
        )
        conn.executemany("insert into stations values (?,?,?,?,?,?)", stations)
        conn.execute(
            "create table weather_monthly (department text, month text, year integer,"
            " stations integer, precipitation_mm real, rain_days real, heavy_rain_days real,"
            " temp_mean_c real, temp_min_c real, frost_days real, wind_mean_ms real,"
            " wind_max_ms real, strong_wind_days real)"
        )
        conn.executemany(f"insert into weather_monthly values ({','.join('?' * 13)})", weather)
    return f"sqlite:///{path}"
