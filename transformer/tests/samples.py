import json

# Real /data/2.5/weather response for Singapore (it has no "rain" block).
SINGAPORE = {
    "coord": {"lon": 103.8503, "lat": 1.29},
    "weather": [
        {
            "id": 803,
            "main": "Clouds",
            "description": "broken clouds",
            "icon": "04n",
        }
    ],
    "base": "stations",
    "main": {
        "temp": 301.87,
        "feels_like": 306.93,
        "temp_min": 301.87,
        "temp_max": 301.87,
        "pressure": 1013,
        "humidity": 79,
        "sea_level": 1013,
        "grnd_level": 1012,
    },
    "visibility": 10000,
    "wind": {"speed": 3.76, "deg": 92, "gust": 4.32},
    "clouds": {"all": 72},
    "dt": 1792590500,
    "sys": {"country": "SG", "sunrise": 1792536403, "sunset": 1792579893},
    "timezone": 28800,
    "id": 1880252,
    "name": "Singapore",
    "cod": 200,
}


def raw_line(row_id, data, run_at="20261021 13:59:30"):
    """One line of an extractor raw file."""
    return json.dumps(
        {
            "id": row_id,
            "run_at": run_at,
            "created_at": run_at,
            "updated_at": run_at,
            "data": data,
        }
    )
