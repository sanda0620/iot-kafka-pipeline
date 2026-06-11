import json
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timezone


SENSOR_IDS = [f"sensor_{i:03d}" for i in range(1, 6)]
LOCATIONS   = ["warehouse_a", "warehouse_b", "rooftop", "server_room", "lobby"]


@dataclass
class SensorReading:
    sensor_id:   str
    location:    str
    temperature: float
    humidity:    float
    pressure:    float
    air_quality: int
    timestamp:   str

    @classmethod
    def generate(cls) -> "SensorReading":
        return cls(
            sensor_id   = random.choice(SENSOR_IDS),
            location    = random.choice(LOCATIONS),
            temperature = round(random.uniform(18.0, 45.0), 2),
            humidity    = round(random.uniform(30.0, 90.0), 2),
            pressure    = round(random.uniform(980.0, 1050.0), 2),
            air_quality = random.randint(0, 500),
            timestamp   = datetime.now(timezone.utc).isoformat(),
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self))