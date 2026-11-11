"""Configuración compartida, simulación de sensores y codificación de 3 bytes."""
import json
import random

# ---------------- Kafka ----------------
BOOTSTRAP = "161.35.11.141:9092"
TOPIC = "23415"          # carné como topic único
KEY = "sensor1"

KAFKA_CONF = {
    "bootstrap.servers": BOOTSTRAP,
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "estudiante",
    "sasl.password": "TryluRandomPass-2!@!",
}

# ---------------- Sensores ----------------
DIRECCIONES = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]  # 8 opciones -> 3 bits
MEDIA = 55
DESV = 15  # varianza = 225


def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


def generar_data():
    """Muestrea de una normal(55, 15) y recorta a los rangos de cada sensor."""
    temperatura = round(_clamp(random.gauss(MEDIA, DESV), 0, 110), 2)
    humedad = int(round(_clamp(random.gauss(MEDIA, DESV), 0, 100)))
    direccion = random.choice(DIRECCIONES)
    return {"temperatura": temperatura, "humedad": humedad, "direccion_viento": direccion}


# ---------------- JSON ----------------
def to_json(data):
    return json.dumps(data).encode("utf-8")


def from_json(raw):
    return json.loads(raw.decode("utf-8"))


# ---------------- Payload restringido: 3 bytes ----------------
# | temperatura*100 (14 bits) | humedad (7 bits) | viento (3 bits) |  = 24 bits
# 110.00 * 100 = 11000 < 2^14 = 16384, así que cabe como entero.

def encode(data):
    temp = int(round(data["temperatura"] * 100))
    hum = int(data["humedad"])
    viento = DIRECCIONES.index(data["direccion_viento"])
    assert 0 <= temp < 2**14 and 0 <= hum < 2**7 and 0 <= viento < 2**3
    packed = (temp << 10) | (hum << 3) | viento
    return packed.to_bytes(3, "big")


def decode(raw):
    packed = int.from_bytes(raw, "big")
    return {
        "temperatura": ((packed >> 10) & 0x3FFF) / 100,
        "humedad": (packed >> 3) & 0x7F,
        "direccion_viento": DIRECCIONES[packed & 0x7],
    }


def parse_mensaje(raw):
    """Detecta el formato: 3 bytes -> decode compacto, si no -> JSON."""
    return decode(raw) if len(raw) == 3 else from_json(raw)
