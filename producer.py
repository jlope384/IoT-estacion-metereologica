"""Productor (dispositivo IoT): envía telemetría cada 15-30 s hasta ser interrumpido.

Uso:
  python producer.py                 # JSON (secciones 3.1-3.3)
  python producer.py --compact       # payload de 3 bytes (sección 3.4)
  python producer.py --min 2 --max 4 # intervalo corto para probar
"""
import argparse
import random
import time

from confluent_kafka import Producer

from common import KAFKA_CONF, TOPIC, KEY, generar_data, to_json, encode


def reporte(err, msg):
    if err:
        print(f"[ERROR] {err}")
    else:
        print(f"  -> entregado a {msg.topic()} [partición {msg.partition()}] offset {msg.offset()}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--compact", action="store_true", help="usar payload de 3 bytes")
    ap.add_argument("--min", type=float, default=15, help="segundos mínimos entre envíos")
    ap.add_argument("--max", type=float, default=30, help="segundos máximos entre envíos")
    args = ap.parse_args()

    producer = Producer(KAFKA_CONF)
    modo = "compacto (3 bytes)" if args.compact else "JSON"
    print(f"Productor iniciado | topic={TOPIC} | modo={modo} | Ctrl+C para detener")

    try:
        while True:
            data = generar_data()
            payload = encode(data) if args.compact else to_json(data)
            producer.produce(TOPIC, key=KEY, value=payload, callback=reporte)
            producer.poll(0)
            print(f"Enviado: {data} | payload={payload!r} ({len(payload)} bytes)")
            producer.flush(10)
            time.sleep(random.uniform(args.min, args.max))
    except KeyboardInterrupt:
        print("\nDeteniendo productor...")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
