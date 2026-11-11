"""Consumidor: se suscribe al topic y grafica la telemetría en vivo.

Acepta mensajes JSON y de 3 bytes (detecta el formato por la longitud).

Uso:
  python consumer.py              # lee desde el inicio del topic (histórico + nuevos)
  python consumer.py --latest     # solo mensajes nuevos
  python consumer.py --save grafica.png   # además guarda la gráfica en cada actualización
"""
import argparse
import time

import matplotlib.pyplot as plt
from confluent_kafka import Consumer, KafkaError

from common import KAFKA_CONF, TOPIC, DIRECCIONES, parse_mensaje


def crear_figura():
    plt.ion()
    fig, (ax_t, ax_h, ax_w) = plt.subplots(3, 1, figsize=(11, 8), sharex=True)
    fig.suptitle(f"Estación meteorológica IoT — topic {TOPIC}")
    return fig, ax_t, ax_h, ax_w


def graficar(fig, ax_t, ax_h, ax_w, temps, hums, winds):
    x = list(range(1, len(temps) + 1))
    for ax in (ax_t, ax_h, ax_w):
        ax.clear()
        ax.grid(alpha=0.3)

    ax_t.plot(x, temps, marker="o", color="tab:red")
    ax_t.set_ylabel("Temperatura (°C)")
    ax_t.set_ylim(0, 110)

    ax_h.plot(x, hums, marker="o", color="tab:blue")
    ax_h.set_ylabel("Humedad (%)")
    ax_h.set_ylim(0, 100)

    ax_w.scatter(x, [DIRECCIONES.index(w) for w in winds], color="tab:green")
    ax_w.set_yticks(range(len(DIRECCIONES)))
    ax_w.set_yticklabels(DIRECCIONES)
    ax_w.set_ylabel("Dir. viento")
    ax_w.set_xlabel("Lectura #")

    if temps:
        ax_t.set_title(
            f"Última: {temps[-1]:.2f} °C | {hums[-1]} % | {winds[-1]}   (n={len(temps)})",
            fontsize=10,
        )
    fig.tight_layout()
    fig.canvas.draw_idle()
    plt.pause(0.01)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--latest", action="store_true", help="solo mensajes nuevos")
    ap.add_argument("--save", help="ruta PNG donde guardar la gráfica en cada actualización")
    args = ap.parse_args()

    conf = dict(KAFKA_CONF)
    conf.update({
        # group id único por ejecución para poder releer el histórico cada vez
        "group.id": f"{TOPIC}-consumer-{int(time.time())}",
        "auto.offset.reset": "latest" if args.latest else "earliest",
        "enable.auto.commit": False,
    })
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    print(f"Consumidor suscrito a {TOPIC}. Ctrl+C o cerrar la ventana para salir.")

    temps, hums, winds = [], [], []
    fig, ax_t, ax_h, ax_w = crear_figura()
    graficar(fig, ax_t, ax_h, ax_w, temps, hums, winds)

    try:
        while plt.fignum_exists(fig.number):
            msg = consumer.poll(0.5)
            if msg is None:
                plt.pause(0.1)
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[ERROR] {msg.error()}")
                continue
            try:
                payload = parse_mensaje(msg.value())
            except Exception as e:
                print(f"Mensaje inválido ({msg.value()!r}): {e}")
                continue

            print(f"Recibido offset {msg.offset()}: raw={msg.value()!r} -> {payload}")
            temps.append(payload["temperatura"])
            hums.append(payload["humedad"])
            winds.append(payload["direccion_viento"])
            graficar(fig, ax_t, ax_h, ax_w, temps, hums, winds)
            if args.save:
                fig.savefig(args.save, dpi=150)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Consumidor cerrado.")


if __name__ == "__main__":
    main()
