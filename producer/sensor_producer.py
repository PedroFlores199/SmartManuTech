import json
import random
import time
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['kafka-broker:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    compression_type='lz4',
    batch_size=16384,
    linger_ms=5
)

MACHINES = [f'M{str(i).zfill(3)}' for i in range(1, 81)]


def generate_sensor_data(machine_id: str) -> dict:
    is_anomaly = random.random() < 0.02
    return {
        'machine_id': machine_id,
        'timestamp': datetime.utcnow().isoformat(),
        'temperature': round(random.gauss(72, 8) + (20 if is_anomaly else 0), 2),
        'vibration': round(random.gauss(6.5, 1.2) + (8 if is_anomaly else 0), 3),
        'production_speed': round(random.gauss(450, 30), 1),
        'energy_kwh': round(random.gauss(12.5, 1.5), 3),
        'error_code': random.choice([0, 0, 0, 0, 1, 2]) if is_anomaly else 0
    }


if __name__ == '__main__':
    print(f"Iniciando productor para {len(MACHINES)} maquinas...")
    messages_sent = 0

    while True:
        for machine in MACHINES:
            data = generate_sensor_data(machine)
            producer.send('iot-sensors', value=data, key=machine.encode())
            messages_sent += 1

        producer.flush()

        if messages_sent % 1000 == 0:
            print(f"[{datetime.utcnow().isoformat()}] Mensajes enviados: {messages_sent}")

        time.sleep(0.5)