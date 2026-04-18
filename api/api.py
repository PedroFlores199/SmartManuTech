from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import uvicorn
import random

app = FastAPI(
    title='SmartManuTech IoT API',
    description='API REST para el sistema de monitoreo IoT industrial',
    version='2.0.0'
)
session = None
class SensorReading(BaseModel):
    machine_id: str
    timestamp: str
    temperature: float
    vibration: float
    production_speed: float
    energy_kwh: float
    alert_level: str
    failure_probability: Optional[float] = 0.0

class AlertSummary(BaseModel):
    machine_id: str
    alert_level: str
    timestamp: str
    failure_probability: float

@app.get('/api/v1/machines/{machine_id}/latest',
         summary='Ultima lectura de una maquina')
async def get_latest_reading(machine_id: str):
    return {
        'machine_id': machine_id,
        'timestamp': datetime.utcnow().isoformat(),
        'temperature': round(random.gauss(72, 8), 2),
        'vibration': round(random.gauss(6.5, 1.2), 3),
        'production_speed': round(random.gauss(450, 30), 1),
        'energy_kwh': round(random.gauss(12.5, 1.5), 3),
        'alert_level': 'OK',
        'failure_probability': 0.05
    }


@app.get('/api/v1/alerts/active',
         summary='Alertas activas en la ultima hora')
async def get_active_alerts():
    return [
        {
            'machine_id': 'M012',
            'alert_level': 'CRITICAL',
            'timestamp': datetime.utcnow().isoformat(),
            'failure_probability': 0.91
        },
        {
            'machine_id': 'M047',
            'alert_level': 'WARNING',
            'timestamp': datetime.utcnow().isoformat(),
            'failure_probability': 0.73
        },
        {
            'machine_id': 'M063',
            'alert_level': 'WARNING',
            'timestamp': datetime.utcnow().isoformat(),
            'failure_probability': 0.68
        },
    ]

@app.get('/api/v1/machines/risk-report',
         summary='Maquinas con alto riesgo de fallo en 24h')
async def risk_report():
    return [
        {
            'machine_id': 'M012',
            'avg_temp': 87.3,
            'avg_vibration': 13.1,
            'failure_probability': 0.91
        },
        {
            'machine_id': 'M047',
            'avg_temp': 78.9,
            'avg_vibration': 10.4,
            'failure_probability': 0.73
        },
    ]

@app.get('/api/v1/machines/{machine_id}/history',
         summary='Historico de lecturas de una maquina')
async def get_machine_history(machine_id: str, hours: int = 24):
    if hours > 72:
        raise HTTPException(status_code=400, detail='Maximo 72 horas de historico')
    return [
        {
            'machine_id': machine_id,
            'timestamp': datetime.utcnow().isoformat(),
            'temperature': round(random.gauss(72, 8), 2),
            'vibration': round(random.gauss(6.5, 1.2), 3),
            'alert_level': 'OK'
        }
        for _ in range(10)
    ]

@app.get('/health', summary='Estado del servicio')
async def health_check():
    return {'status': 'ok', 'timestamp': datetime.utcnow().isoformat()}

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000, reload=False)