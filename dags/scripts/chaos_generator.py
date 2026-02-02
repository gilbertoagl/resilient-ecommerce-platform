import requests
import json
import random
import uuid
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

# Conexión interna
DB_CONNECTION = "postgresql://airflow:airflow@postgres/airflow"

def get_base_data():
    """Trae datos reales de la API para usarlos de base"""
    try:
        url = "https://fakestoreapi.com/products"
        response = requests.get(url)
        return response.json()
    except Exception as e:
        print(f"Error conectando a la API: {e}")
        return []

def generate_event(base_products):
    """Genera un evento aleatorio: Created, Updated o Error"""
    if not base_products:
        return None

    event_type = random.choice(['product_created', 'price_updated', 'inventory_error'])
    product = random.choice(base_products)
    
    event_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat()
    
    payload = {}

    if event_type == 'product_created':
        payload = product 
        
    elif event_type == 'price_updated':
        # Simulamos cambio de precio
        new_price = round(product['price'] * random.uniform(0.8, 1.2), 2)
        payload = {
            "id": product['id'],
            "old_price": product['price'],
            "new_price": new_price
        }
        
    elif event_type == 'inventory_error':
        # ERROR INTENCIONL: Datos sucios
        payload = {
            "id": product['id'],
            "price": -50.00,  # Negativo
            "category": None  # Nulo
        }

    event = {
        "event_id": event_id,
        "event_type": event_type,
        "event_timestamp": timestamp,
        "source": "fakestore_api_simulator",
        "payload": json.dumps(payload)
    }
    return event

def save_to_postgres(event):
    if not event: return
    
    try:
        engine = create_engine(DB_CONNECTION)
        df = pd.DataFrame([event])
        df.to_sql('raw_events', engine, if_exists='append', index=False)
        print(f"Evento guardado: {event['event_type']}")
    except Exception as e:
        print(f"Error guardando en BD: {e}")

if __name__ == "__main__":
    print("Iniciando generador de caos...")
    products = get_base_data()
    # Generamos 10 eventos
    for _ in range(10):
        ev = generate_event(products)
        save_to_postgres(ev)
    print("Finalizado.")