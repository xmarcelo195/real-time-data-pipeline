from bitcoinrpc.authproxy import AuthServiceProxy
from kafka import KafkaProducer
from decimal import Decimal
import json
import time
import os

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

# Get credentials from environment variables
rpc_user = os.getenv('RPC_USER')
rpc_password = os.getenv('RPC_PASSWORD')
rpc_host = os.getenv('RPC_HOST', 'host.docker.internal')
rpc_port = os.getenv('RPC_PORT', '8332')

# Kafka configuration
kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:9092')

rpc_url = f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}"
rpc = AuthServiceProxy(rpc_url)

producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    value_serializer=lambda v: json.dumps(v, cls=CustomJSONEncoder).encode('utf-8'),
)

def get_latest_block_height():
    return rpc.getblockcount()

def ingest_new_blocks():
    last_height = 0
    while True:
        current_height = get_latest_block_height()
        print(current_height)
        if current_height > last_height:
            for height in range(last_height + 1, current_height + 1):
                block_hash = rpc.getblockhash(height)
                block = rpc.getblock(block_hash, 2)
                producer.send("btc", block)
                print(f"Enviado bloco {height} para Kafka")
                last_height = height
        time.sleep(3)

if __name__ == "__main__":
    ingest_new_blocks()