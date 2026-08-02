from kafka import KafkaConsumer
from minio import Minio
import io
import os
from datetime import datetime
import json

# Kafka
consumer = KafkaConsumer(
    "btc",
    bootstrap_servers=["kafka:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="minio"
)

# MinIO client (ajuste o host conforme sua rede)
MINIO_ROOT_USER = os.getenv('MINIO_ROOT_USER')
MINIO_ROOT_PASSWORD = os.getenv('MINIO_ROOT_PASSWORD')
minio_client = Minio(
    "minio:9000",   # se estiver na mesma rede docker
    # "localhost:9000", # se acessar pela máquina host
    access_key="MINIO_ROOT_USER",
    secret_key="MINIO_ROOT_PASSWORD",
    secure=False
)

bucket = "btc"
if not minio_client.bucket_exists(bucket):
    minio_client.make_bucket(bucket)

# Loop de consumo
for msg in consumer:
    try:
        # Converte a mensagem para JSON
        conteudo = json.loads(msg.value.decode("utf-8"))

        # Usa o campo "height" como nome do arquivo
        height = conteudo.get("height")
        if height is None:
            print("Mensagem recebida sem campo 'height'")
            continue

        filename = f"{height}.json"
        data_bytes = json.dumps(conteudo, indent=2).encode("utf-8")

        # Salva no MinIO
        minio_client.put_object(
            bucket,
            filename,
            data=io.BytesIO(data_bytes),
            length=len(data_bytes),
            content_type="application/json"
        )
        print(f"Mensagem do bloco {height} salva no MinIO como {filename}")

    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")
