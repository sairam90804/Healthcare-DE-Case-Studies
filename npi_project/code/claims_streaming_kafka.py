
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

claim = {
    "npi_number": "1234567890",
    "provider_type": "General Practitioner",
    "billed_amount": 15000,
    "service_code": "99395"
}

producer.send("claims_stream", value=claim)
