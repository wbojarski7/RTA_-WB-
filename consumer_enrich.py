from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='enrich-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for msg in consumer:
    tx = msg.value
    if tx['amount'] > 3000:
        tx['risk_level'] = 'HIGH'
    elif tx['amount'] > 1000:
        tx['risk_level'] = 'MEDIUM'
    else:
        tx['risk_level'] = 'LOW'
    print(f"{tx['risk_level']} | {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']}")
