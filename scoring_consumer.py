from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def score_transaction(tx):
    score = 0
    rules = []
    if tx['amount'] > 3000:
        score += 3
        rules.append('R1')
    if tx.get('category') == 'elektronika' and tx['amount'] > 1500:
        score += 2
        rules.append('R2')
    hour = tx.get('hour', datetime.fromisoformat(tx['timestamp']).hour)
    if hour < 6:
        score += 2
        rules.append('R3')
    return score, rules

for msg in consumer:
    tx = msg.value
    score, rules = score_transaction(tx)
    if score >= 3:
        tx['score'] = score
        tx['rules'] = rules
        alert_producer.send('alerts', value=tx)
        print(f"ALERT | {tx['tx_id']} | {tx['amount']:.2f} PLN | score={score} | reguły={rules}")
