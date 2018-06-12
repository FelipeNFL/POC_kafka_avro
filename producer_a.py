import io
import requests
import avro
from kafka import KafkaProducer
import avro.schema

res = requests.get('http://schema_registry:5000/schema/text_to_word/v1')
schema_json = res.json()
schema_avro = avro.schema.Parse(schema_json)

producer = KafkaProducer(bootstrap_servers=['kafka:9092'])


def get_binaries(message):

    buf = io.BytesIO()

    writer = avro.datafile.DataFileWriter(buf, avro.io.DatumWriter(), schema_avro)
    writer.append(message)
    writer.flush()
    buf.seek(0)
    return buf.read()


while True:

    text = input('Digite seu texto:')
    message = get_binaries({"version": "v1", "text": text})
    producer.send('text_to_process', message)


