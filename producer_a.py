import io
import requests
from avro.datafile import DataFileWriter
from kafka import KafkaProducer
import avro.schema

TOPIC = 'text_to_process'
VERSION = 'v1'

res = requests.get('http://schema_registry:5000/schema/{}/{}'.format(TOPIC, VERSION))
schema_json = res.text
schema_avro = avro.schema.Parse(schema_json)

producer = KafkaProducer(bootstrap_servers=['kafka:9092'])


def get_binaries(message):

    buf = io.BytesIO()

    writer = DataFileWriter(buf, avro.io.DatumWriter(), schema_avro)
    writer.append(message)
    writer.flush()
    buf.seek(0)
    return buf.read()


while True:

    text = input('Digite seu texto: ')
    message = get_binaries({"version": VERSION, "word": text})
    producer.send(TOPIC, message)
