import io
import requests
import json
from avro.datafile import DataFileWriter
from kafka import KafkaProducer
import avro.schema

TOPIC = 'text_to_process'
VERSION = 'v1'

schema_text = {
    "namespace": "poc.avro",
    "type": "record",
    "name": "schema_wrong",
    "fields": [
        {
            "name": "version",
            "type": "string"
        },
        {
            "name": "word",
            "type": "string"
        }
    ]
}

schema_avro = avro.schema.Parse(json.dumps(schema_text))

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
