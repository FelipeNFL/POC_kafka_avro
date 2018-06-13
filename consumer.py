import json
import requests
import logging
from io import BytesIO
from kafka import KafkaConsumer
from avro.schema import Parse
from avro.datafile import DataFileReader
from avro.io import DatumReader

MY_TOPIC = 'text_to_process'

def process_v1(value):
    word = value['word']
    print('Palavra: {}'.format(word))
    print('Essa palavra tem {} caracter(es)'.format(len(word)))

def process_v2(value):
    phrase = value['phrase']
    print('Frase: {}'.format(phrase))
    print('Essa frase tem {} caracter(es)'.format(len(phrase)))
    print('Essa frase tem {} palavra(s)'.format(len(phrase.split())))

def desserialize_avro_message(message):

    with DataFileReader(message, DatumReader()) as reader:

        if not is_schema_valid(reader):
            logging.warn('A message was received with wrong schema')
            return None

        for message in reader:
            yield message

def get_schemas():

    res = requests.get('http://schema_registry:5000/schema/{}'.format(MY_TOPIC))
    schemas = {}

    for version, schema in res.json().items():

        schema_avro = Parse(json.dumps(schema))
        schemas[version] = schema_avro

    return schemas

def is_schema_valid(reader):
    
    schema_text = reader.meta['avro.schema'].decode()
    schema = json.loads(schema_text)

    return schema['name'] == MY_TOPIC

schemas = get_schemas()
processors = {'v1': process_v1, 'v2': process_v2}
consumer = KafkaConsumer('text_to_process', bootstrap_servers=['kafka:9092'])

for message_block in consumer:

    message_buf = BytesIO(message_block.value)

    for message in desserialize_avro_message(message_buf):
            
        print()
        version = message['version']
        processors[version](message)
