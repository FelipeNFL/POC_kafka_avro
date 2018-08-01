import sys
import json
import pprint
import requests
import logging
from confluent_kafka import avro
from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroProducer, AvroConsumer
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    filemode='w')

topic = 'test_backward'
broker = 'kafka:9092'
schema_registry_url = 'http://schema-registry:8081'
schema_registry = CachedSchemaRegistryClient(schema_registry_url)


def consumer_message():

  print('consumer initialized')

  consumer = AvroConsumer({
      'bootstrap.servers': broker,
      'group.id': 'test_poc'},
      schema_registry=schema_registry
  )

  consumer.subscribe([topic])

  while True:
      msg = consumer.poll(0.5)

      if msg is None:
          continue
      if msg.error():
          if msg.error().code() == KafkaError._PARTITION_EOF:
              continue
          else:
              print(msg.error())
              break

      print('\nReceived message: {}\n'.format(msg.value()))


def print_title(title):
  print('----------------------------------------------------------')
  print(title)
  print('----------------------------------------------------------')


def produce(schema_json, data):

  print('schema:\n')
  pprint.pprint(schema_json)
  print('\n')

  print('message:\n')
  pprint.pprint(data)
  print('\n')

  schema_avro = avro.loads(json.dumps(schema_json))
  producer = AvroProducer({'bootstrap.servers': broker},
                          default_value_schema=schema_avro,
                          schema_registry=schema_registry)

  producer.poll(0)
  producer.produce(topic=topic, value=data)
  producer.flush()


def post_data_v1():

  print_title('Schema v1')

  schema = {
        "namespace": "poc.avro",
        "type": "record",
        "name": "poc.v1",
        "fields": [
          {"name": "indicators", "type": "string"},
          {"name": "notes", "type": "string"}
        ]
  }

  data = {'indicators': 'gross_revenue', 'notes': 'serasa'}

  produce(schema, data)


def post_data_v2():

  print_title('Schema v2 - Adiciona tag portfolio')

  schema = {
        "namespace": "poc.avro",
        "type": "record",
        "name": "poc.v2",
        "fields": [
          {"name": "indicators", "type": "string"},
          {"name": "notes", "type": "string"},
          {"name": "portfolios", "type": "int", "default": 0}
        ]
  }

  data = {'indicators': 'gross_revenue',
          'notes': 'serasa',
          'portfolios': 3}
  
  produce(schema, data)


def post_data_v3_incompatible_forward():

  print_title('Schema v3 - Delete tags notes e portfolios - incompatível forward')

  schema = {
          "namespace": "poc.avro",
          "type": "record",
          "name": "poc.v3",
          "fields": [
            {"name": "indicators", "type": "string"}
          ]
    }

  data = {'indicators': 'gross_revenue'}

  produce(schema, data)


def post_data_v4_incompatible():

  print_title('Schema v4 incompatível - campo novo sem default')

  schema = {
        "namespace": "poc.avro",
        "type": "record",
        "name": "poc.v4",
        "fields": [
          {"name": "indicators", "type": "string"},
          {"name": "notes", "type": "string"},
          {"name": "portfolios", "type": "int", "default": 0},
          {"name": "collaterals", "type": "string"}
        ]
  }

  data = {'indicators': 'gross_revenue', 'notes': 'serasa'}

  try:
    produce(schema, data)
  except Exception as e:
    print(e)

if __name__ == "__main__":

  compatibility = {"compatibility": "BACKWARD"}
  config_endpoint = '{url}/config/{topic}'.format(url=schema_registry_url,
                                                  topic=topic)

  res = requests.put(url=config_endpoint, json=compatibility)

  print(res.json())

  options = [post_data_v1,
             post_data_v2,
             post_data_v3_incompatible_forward,
             post_data_v4_incompatible]

  if len(sys.argv) > 1:
    if sys.argv[1] == 'consumer':
      consumer_message()

  for option in options:

    option()
    input('\nEnter para próximo teste\n')
