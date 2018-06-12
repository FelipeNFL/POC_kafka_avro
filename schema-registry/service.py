import json
from flask import Flask, jsonify

app = Flask(__name__)

with open('schema-registry/text_to_process.v1.json') as fp:
    text_to_process_v1 = json.loads(fp.read())

with open('schema-registry/text_to_process.v2.json') as fp:
    text_to_process_v2 = json.loads(fp.read())

@app.route("/schema/text_to_process/v1")
def schema_v1():
    return jsonify(text_to_process_v1)

@app.route("/schema/text_to_process/v2")
def schema_v2():
    return jsonify(text_to_process_v2)

if __name__ == '__main__':
    app.run()