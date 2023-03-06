from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from models.produto import Produto, product_to_dict



if __name__ == "__main__":

    with open("schema.json") as schema:
        schema_def =  schema.read()

    schema_registry_cnfg = {"url": "http://localhost:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_cnfg)

    string_serializer = StringSerializer()
    json_schema_serializer = JSONSerializer(schema_str=schema_def, schema_registry_client=schema_registry_client, to_dict=product_to_dict)

    cnf  = {
        "bootstrap.servers": "localhost:9092"
    }


    producer = Producer(cnf)

    produtos =  [
        Produto("9909009SD", "Aspirador de Pó Inteligente ROMBA 650"),
        Produto("902343333", "Café Terra de Sabores - 100% Organico"),
        Produto("90923332S", "Café Pilão Extra Forte Arábica"),
        Produto("90234556N", "Cocktop Consul 5 Bocas Aço Inox")
    ]


    for produto in produtos:
        try:
            topic = "produtos"
            producer.produce(topic=topic, key=string_serializer(str(uuid4())), value=json_schema_serializer(produto, SerializationContext(topic, MessageField.VALUE)))
        except Exception as ex:
            print(ex)


    producer.flush()
