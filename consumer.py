from confluent_kafka import Consumer

from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

from models.produto import Produto, dict_to_product


# Type Hints em Python
def consume_messages(consumer, topic):
    try:
        consumer.subscribe(topic)

        while True:
            msg = consumer.poll(timeout=1.0)

            # Verificando se a mensagem está nula 
            if msg is None: continue

            produto =  msg.Value()

            print(f"{produto.modelo} {produto.sku}")

            if msg.error():
                print("Topico: {}. Partição: {}".format(msg.topic(), msg.partition()))


    finally:
        consumer.close()
        print("Finish")




if __name__ == "__main__":

    with open("schema.json") as schema_file:
        json_schema =  schema_file.read()


    string_deserializer = StringDeserializer("utf-8")
    json_deserializer =  JSONDeserializer(schema_str=json_schema, from_dict=dict_to_product)


    # Mapas em Python
    conf =  {
        "bootstrap.servers": "localhost:9092",
        "group.id": "produtos_consumer_group",
        "auto.offset.reset": "latest",
        "key.deserializer": string_deserializer,
        "value.deserializer": json_deserializer
    }

    cons = Consumer(conf)

    consume_messages(cons, "produtos")
