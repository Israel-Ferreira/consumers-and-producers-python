from confluent_kafka.serialization import SerializationContext

class Produto:
    def __init__(self, sku, modelo):
        self.modelo = modelo
        self.sku = sku


    def show_model_and_string(self) -> str:
        return "{} - {}".format(self.sku, self.modelo)



def dict_to_product(obj, ctx):
    return Produto(modelo=obj["modelo"],sku=obj["sku"])


def product_to_dict(product: Produto, ctx: SerializationContext):
    return dict(modelo=product.modelo, sku=product.sku)

