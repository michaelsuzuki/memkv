

from memkv.client.api import Client


client = Client("127.0.0.1", 9001)
res0 = client.set({"keyOne": b"valueOne", "keyTwo": b"valueTwo"})
res1 = client.get(["keyOne"])
print("GOT STUFF")
res3 = client.metrics()
print(res3)
client.close()

