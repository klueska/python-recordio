import json
import recordio

def MessageToJson(message):
    return json.dumps(message)
    

def Parse(string):
    return json.loads(string)


encoder = recordio.Encoder(MessageToJson)
decoder = recordio.Decoder(Parse)


message = {
    "type" : "ATTACH_CONTAINER_OUTPUT",
    "containerId" : "123456789"
}

encoded = encoder.encode(message)
encoded += encoder.encode(message)
encoded += encoder.encode(message)
encoded += encoder.encode(message)
encoded += encoder.encode(message)

offset = 0
chunkSize = 5
while offset < len(encoded):
    records = decoder.decode(encoded[offset:offset + chunkSize])
    if records:
        for r in records:
            print(r)
    offset += chunkSize
