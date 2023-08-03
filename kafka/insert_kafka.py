import json


from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["192.168.74.128:6667"],
    security_protocol="SSL",
    api_version=(1, 0, 0),
    value_serializer=lambda v: json.dumps(v).encode('ascii'),
    key_serializer=lambda v: json.dumps(v).encode('ascii')
)

producer.send("ftp-download",
                key={"county": "Alameda-CA"},
                value={"host": "192.168.74.130",
                       "username" : "alameda",
                       "password" : "alameda"}
            )
producer.flush()
