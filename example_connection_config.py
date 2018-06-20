from collections import (
    namedtuple,
)

Host = namedtuple("Host", ["alias", "ip", "port", "user"])

hosts = (
    Host("local", "127.0.0.1", 22, "kevin"),
)

key_filenames = [
    "/home/ubuntu/pem1.pem",
    "/home/ubuntu/pem2.pem",
]
