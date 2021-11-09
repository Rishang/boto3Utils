import sys

sys.path.append("../")
from botoProfile import botoSession

b3 = botoSession()
b3.config("./config.json")
sess = b3.session()

client = sess.client("s3")

response = client.list_buckets()
print(response)
