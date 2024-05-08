from adistools.adisconfig import adisconfig
from adistools.log import Log

from pika import BlockingConnection, PlainCredentials, ConnectionParameters
from websockets.sync.client import connect as websocket_connect
from functools import partial
from json import dumps
from time import time
from base64 import b64encode
from hashlib import sha384
import hmac



class Conn_Settings:
    def __init__(self, api_key, api_secret):
        self._api_key=api_key
        self._api_secret=api_secret

        self._time=time()


    @property
    def payload(self):
        return {
            "request": "/v1/order/events",
            "nonce": self._time
            }

    @property
    def signature(self):
        return hmac.new(
            self._api_secret.encode(),
            b64encode(dumps(self.payload).encode()),
            sha384
            ).hexdigest()


    @property
    def headers(self):
        return {
            "X-GEMINI-PAYLOAD": b64encode(dumps(self.payload).encode()).decode(),
            "X-GEMINI-APIKEY": self._api_key,
            "X-GEMINI-SIGNATURE": self.signature
        }


class Fetcher:
    project_name = "moma-orders_fetcher"

    def __init__(self):
        self.active=True
        self.websocket_conn=None


        self.config = adisconfig('/opt/adistools/configs/moma-orders_fetcher.yaml')
        self.log = Log(
            parent=self,
            rabbitmq_host=self.config.rabbitmq.host,
            rabbitmq_port=self.config.rabbitmq.port,
            rabbitmq_user=self.config.rabbitmq.user,
            rabbitmq_passwd=self.config.rabbitmq.password,
            debug=self.config.log.debug,
        )

        self.rabbitmq_conn = BlockingConnection(
            ConnectionParameters(
                host=self.config.rabbitmq.host,
                port=self.config.rabbitmq.port,
                credentials=PlainCredentials(
                    self.config.rabbitmq.user,
                    self.config.rabbitmq.password
                )
            )
        )

        self.rabbitmq_channel = self.rabbitmq_conn.channel()

        self.websocket_connect()



    def websocket_connect(self):
        conn_settings=Conn_Settings(self.config.gemini.api_key,self.config.gemini.api_secret)
        self.websocket_conn=websocket_connect(
            additional_headers=conn_settings.headers,
            uri="wss://api.sandbox.gemini.com/v1/order/events"
            )

    def loop(self):
        while self.active:
            result=self.websocket_conn.recv()

            self.rabbitmq_channel.basic_publish(
                exchange="",
                routing_key="moma-orders_fetched",
                body=result
            )


    def start(self):
        self.loop()

if __name__ == "__main__":
    worker = Fetcher()
    worker.start()
