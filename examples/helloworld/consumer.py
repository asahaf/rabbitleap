# -*- coding: utf-8 -*-
from rabbitleap.consumer import Consumer
from rabbitleap.handling import MessageHandler


class RabbitEatHandler(MessageHandler):

    def handle(self):
        print('rabbit eat: {}'.format(self.envelope.payload.decode('utf-8')))


# Handling both "leap" and "jump"
class RabbitLeapJumpHandler(MessageHandler):

    def handle(self):
        print('{}: {}'.format(self.envelope.type,
                              self.envelope.payload.decode('utf-8')))


class DogEatHandler(MessageHandler):

    def handle(self):
        print('dog eat: {}'.format(self.envelope.payload.decode('utf-8')))


consumer_queue_name = 'consumer_queue'
amqp_url = r'amqp://guest:guest@localhost:5672/%2f'

consumer = Consumer(amqp_url=amqp_url, queue_name=consumer_queue_name)
# route message of type `rabbit.eat` to RabbitEatHandler
consumer.add_handler(r'rabbit\.eat', RabbitEatHandler)
# route message of types rabbit.leap or rabbit.jump to RabbitLeapJumpHandler
consumer.add_handler(r'rabbit\.(leap|jump)', RabbitLeapJumpHandler)
consumer.add_handler(r'dog\.eat', DogEatHandler)

try:
    consumer.start()
except KeyboardInterrupt:
    consumer.stop()
