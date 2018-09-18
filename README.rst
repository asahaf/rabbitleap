.. image:: https://travis-ci.org/asahaf/rabbitleap.svg?branch=master
    :target: https://travis-ci.org/asahaf/rabbitleap

##########
RabbitLeap
##########

RabbitLeap is a simple RabbitMQ consuming framework. It's built on top of Pika, a RabbitMQ client library for Python.

Features
========
- Automatically recovers from connection failures
- Configurable retry policy for handing failures
- Automatically route messages to handlers, based on custom logic and different message properties

Installation
============
.. code-block:: console

    $ pip install rabbitleap

Hello, world
============

.. code-block:: python

    from rabbitleap.consumer import Consumer
    from rabbitleap.handling import MessageHandler


    class RabbitEatHandler(MessageHandler):

        def handle(self):
            print('rabbit eat: {}'.format(self.envelope.payload.decode('utf-8')))

    consumer_queue_name = 'consumer_queue'
    amqp_url = r'amqp://guest:guest@localhost:5672/%2f'

    consumer = Consumer(amqp_url=amqp_url, queue_name=consumer_queue_name)
    # route message of type `rabbit.eat` to RabbitEatHandler
    consumer.add_handler(r'rabbit\.eat', RabbitEatHandler)

    try:
        consumer.start()
    except KeyboardInterrupt:
        consumer.stop()

Documentation
=============
Documentation and resources are available at https://rabbitleap.readthedocs.io
