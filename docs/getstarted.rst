.. highlight:: shell

===========
Get Started
===========

Overview
--------
RabbitLeap is a simple RabbitMQ consuming framework. It's built on top of Pika, a RabbitMQ client library for Python.

Features
========
- Automatically recovers from connection failures
- Configurable retry policy for handing failures
- Automatically route messages to handlers, based on custom logic and different message properties

Installation
------------
Stable release
==============

To install RabbitLeap, run this command in your terminal:

.. code-block:: console

    $ pip install rabbit-leap

This is the preferred method to install RabbitLeap, as it will always install the most recent stable release.

If you don't have `pip`_ installed, this `Python installation guide`_ can guide
you through the process.

.. _pip: https://pip.pypa.io
.. _Python installation guide: http://docs.python-guide.org/en/latest/starting/installation/


From sources
============

The sources for RabbitLeap can be downloaded from the `Github repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone git://github.com/asahaf/rabbit-leap

Or download the `tarball`_:

.. code-block:: console

    $ curl  -OL https://github.com/asahaf/rabbit-leap/tarball/master

Once you have a copy of the source, you can install it with:

.. code-block:: console

    $ python setup.py install

Hello, World!
-------------
In this example, we're going to create a consumer that consumes 4 types of messages:
    1. Messages of type "rabbit.eat"
    2. Messages of type "rabbit.leap" or "rabbit.jump"
    3. Messages of type "dog.eat"

1. Defining Handlers
====================
We start with defining handlers for these 4 types of messages.

.. code-block:: python

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

Notice inside ``handle`` methods we access ``self.envelope``; The consumer creates an
envelope for each message it receives from RabbitMQ and it's available to the handler.
The :class:`.Envelope` contains message properties, payload, and delivery information.

2. Creating a Consumer
======================
Now, after we have all handlers defined, it's time to create a consumer and add the
handlers to it.

.. code-block:: python

    consumer_queue_name = 'consumer_queue'
    amqp_url = r'amqp://guest:guest@localhost:5672/%2f'

    consumer = Consumer(amqp_url=amqp_url, queue_name=consumer_queue_name)
    # route message of type `rabbit.eat` to RabbitEatHandler
    consumer.add_handler(r'rabbit\.eat', RabbitEatHandler)
    # route message of types rabbit.leap or rabbit.jump to RabbitLeapJumpHandler
    consumer.add_handler(r'rabbit\.(leap|jump)', RabbitLeapJumpHandler)
    consumer.add_handler(r'dog\.eat', DogEatHandler)

:meth:`~.Consumer.add_handler` accepts a regular expression pattern which is for
message type matching, and a Handler class which handles the message envelope.

3. Starting The Consumer
========================
Everything now is set and ready, lets start the consumer.

.. code-block:: python

    try:
        consumer.start()
    except KeyboardInterrupt:
        consumer.stop()

4. Putting Everything Together
==============================

.. code-block:: python

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

Save the file as ``consumer.py``

5. Running The Consumer Script
==============================
.. code-block:: console

    $ python consumer.py

6. Testing The Consumer
=======================
To test the consumer, we need to send some messages to its queue. To do that, we need
to create a small python program that connects to RabbitMQ and sends messages to the
consumer queue.

.. code-block:: python

    import pika
    from pika.spec import BasicProperties

    amqp_url = r'amqp://guest:guest@localhost:5672/%2f'
    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))

    channel = connection.channel()
    queue_name = 'consumer_queue'
    channel.queue_declare(queue=queue_name, durable=True)

    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        properties=BasicProperties(type='rabbit.eat'),
        body='carrot')

    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        properties=BasicProperties(type='rabbit.leap'),
        body='over the dog')

    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        properties=BasicProperties(type='rabbit.jump'),
        body='over the dog')

    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        properties=BasicProperties(type='dog.eat'),
        body='meat')

Save the file as ``publisher.py``

7. Running The Publisher Script
===============================
.. code-block:: console

    $ python publisher.py

Congratulations! you've created the first consumer. Next, navigate to :doc:`key_topics`
to understand the concepts.

.. _Github repo: https://github.com/asahaf/rabbit-leap
.. _tarball: https://github.com/asahaf/rabbit-leap/tarball/master
