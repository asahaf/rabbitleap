# -*- coding: utf-8 -*-
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
