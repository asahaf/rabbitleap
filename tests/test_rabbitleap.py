# -*- coding: utf-8 -*-
from unittest import TestCase
from mock import MagicMock

from pika.adapters import SelectConnection
from pika.channel import Channel
from pika.exceptions import AMQPChannelError, AMQPConnectionError
from pika.spec import Basic, BasicProperties

from rabbitleap.consumer import Consumer
from rabbitleap.envelope import Envelope
from rabbitleap.exceptions import AbortHandling, HandlingError, SkipHandling
from rabbitleap.handling import Handler, MessageHandler
from rabbitleap.retry_policies import (FixedDelayLimitedRetriesPolicy,
                                       FixedDelayUnlimitedRetriesPolicy,
                                       LimitedRetriesPolicy, RetryPolicy,
                                       UnlimitedRetriesPolicy)
from rabbitleap.routing import (AnyMatches, Matcher, MessageTypeMatches,
                                NoneMatches, Router, Rule, RuleRouter)


def create_envelope(payload='some payload'):
    prop = BasicProperties(
        content_type='application/json',
        content_encoding='utf-8',
        headers=None,
        delivery_mode=2,
        priority=7,
        correlation_id='some-correlation-id',
        reply_to='reply_to_name',
        expiration='some-expiration',
        message_id='some-message-id',
        timestamp='123456789',
        type='message-type-a',
        user_id='user-id',
        app_id='app-id',
        cluster_id='cluster-id')
    payload = payload or 'some data'
    delivery_info = Basic.Deliver(
        consumer_tag=34,
        delivery_tag=132,
        redelivered=True,
        exchange='exchange1',
        routing_key='routing_key1')
    envelope = Envelope(prop, payload, delivery_info)
    return envelope


def create_consumer():
    amqp_url = r'amqp://guest:guest@localhost:5672/%2f'
    queue_name = 'queue1'
    durable = True
    exclusive = True
    dlx_name = 'dead_exchange'
    auto_reconnect = True
    auto_reconnect_delay = 10

    consumer = Consumer(
        amqp_url=amqp_url,
        queue_name=queue_name,
        durable=durable,
        exclusive=exclusive,
        dlx_name=dlx_name,
        auto_reconnect=auto_reconnect,
        auto_reconnect_delay=auto_reconnect_delay)
    return consumer


class TestRabbitleap(TestCase):

    def test_envelope(self):
        prop = BasicProperties(
            content_type='application/json',
            content_encoding='utf-8',
            headers=None,
            delivery_mode=2,
            priority=7,
            correlation_id='some-correlation-id',
            reply_to='reply_to_name',
            expiration='some-expiration',
            message_id='some-message-id',
            timestamp='123456789',
            type='message-type-a',
            user_id='user-id',
            app_id='app-id',
            cluster_id='cluster-id')
        payload = 'some data'
        delivery_info = Basic.Deliver(
            consumer_tag=34,
            delivery_tag=132,
            redelivered=True,
            exchange='exchange1',
            routing_key='routing_key1')
        envelope = Envelope(prop, payload, delivery_info)

        self.assertEqual(envelope.payload, payload)
        self.assertEqual(envelope.properties, prop)
        self.assertEqual(envelope.delivery_info, delivery_info)
        self.assertEqual(envelope.consumer_tag, delivery_info.consumer_tag)
        self.assertEqual(envelope.delivery_tag, delivery_info.delivery_tag)
        self.assertEqual(envelope.redelivered, delivery_info.redelivered)
        self.assertEqual(envelope.routing_key, delivery_info.routing_key)
        self.assertEqual(envelope.exchange, delivery_info.exchange)
        self.assertEqual(envelope.content_type, prop.content_type)
        self.assertEqual(envelope.content_encoding, prop.content_encoding)
        self.assertEqual(envelope.headers, prop.headers)
        self.assertEqual(envelope.delivery_mode, prop.delivery_mode)
        self.assertEqual(envelope.priority, prop.priority)
        self.assertEqual(envelope.correlation_id, prop.correlation_id)
        self.assertEqual(envelope.reply_to, prop.reply_to)
        self.assertEqual(envelope.expiration, prop.expiration)
        self.assertEqual(envelope.message_id, prop.message_id)
        self.assertEqual(envelope.timestamp, prop.timestamp)
        self.assertEqual(envelope.type, prop.type)
        self.assertEqual(envelope.user_id, prop.user_id)
        self.assertEqual(envelope.app_id, prop.app_id)
        self.assertEqual(envelope.cluster_id, prop.cluster_id)
        self.assertEqual(envelope.headers, prop.headers)
        self.assertIsNone(envelope.get_header('some_header'))
        envelope.set_header('some_header', 50)
        envelope.set_header('another_header', 'hello')
        self.assertEqual(envelope.headers,
                         dict(some_header=50, another_header='hello'))
        self.assertEqual(envelope.get_header('some_header'), 50)
        self.assertEqual(envelope.get_header('another_header'), 'hello')
        self.assertIsNone(envelope.get_header('non_exist_header'))

    def test_exceptions(self):

        def raise_exception(Exception, **kwargs):
            raise Exception(**kwargs)

        msg = 'some message'
        with self.assertRaises(AbortHandling) as cm:
            raise_exception(AbortHandling, reason=msg)
            self.assertEqual(cm.exception.reason, msg)
            self.assertEqual(str(cm.exception), cm.exception.reason)

        with self.assertRaises(SkipHandling) as cm:
            raise_exception(SkipHandling, reason=msg)
            self.assertEqual(cm.exception.reason, msg)
            self.assertEqual(str(cm.exception), cm.exception.reason)

        with self.assertRaises(HandlingError) as cm:
            raise_exception(HandlingError, error_msg=msg)
            self.assertEqual(cm.exception.error_msg, msg)
            self.assertEqual(str(cm.exception), cm.exception.error_msg)

    def test_handling(self):
        envelope = create_envelope()
        handler = Handler(envelope)
        self.assertEqual(handler.envelope, envelope)
        handler.initialize(key='value', key2='value2')
        handler.pre_handle()
        self.assertRaises(NotImplementedError, handler.handle)
        handler.post_handle()

        envelope = create_envelope()
        consumer = Consumer('localhost', 'test')
        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        consumer.connection = mock_connection
        consumer.channel = mock_channel
        handler = MessageHandler(consumer, envelope)
        self.assertEqual(handler.envelope, envelope)
        self.assertEqual(handler.consumer, consumer)
        self.assertEqual(handler.channel, mock_channel)
        handler.initialize(key='value', key2='value2')
        handler.pre_handle()
        self.assertRaises(NotImplementedError, handler.handle)
        handler.post_handle()
        msg = 'some message'
        with self.assertRaises(AbortHandling) as cm:
            handler.abort(reason=msg)
            self.assertEqual(cm.exception.reason, msg)
        with self.assertRaises(SkipHandling) as cm:
            handler.skip(reason=msg)
            self.assertEqual(cm.exception.reason, msg)
        with self.assertRaises(HandlingError) as cm:
            handler.error(error_msg=msg)
            self.assertEqual(cm.exception.error_msg, msg)

    def test_retry_policy(self):
        envelope = create_envelope()
        consumer = create_consumer()
        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        consumer.connection = mock_connection
        consumer.channel = mock_channel
        retry_policy = RetryPolicy()
        self.assertRaises(
            NotImplementedError, retry_policy.retry, envelope=envelope)

    def test_unlimited_retries_policy(self):
        envelope = create_envelope()
        consumer = create_consumer()
        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        consumer.connection = mock_connection
        consumer.channel = mock_channel

        retry_policy = UnlimitedRetriesPolicy(
            consumer=consumer,
            initial_delay=0,
            delay_incremented_by=5,
            max_delay=30,
            retry_queue_suffix='r')
        mock_channel.queue_declare = MagicMock()
        mock_channel.basic_publish = MagicMock()
        mock_channel.basic_ack = MagicMock()

        retry_policy.retry(envelope)
        mock_channel.queue_declare.assert_called_with(
            callback=None,
            queue='{}.{}.{}'.format(consumer.queue_name,
                                    retry_policy.retry_queue_suffix, 0),
            durable=consumer.durable,
            nowait=True,
            arguments={
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': consumer.queue_name,
                'x-message-ttl': 0,
                'x-expires': 0 + retry_policy.min_retry_queue_ttl
            })

        mock_channel.basic_publish.assert_called_with(
            exchange='',
            routing_key='{}.{}.{}'.format(consumer.queue_name,
                                          retry_policy.retry_queue_suffix, 0),
            properties=envelope.properties,
            body=envelope.payload)
        mock_channel.basic_ack.assert_called_with(envelope.delivery_tag)

        self.assertEqual(
            envelope.get_header('x-original-delivery-info'), {
                'consumer_tag': envelope.consumer_tag,
                'delivery_tag': envelope.delivery_tag,
                'redelivered': envelope.redelivered,
                'exchange': envelope.exchange,
                'routing_key': envelope.routing_key
            })

        envelope.set_header(
            'x-death',
            [{
                'queue': consumer.queue_name,
                'count': 1
            },
             {
                 'queue':
                 '{}.{}.{}'.format(consumer.queue_name,
                                   retry_policy.retry_queue_suffix, 10000),
                 'count':
                 120
             }, {
                 'queue': 'some-queue',
                 'count': 1
             }])

        retry_policy.retry(envelope)
        delay = retry_policy.max_delay
        retry_queue_name = '{}.{}.{}'.format(
            consumer.queue_name, retry_policy.retry_queue_suffix, delay * 1000)
        mock_channel.queue_declare.assert_called_with(
            callback=None,
            queue=retry_queue_name,
            durable=consumer.durable,
            nowait=True,
            arguments={
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': consumer.queue_name,
                'x-message-ttl': delay * 1000,
                'x-expires': delay * 2 * 1000
            })

        mock_channel.basic_publish.assert_called_with(
            exchange='',
            routing_key=retry_queue_name,
            properties=envelope.properties,
            body=envelope.payload)
        mock_channel.basic_ack.assert_called_with(envelope.delivery_tag)
        self.assertEqual(
            envelope.get_header('x-original-delivery-info'), {
                'consumer_tag': envelope.consumer_tag,
                'delivery_tag': envelope.delivery_tag,
                'redelivered': envelope.redelivered,
                'exchange': envelope.exchange,
                'routing_key': envelope.routing_key
            })

    def test_limited_retries_policy(self):
        envelope = create_envelope()
        consumer = create_consumer()
        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        consumer.connection = mock_connection
        consumer.channel = mock_channel

        retry_policy = LimitedRetriesPolicy(
            consumer=consumer,
            retry_delays=(1, 5, 10, 50, 5 * 60),
            retry_queue_suffix='r')
        mock_channel.queue_declare = MagicMock()
        mock_channel.basic_publish = MagicMock()
        mock_channel.basic_ack = MagicMock()
        mock_channel.basic_reject = MagicMock()

        retry_policy.retry(envelope)
        mock_channel.queue_declare.assert_called_with(
            callback=None,
            queue='{}.{}.{}'.format(consumer.queue_name,
                                    retry_policy.retry_queue_suffix, 1000),
            durable=consumer.durable,
            nowait=True,
            arguments={
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': consumer.queue_name,
                'x-message-ttl': 1000,
                'x-expires': retry_policy.min_retry_queue_ttl
            })

        mock_channel.basic_publish.assert_called_with(
            exchange='',
            routing_key='{}.{}.{}'.format(
                consumer.queue_name, retry_policy.retry_queue_suffix, 1000),
            properties=envelope.properties,
            body=envelope.payload)
        mock_channel.basic_ack.assert_called_with(envelope.delivery_tag)

        self.assertEqual(
            envelope.get_header('x-original-delivery-info'), {
                'consumer_tag': envelope.consumer_tag,
                'delivery_tag': envelope.delivery_tag,
                'redelivered': envelope.redelivered,
                'exchange': envelope.exchange,
                'routing_key': envelope.routing_key
            })

        envelope.set_header(
            'x-death',
            [{
                'queue': consumer.queue_name,
                'count': 1
            },
             {
                 'queue':
                 '{}.{}.{}'.format(consumer.queue_name,
                                   retry_policy.retry_queue_suffix, 10000),
                 'count':
                 3
             }, {
                 'queue': 'some-queue',
                 'count': 1
             }])

        retry_policy.retry(envelope)
        delay = 5 * 60
        retry_queue_name = '{}.{}.{}'.format(
            consumer.queue_name, retry_policy.retry_queue_suffix, delay * 1000)
        mock_channel.queue_declare.assert_called_with(
            callback=None,
            queue=retry_queue_name,
            durable=consumer.durable,
            nowait=True,
            arguments={
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': consumer.queue_name,
                'x-message-ttl': delay * 1000,
                'x-expires': delay * 2 * 1000
            })

        mock_channel.basic_publish.assert_called_with(
            exchange='',
            routing_key=retry_queue_name,
            properties=envelope.properties,
            body=envelope.payload)
        mock_channel.basic_ack.assert_called_with(envelope.delivery_tag)
        self.assertEqual(
            envelope.get_header('x-original-delivery-info'), {
                'consumer_tag': envelope.consumer_tag,
                'delivery_tag': envelope.delivery_tag,
                'redelivered': envelope.redelivered,
                'exchange': envelope.exchange,
                'routing_key': envelope.routing_key
            })

        envelope.set_header(
            'x-death',
            [{
                'queue': consumer.queue_name,
                'count': 1
            },
             {
                 'queue':
                 '{}.{}.{}'.format(consumer.queue_name,
                                   retry_policy.retry_queue_suffix, 10000),
                 'count':
                 4
             }, {
                 'queue': 'some-queue',
                 'count': 1
             }])

        retry_policy.retry(envelope)
        mock_channel.basic_reject.assert_called_with(
            envelope.delivery_tag, requeue=False)

        envelope.set_header(
            'x-death',
            [{
                'queue': consumer.queue_name,
                'count': 1
            },
             {
                 'queue':
                 '{}.{}.{}'.format(consumer.queue_name,
                                   retry_policy.retry_queue_suffix, 10000),
                 'count':
                 46
             }, {
                 'queue': 'some-queue',
                 'count': 1
             }])

        retry_policy.retry(envelope)
        mock_channel.basic_reject.assert_called_with(
            envelope.delivery_tag, requeue=False)

    def test_fixed_delay_unlimited_retries_policy(self):
        consumer = create_consumer()
        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        consumer.connection = mock_connection
        consumer.channel = mock_channel
        retry_policy = FixedDelayUnlimitedRetriesPolicy(
            consumer, 10, retry_queue_suffix='h')
        self.assertEqual(isinstance(retry_policy, UnlimitedRetriesPolicy), True)
        self.assertEqual(retry_policy.initial_delay, 10)
        self.assertEqual(retry_policy.max_delay, 10)
        self.assertEqual(retry_policy.delay_incremented_by, 0)
        self.assertEqual(retry_policy.retry_queue_suffix, 'h')

    def test_fixed_delay_limited_retries_policy(self):
        consumer = create_consumer()
        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        consumer.connection = mock_connection
        consumer.channel = mock_channel
        retry_policy = FixedDelayLimitedRetriesPolicy(
            consumer, delay=10, retries_limit=7, retry_queue_suffix='s')
        self.assertEqual(isinstance(retry_policy, LimitedRetriesPolicy), True)
        self.assertEqual(retry_policy.retry_delays, tuple([10] * 7))
        self.assertEqual(retry_policy.retry_queue_suffix, 's')

    def test_router(self):
        envelope = create_envelope()
        router = Router()
        self.assertRaises(
            NotImplementedError, router.find_handler, envelope=envelope)

    def test_matcher(self):
        envelope = create_envelope()
        matcher = Matcher()
        self.assertRaises(NotImplementedError, matcher.match, envelope=envelope)

    def test_any_matches(self):
        envelope = create_envelope()
        matcher = AnyMatches()
        self.assertEqual(matcher.match(None), True)
        self.assertEqual(matcher.match(envelope), True)

    def test_none_matches(self):
        envelope = create_envelope()
        matcher = NoneMatches()
        self.assertEqual(matcher.match(None), False)
        self.assertEqual(matcher.match(envelope), False)

    def test_message_type_matches(self):
        envelope = create_envelope()
        # str pattern
        matcher = MessageTypeMatches(r'cat\.jump')
        self.assertEqual(matcher.match(envelope), False)
        envelope.properties.type = 'cat.jump'
        self.assertEqual(matcher.match(envelope), True)
        matcher = MessageTypeMatches(r'(cat|rabbit)\.jump')
        self.assertEqual(matcher.match(envelope), True)
        envelope.properties.type = 'rabbit.jump'
        self.assertEqual(matcher.match(envelope), True)
        envelope.properties.type = 'dog.jump'
        self.assertEqual(matcher.match(envelope), False)
        envelope.properties.type = None
        self.assertEqual(matcher.match(envelope), False)

        # re.Parrern
        import re
        envelope = create_envelope()
        matcher = MessageTypeMatches(re.compile(r'cat\.jump'))
        self.assertEqual(matcher.match(envelope), False)
        envelope.properties.type = 'cat.jump'
        self.assertEqual(matcher.match(envelope), True)
        matcher = MessageTypeMatches(re.compile(r'(cat|rabbit)\.jump'))
        self.assertEqual(matcher.match(envelope), True)
        envelope.properties.type = 'rabbit.jump'
        self.assertEqual(matcher.match(envelope), True)
        envelope.properties.type = 'dog.jump'
        self.assertEqual(matcher.match(envelope), False)
        envelope.properties.type = None
        self.assertEqual(matcher.match(envelope), False)

    def test_rule(self):

        with self.assertRaises(AssertionError):
            Rule('test', None, None)

        with self.assertRaises(AssertionError):
            Rule(AnyMatches(), 'test', None)

        with self.assertRaises(AssertionError):
            Rule(AnyMatches(), Router, None)

        router = Router()
        Rule(AnyMatches(), router, None)
        Rule(AnyMatches(), Handler, {'key': 10})

    def test_rule_router(self):
        consumer = create_consumer()
        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        consumer.connection = mock_connection
        consumer.channel = mock_channel

        with self.assertRaises(AssertionError):
            RuleRouter(consumer, 'None rule object')

        router = RuleRouter(consumer)
        self.assertEqual(router.consumer, consumer)
        envelope = create_envelope()
        self.assertIsNone(router.find_handler(envelope))
        default_rule = Rule(AnyMatches(), MessageHandler, None)
        router.set_default_rule(default_rule)
        self.assertIsNotNone(router.find_handler(envelope))
        handler = router.find_handler(envelope)
        self.assertEqual(isinstance(handler, Handler), True)
        router.set_default_rule(None)
        self.assertIsNone(router.find_handler(envelope))
        router = RuleRouter(consumer, default_rule)
        self.assertIsNotNone(router.find_handler(envelope))

        class CustomHandler(MessageHandler):

            def initialize(self, arg1, arg2, **kwargs):
                super(CustomHandler, self).initialize(**kwargs)
                self.arg1 = arg1
                self.arg2 = arg2

        message_type_matcher = MessageTypeMatches(r'message_a')
        rule = Rule(message_type_matcher, CustomHandler, {'arg1': 1, 'arg2': 2})
        router.add_rule(rule)
        envelope.properties.type = 'message_a'
        handler = router.find_handler(envelope)
        self.assertEqual(isinstance(handler, CustomHandler), True)
        self.assertEqual(handler.arg1, 1)
        self.assertEqual(handler.arg2, 2)
        self.assertEqual(handler.consumer, consumer)
        router.set_default_rule(default_rule)
        handler = router.find_handler(envelope)
        self.assertEqual(isinstance(handler, CustomHandler), True)
        envelope = create_envelope()
        handler = router.find_handler(envelope)
        self.assertEqual(isinstance(handler, Handler), True)

        # test subrouting
        default_subrouter_rule = Rule(message_type_matcher, CustomHandler, {
            'arg1': 1,
            'arg2': 2
        })
        subrouter = RuleRouter(consumer, default_subrouter_rule)
        # default rule for main router is the subrouter
        main_default_rule = Rule(AnyMatches(), subrouter, None)
        main_router = RuleRouter(consumer, main_default_rule)
        handler = main_router.find_handler(envelope)
        self.assertIsNone(handler)
        envelope.properties.type = 'message_a'
        handler = main_router.find_handler(envelope)
        self.assertEqual(isinstance(handler, CustomHandler), True)
        self.assertEqual(handler.arg1, 1)
        self.assertEqual(handler.arg2, 2)
        self.assertEqual(handler.consumer, consumer)

    def test_consumer(self):
        amqp_url = r'amqp://guest:guest@localhost:5672/%2f'
        queue_name = 'queue1'
        durable = True
        exclusive = True
        dlx_name = 'dead_exchange'
        auto_reconnect = True
        auto_reconnect_delay = 10

        consumer = Consumer(
            amqp_url=amqp_url,
            queue_name=queue_name,
            durable=durable,
            exclusive=exclusive,
            dlx_name=dlx_name,
            auto_reconnect=auto_reconnect,
            auto_reconnect_delay=auto_reconnect_delay)

        self.assertEqual(consumer.amqp_url, amqp_url)
        self.assertEqual(consumer.queue_name, queue_name)
        self.assertEqual(consumer.durable, durable)
        self.assertEqual(consumer.exclusive, exclusive)
        self.assertEqual(consumer.dlx_name, dlx_name)
        self.assertEqual(consumer.auto_reconnect, auto_reconnect)
        self.assertEqual(consumer.auto_reconnect_delay, auto_reconnect_delay)

        # Test abort
        with self.assertRaises(AbortHandling) as cm:
            consumer.abort('some reason')
            self.assertEqual(cm.exception.reason, 'some reason')

        # Test skip
        with self.assertRaises(SkipHandling) as cm:
            consumer.skip('some reason')
            self.assertEqual(cm.exception.reason, 'some reason')

        # Test error
        with self.assertRaises(HandlingError) as cm:
            consumer.error('some error')
            self.assertEqual(cm.exception.error_msg, 'some error')

        # Test bind to exchange
        exchange_name = 'exchange1'
        routing_key = 'some_routing_key'
        declare_exchange = True
        declare_kwargs = {'type': 'topic'}

        consumer.add_exchange_bind(
            exchange_name=exchange_name,
            routing_key=routing_key,
            declare_exchange=declare_exchange,
            declare_kwargs=declare_kwargs)

        self.assertEqual(
            consumer._exchange_binds[0],
            ((exchange_name, routing_key, declare_exchange, declare_kwargs)))

        with self.assertRaises(AssertionError) as cm:
            consumer.add_exchange_bind(
                exchange_name=exchange_name,
                routing_key=routing_key,
                declare_exchange=declare_exchange,
                declare_kwargs=None)

        with self.assertRaises(AssertionError) as cm:
            consumer.add_exchange_bind(
                exchange_name=exchange_name,
                routing_key=routing_key,
                declare_exchange=declare_exchange,
                declare_kwargs={})

        # Test add handler

        consumer.add_handler('some_pattern', Handler, None)
        self.assertEqual(len(consumer.rules), 2)
        self.assertEqual(
            isinstance(consumer.rules[-2].matcher, MessageTypeMatches), True)
        self.assertEqual(consumer.rules[-2].matcher.message_type_pattern,
                         'some_pattern')

        consumer.add_handler(AnyMatches(), Handler, None)
        self.assertEqual(len(consumer.rules), 3)
        self.assertEqual(
            isinstance(consumer.rules[-2].matcher, AnyMatches), True)

        # Test set default  handler

        consumer.set_default_handler(Handler)
        self.assertEqual(
            isinstance(consumer.rules[-1].matcher, AnyMatches), True)

        consumer.set_default_handler(None)
        self.assertEqual(
            isinstance(consumer.rules[-1].matcher, NoneMatches), True)

        # Test set/unset policy
        policy = FixedDelayLimitedRetriesPolicy(
            consumer=consumer, delay=5, retries_limit=15)
        consumer.set_retry_policy(policy)
        self.assertEqual(consumer._retry_policy, policy)

        self.assertRaises(AssertionError, consumer.set_retry_policy,
                          'non policy object')

        consumer.unset_retry_policy()
        self.assertIsNone(consumer._retry_policy)

        # Test handling message
        envelope = create_envelope()
        # Create fresh consumer
        consumer = create_consumer()

        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        mock_channel.basic_nack = MagicMock()
        mock_channel.basic_reject = MagicMock()
        consumer.channel = mock_channel

        # Test no handler, should reject message
        consumer._on_message(mock_channel, envelope.delivery_info,
                             envelope.properties, envelope.payload)

        consumer.channel.basic_reject.assert_called_with(
            envelope.delivery_tag, requeue=False)

        # Test positive message handling
        consumer.channel.basic_ack = MagicMock()
        pre_handle_mock = MagicMock()
        handle_mock = MagicMock()
        post_handle_mock = MagicMock()

        class SuccessHandler(Handler):

            def __init__(self, envelope, **kwargs):
                super(SuccessHandler, self).__init__(envelope, **kwargs)
                self.pre_handle = pre_handle_mock
                self.handle = handle_mock
                self.post_handle = post_handle_mock

        consumer.add_handler(AnyMatches(), SuccessHandler)
        consumer._on_message(mock_channel, envelope.delivery_info,
                             envelope.properties, envelope.payload)

        pre_handle_mock.assert_called_once()
        handle_mock.assert_called_once()
        post_handle_mock.assert_called_once()
        consumer.channel.basic_ack.assert_called_with(envelope.delivery_tag)

        # Test skip message handling
        # Create fresh consumer
        consumer = create_consumer()

        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        mock_channel.basic_nack = MagicMock()
        mock_channel.basic_reject = MagicMock()
        consumer.channel = mock_channel
        consumer.channel.basic_ack = MagicMock()
        pre_handle_mock = MagicMock()
        handle_mock = MagicMock()
        post_handle_mock = MagicMock()

        class SkipHandler(MessageHandler):

            def __init__(self, consumer, envelope, **kwargs):
                super(SkipHandler, self).__init__(consumer, envelope, **kwargs)
                self.handle = handle_mock
                self.post_handle = post_handle_mock

            def pre_handle(self):
                self.skip(reason='some reason')

        consumer.add_handler(AnyMatches(), SkipHandler)
        consumer._on_message(mock_channel, envelope.delivery_info,
                             envelope.properties, envelope.payload)

        handle_mock.assert_not_called()
        post_handle_mock.assert_not_called()
        consumer.channel.basic_ack.assert_called_with(envelope.delivery_tag)

        # Test abort message handling
        # Create fresh consumer
        consumer = create_consumer()

        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        mock_channel.basic_nack = MagicMock()
        mock_channel.basic_reject = MagicMock()
        consumer.channel = mock_channel
        consumer.channel.basic_ack = MagicMock()
        pre_handle_mock = MagicMock()
        handle_mock = MagicMock()
        post_handle_mock = MagicMock()

        class AbortHandler(MessageHandler):

            def __init__(self, consumer, envelope, **kwargs):
                super(AbortHandler, self).__init__(consumer, envelope, **kwargs)
                self.handle = handle_mock
                self.post_handle = post_handle_mock

            def pre_handle(self):
                self.abort(reason='some reason')

        consumer.add_handler(AnyMatches(), AbortHandler)
        consumer._on_message(mock_channel, envelope.delivery_info,
                             envelope.properties, envelope.payload)

        handle_mock.assert_not_called()
        post_handle_mock.assert_not_called()
        consumer.channel.basic_reject.assert_called_with(
            envelope.delivery_tag, requeue=False)

        consumer.channel.basic_ack.assert_not_called()

        # Test error message handling
        # Create fresh consumer
        consumer = create_consumer()

        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        mock_channel.basic_nack = MagicMock()
        mock_channel.basic_reject = MagicMock()
        consumer.channel = mock_channel
        consumer.channel.basic_ack = MagicMock()
        pre_handle_mock = MagicMock()
        handle_mock = MagicMock()
        post_handle_mock = MagicMock()

        class ErrorHandler(MessageHandler):

            def __init__(self, consumer, envelope, **kwargs):
                super(ErrorHandler, self).__init__(consumer, envelope, **kwargs)
                self.handle = handle_mock
                self.post_handle = post_handle_mock

            def pre_handle(self):
                self.error(error_msg='some reason')

        consumer.add_handler(AnyMatches(), ErrorHandler)
        consumer._on_message(mock_channel, envelope.delivery_info,
                             envelope.properties, envelope.payload)

        handle_mock.assert_not_called()
        post_handle_mock.assert_not_called()
        consumer.channel.basic_reject.assert_called_with(
            envelope.delivery_tag, requeue=False)

        consumer.channel.basic_ack.assert_not_called()

        # Test error message handling with retry policy
        # Create fresh consumer
        consumer = create_consumer()

        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        mock_channel.basic_nack = MagicMock()
        mock_channel.basic_reject = MagicMock()
        consumer.channel = mock_channel
        consumer.channel.basic_ack = MagicMock()
        pre_handle_mock = MagicMock()
        handle_mock = MagicMock()
        post_handle_mock = MagicMock()

        retry_policy = RetryPolicy()
        retry_policy.retry = MagicMock()
        consumer.set_retry_policy(retry_policy)
        consumer.add_handler(AnyMatches(), ErrorHandler)
        consumer._on_message(mock_channel, envelope.delivery_info,
                             envelope.properties, envelope.payload)

        handle_mock.assert_not_called()
        post_handle_mock.assert_not_called()
        consumer.channel.basic_reject.assert_not_called()
        consumer.channel.basic_ack.assert_not_called()
        retry_policy.retry.assert_called_once()

        # Test error message handling; general exception
        # Create fresh consumer
        consumer = create_consumer()

        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        mock_channel.basic_nack = MagicMock()
        mock_channel.basic_reject = MagicMock()
        consumer.channel = mock_channel
        consumer.channel.basic_ack = MagicMock()
        pre_handle_mock = MagicMock()
        handle_mock = MagicMock()
        post_handle_mock = MagicMock()

        class ExceHandler(MessageHandler):

            def __init__(self, consumer, envelope, **kwargs):
                super(ExceHandler, self).__init__(consumer, envelope, **kwargs)
                self.handle = handle_mock
                self.post_handle = post_handle_mock

            def pre_handle(self):
                raise Exception()

        consumer.add_handler(AnyMatches(), ExceHandler)
        consumer._on_message(mock_channel, envelope.delivery_info,
                             envelope.properties, envelope.payload)

        handle_mock.assert_not_called()
        post_handle_mock.assert_not_called()
        consumer.channel.basic_reject.assert_called_with(
            envelope.delivery_tag, requeue=False)

        consumer.channel.basic_ack.assert_not_called()

        # Test error message handling; general exception with retry policy
        # Create fresh consumer
        consumer = create_consumer()

        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        mock_channel.basic_nack = MagicMock()
        mock_channel.basic_reject = MagicMock()
        consumer.channel = mock_channel
        consumer.channel.basic_ack = MagicMock()
        pre_handle_mock = MagicMock()
        handle_mock = MagicMock()
        post_handle_mock = MagicMock()

        retry_policy = RetryPolicy()
        retry_policy.retry = MagicMock()
        consumer.set_retry_policy(retry_policy)
        consumer.add_handler(AnyMatches(), ExceHandler)
        consumer._on_message(mock_channel, envelope.delivery_info,
                             envelope.properties, envelope.payload)

        handle_mock.assert_not_called()
        post_handle_mock.assert_not_called()
        consumer.channel.basic_reject.assert_not_called()
        consumer.channel.basic_ack.assert_not_called()
        retry_policy.retry.assert_called_once()

        # Should handle AMQPConnectionError
        consumer = create_consumer()

        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        mock_channel.basic_nack = MagicMock()
        mock_channel.basic_reject = MagicMock()
        consumer.channel = mock_channel
        consumer.channel.basic_ack = MagicMock()
        pre_handle_mock = MagicMock()
        handle_mock = MagicMock()
        post_handle_mock = MagicMock()

        class AMQPConnectionErrorHandler(MessageHandler):

            def __init__(self, consumer, envelope, **kwargs):
                super(AMQPConnectionErrorHandler, self).__init__(
                    consumer, envelope, **kwargs)
                self.handle = handle_mock
                self.post_handle = post_handle_mock

            def pre_handle(self):
                raise AMQPConnectionError()

        consumer.add_handler(AnyMatches(), AMQPConnectionErrorHandler)
        consumer._on_message(mock_channel, envelope.delivery_info,
                             envelope.properties, envelope.payload)

        handle_mock.assert_not_called()
        post_handle_mock.assert_not_called()
        consumer.channel.basic_reject.assert_not_called()
        consumer.channel.basic_ack.assert_not_called()

        # Should handle AMQPChannelError
        consumer = create_consumer()

        mock_connection = SelectConnection()
        mock_channel = Channel(mock_connection, 10, None)
        mock_channel.basic_nack = MagicMock()
        mock_channel.basic_reject = MagicMock()
        consumer.channel = mock_channel
        consumer.channel.basic_ack = MagicMock()
        pre_handle_mock = MagicMock()
        handle_mock = MagicMock()
        post_handle_mock = MagicMock()

        class AMQPChannelErrorHandler(MessageHandler):

            def __init__(self, consumer, envelope, **kwargs):
                super(AMQPChannelErrorHandler, self).__init__(
                    consumer, envelope, **kwargs)
                self.handle = handle_mock
                self.post_handle = post_handle_mock

            def pre_handle(self):
                raise AMQPChannelError()

        consumer.add_handler(AnyMatches(), AMQPChannelErrorHandler)
        consumer._on_message(mock_channel, envelope.delivery_info,
                             envelope.properties, envelope.payload)

        handle_mock.assert_not_called()
        post_handle_mock.assert_not_called()
        consumer.channel.basic_reject.assert_not_called()
        consumer.channel.basic_ack.assert_not_called()

    def test_consumer_start_up_process(self):
        consumer = create_consumer()
        envelope = create_envelope()

        consumer.connection = SelectConnection()
        mock_channel = Channel(consumer.connection, 10, None)

        # Mock exchange_declare
        mock_channel.exchange_declare = MagicMock()

        def on_exchange_declareok(callback, exchange, exchange_type, durable,
                                  auto_delete, internal, arguments):
            self.assertEqual(exchange, 'exchange1')
            self.assertEqual(exchange_type, 'topic')
            self.assertEqual(durable, False)
            self.assertEqual(auto_delete, False)
            self.assertEqual(internal, False)
            self.assertEqual(arguments, None)
            callback(None)

        mock_channel.exchange_declare.side_effect = on_exchange_declareok

        # Mock queue_declare
        mock_channel.queue_declare = MagicMock()

        def on_consumer_queue_declareok(callback, queue, durable, exclusive,
                                        arguments):
            self.assertEqual(queue, consumer.queue_name)
            self.assertEqual(durable, consumer.durable)
            self.assertEqual(exclusive, consumer.exclusive)
            self.assertEqual(arguments,
                             {'x-dead-letter-exchange': consumer.dlx_name})
            callback(None)

        mock_channel.queue_declare.side_effect = on_consumer_queue_declareok

        # Mock queue_bind
        mock_channel.queue_bind = MagicMock()

        def on_bindok(callback, queue, exchange, routing_key):
            self.assertEqual(queue, consumer.queue_name)
            self.assertEqual(routing_key, 'key1')
            callback(None)

        mock_channel.queue_bind.side_effect = on_bindok

        mock_channel.add_on_close_callback = MagicMock()

        # Mock basic_consume
        mock_channel.basic_consume = MagicMock()

        def on_message(callback, queue):
            self.assertEqual(queue, consumer.queue_name)
            mock_channel.basic_reject = MagicMock()
            callback(mock_channel, envelope.delivery_info, envelope.properties,
                     envelope.payload)

        mock_channel.basic_consume.side_effect = on_message

        # connection.channel method used to open a new channel
        # mock method is used to return the created mock_channel
        consumer.connection.channel = MagicMock()

        def on_channel_open(on_open_callback):
            on_open_callback(mock_channel)

        consumer.connection.channel.side_effect = on_channel_open

        consumer.connection.ioloop.start = MagicMock()
        consumer.connection.ioloop.stop = MagicMock()
        consumer.add_exchange_bind(
            exchange_name='exchange1',
            routing_key='key1',
            declare_exchange=True,
            declare_kwargs={'type': 'topic'})
        consumer.add_exchange_bind(
            exchange_name='exchange1', routing_key='key1')

        # Initiate open connection manually
        consumer._on_connection_open(None)

        mock_channel.add_on_close_callback.assert_called_once()
        mock_channel.queue_declare.assert_called_once()
        mock_channel.basic_reject.assert_called_once()
        mock_channel.queue_declare.assert_called_once()
        queue_bind_count = len(mock_channel.queue_bind.call_args_list)
        self.assertEqual(queue_bind_count, 2)
        mock_channel.basic_consume.assert_called_once()

        # Stopping consumer

        # Mock add_timeout
        consumer.connection.add_timeout = MagicMock()

        def add_timeout_side_effect(delay, callback):
            callback()

        consumer.connection.add_timeout.side_effect = add_timeout_side_effect

        # Mock basic_cancel
        consumer.channel.basic_cancel = MagicMock()

        def on_cancelok(callback, consumer_tag):
            callback(None)

        consumer.channel.basic_cancel.side_effect = on_cancelok

        # Mock channel close
        consumer.channel.close = MagicMock()

        def channel_close_side_effect():
            consumer._on_channel_closed(consumer.channel, '', '')

        consumer.channel.close.side_effect = channel_close_side_effect

        # Mock connection close
        consumer.connection.close = MagicMock()

        def on_connection_closed():
            consumer._on_connection_closed(consumer.connection, '', '')

        consumer.connection.close.side_effect = on_connection_closed

        # Simulate stop
        consumer.stop()

        consumer.connection.add_timeout.assert_called_once()
        consumer.connection.close.assert_called_once()
        self.assertIsNone(consumer.channel)
        consumer.connection.ioloop.stop.assert_called_once()

    def test_consumer_auto_reconnect(self):
        consumer = create_consumer()

        consumer.connection = SelectConnection()
        consumer.connection.ioloop.stop = MagicMock()
        consumer.connection.ioloop.start = MagicMock()

        # Mock add_timeout
        consumer.connection.add_timeout = MagicMock()

        def add_timeout_side_effect(delay, callback):
            self.assertEqual(delay, consumer.auto_reconnect_delay)
            self.assertEqual(callback, consumer._reconnect)

        consumer.connection.add_timeout.side_effect = add_timeout_side_effect

        # Simulate connection failute
        consumer._on_connection_closed(consumer.connection, '', '')
        consumer.connection.add_timeout.assert_called_once()

        # Test shutdown on connection failure
        consumer.auto_reconnect = False
        consumer._on_connection_closed(consumer.connection, '', '')
        consumer.connection.ioloop.stop.assert_called_once()

    def test_consumer_auto_reconnect_error(self):
        consumer = create_consumer()

        consumer.connection = SelectConnection()
        consumer.connection.ioloop.stop = MagicMock()
        consumer.connection.ioloop.start = MagicMock()

        # Mock add_timeout
        consumer.connection.add_timeout = MagicMock()

        def add_timeout_side_effect(delay, callback):
            self.assertEqual(delay, consumer.auto_reconnect_delay)
            self.assertEqual(callback, consumer._reconnect)

        consumer.connection.add_timeout.side_effect = add_timeout_side_effect

        # Simulate reconnection failute
        consumer._on_open_connection_error(consumer.connection, Exception())
        consumer.connection.add_timeout.assert_called_once()

        # Test shutdown on reconnection failure
        consumer.auto_reconnect = False
        consumer._on_open_connection_error(consumer.connection, Exception())
        consumer.connection.ioloop.stop.assert_called_once()

    def test_consumer_reconnect(self):
        consumer = create_consumer()

        consumer.connection = SelectConnection()
        consumer._connect = MagicMock()

        def connect_side_effect():
            return consumer.connection

        consumer._connect.side_effect = connect_side_effect

        consumer.connection.ioloop.stop = MagicMock()
        consumer.connection.ioloop.start = MagicMock()

        consumer._reconnect()
        consumer._connect.assert_called_once()
        consumer.connection.ioloop.stop.assert_called_once()
        consumer.connection.ioloop.start.assert_called_once()

    def test_consumer_restart(self):
        consumer = create_consumer()

        consumer.connection = SelectConnection()
        consumer.connection.ioloop.stop = MagicMock()
        consumer.connection.ioloop.start = MagicMock()

        # Mock add_timeout
        consumer.connection.add_timeout = MagicMock()

        def add_timeout_side_effect(delay, callback):
            self.assertEqual(delay, 0)
            self.assertEqual(callback, consumer._stop_consuming)

        consumer.connection.add_timeout.side_effect = add_timeout_side_effect

        consumer.restart()
        consumer.connection.add_timeout.assert_called_once()
