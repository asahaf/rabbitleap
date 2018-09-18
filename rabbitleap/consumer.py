# -*- coding: utf-8 -*-
"""
The :class:`.Consumer` is the main part of RabbitLeap framework. It's responsible
for connecting to RabbitMQ, besides, receiving, acknowledging, and rejecting messages.
It handles connection failures and automatically reconnects to RabbitMQ.  The consumer
consumes a share or exclusive RabbitMQ queue.

When the :class:`.Consumer` receives a message from RabbitMQ, it packages the message
properties, payload, and delivery information in an :class:`.Envelope` object,
preparing it for handling.  The envelope then, is routed to its handler by the
:class:`.Router`, which the consumer relies on to route envelopes to their handlers.

The consumer can be configured with a retry policy.  The :class:`.RetryPolicy` defines
how to do handling retries incase a timeout or an error happens
"""
import logging

import pika
from pika import SelectConnection
from pika.exceptions import AMQPChannelError, AMQPConnectionError

from .envelope import Envelope
from .exceptions import AbortHandling, HandlingError, SkipHandling
from .retry_policies import RetryPolicy
from .routing import (AnyMatches, MessageTypeMatches, NoneMatches, Rule,
                      RuleRouter)

logger = logging.getLogger(__name__)


class Consumer(RuleRouter):
    """RabbitMQ Consumer.

    Public Methods:

    :meth:`start`: start consumer.

    :meth:`stop`: stop consumer.

    :meth:`restart`: restart consumer.

    :meth:`abort`: reject message.

    :meth:`skip`: skip message handling.

    :meth:`error`: raise :exc:`.HandlingError` exception to report a handling error.

    :meth:`add_exchange_bind`: add exchange bind to the bindings list.

    :meth:`add_rule`: add a routing rule to the routing rules list.

    :meth:`set_default_rule`: set default routing rule to catch all unmatched messages

    :meth:`add_handler`: add handler, it creates a routing rule then add it to the
    routing rules list.

    :meth:`set_default_handler`: set default handler, it creates a rule then set it as
    default routing rule

    :meth:`set_retry_policy`: set a retry policy

    :meth:`unset_retry_policy`: un-set retry policy
    """

    def __init__(self,
                 amqp_url,
                 queue_name,
                 durable=True,
                 exclusive=False,
                 dlx_name=None,
                 auto_reconnect=True,
                 auto_reconnect_delay=3):
        # type: (Consumer, str, str, bool, bool, str, bool, int) -> None
        """Create a new instance of the consumer.

        :param str amqp_url: The AMQP url to connect with.

        :param str queue_name: Name of the queue which the consumer will consume.
            A new queue will be declared if queue doesn't exist.

        :param bool durable: Used when declaring a new queue, durable queue will survive
            reboots of the broker. Optional, default to ``True``

        :param bool exclusive: Used when declaring a new queue, exclusive queue only
            allows access by the current connection.  Optional, default to ``False``

        :param str dlx_name: Name of the dead letter exchange for the consumer's queue.
            Optional, default to ``None``

        :param bool auto_reconnect: Should consumer auto reconnect on a connection
            failure.  Optional, default to ``True``

        :param int auto_reconnect_delay: Delay between reconnect attempts in seconds.
            Optional, default to ``3``
        """
        super(Consumer, self).__init__(self)
        self._retry_policy = None
        self.connection = None
        self.channel = None
        self._closing = False
        self._consumer_tag = None
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.durable = durable
        self.exclusive = exclusive
        self.dlx_name = dlx_name
        self.auto_reconnect = auto_reconnect
        self.auto_reconnect_delay = auto_reconnect_delay
        self._exchange_binds = []
        self.ext = {}

    def start(self):
        # type: (Consumer) -> None
        """Start consumer."""
        logger.info('Starting...')
        self._closing = False
        self.connection = self._connect()
        self.connection.ioloop.start()

    def stop(self):
        # type: (Consumer) -> None
        """Stop consumer. """
        logger.info('Stopping...')
        self._closing = True
        # Graceful stop by scheduling
        self.connection.add_timeout(0, self._stop_consuming)

    def restart(self):
        # type: (Consumer) -> None
        """Restart consumer.

        Close the connection and reconnect again.
        """
        logger.info('Restarting...')
        # Graceful stop and restart by scheduling stop
        self.connection.add_timeout(0, self._stop_consuming)

    def abort(self, reason=None):
        # type: (Consumer, str) -> None
        """Abort handling the message.

        This can be called during :meth:`~.Handler.pre_handle` or
        :func:`~.Handler.handle` to abort handling. It raises :exc:`.AbortHandling`
        exception. The exception is handled by the consumer, and causes the consumer
        to reject the message.

        :raise: :exc:`.AbortHandling` when called

        NOTE: when called inside the handler, the handler MUST re-raise
        :exc:`.AbortHandling` exception to the consumer, in case the exception is
        handled inside.

        :param str reason: Reason for aborting handling the message
        """
        raise AbortHandling(reason=reason)

    def skip(self, reason=None):
        # type: (Consumer, str) -> None
        """Skip handling the message.

        This can be called during :meth:`~.Handler.pre_handle` or
        :meth:`~.Handler.handle` to skip handling.  It raises :exc:`.SkipHandling`
        exception. The exception is handled by the consumer, and causes the consumer
        to ack and skip the message.

        :raise: :exc:`.SkipHandling` when called

        NOTE: when called inside the handler, the handler should re-raise
        :exc:`.SkipHandling` exception to the consumer, in case the exception is
        handled inside.

        :param str reason: Reason for skipping handling the message
        """
        raise SkipHandling(reason=reason)

    def error(self, error_msg=None):
        # type: (Consumer, str) -> None
        """Raise :exc:`.HandlingError` exception.

        This method can be called inside the handler or invoked by the consumer in
        case of an error happened while handling the message.  This method
        raises :exc:`.HandlingError` which is handled by the consumer.  The consumer
        will retry handling the message if a retry policy is set or reject the message
        incase no retry policy is set. When calling this method inside the handler, it
        should be called during :meth:`~.Handler.pre_handle` or
        :meth:`~.Handler.handle`.

        :raise: :exc:`.HandlingError` when called

        NOTE: when called inside the handler, the handler should re-raise
        :exc:`.HandlingError` exception to the consumer, in case the exception is
        handled inside.

        :param str error_msg: Error message
        """
        raise HandlingError(error_msg)

    def add_exchange_bind(self,
                          exchange_name,
                          routing_key,
                          declare_exchange=False,
                          declare_kwargs=None):
        # type: (Consumer, str, str, bool, dict) -> None
        """Add exchange binding to the bindings list

        NOTE: Actual exchange binding is happening during the consumer start up,
        when the connection with RabbitMQ established.  Invoking this method after
        the connection is already established won't take affect until the consumer
        re-establishes the connection.  The method :meth:`restart` can be called to
        force the consumer to disconnect and reconnect again, doing exchange binding
        as part of that process.

        :raise: :exc:`.AssertionError` if `declare_exchange` is ``True`` and
            `declare_kwargs` is ``None`` or 'type' is not in `declare_kwargs`

        :param str exchange_name: name of the exchange to bind to.

        :param str routing_key: binding routing key.

        :param bool declare_exchange: should declare exchange before binding.

        :param dict declare_kwargs: is `exchange_declare()` arguments.

        declare_kwargs dict::

            'exchange_type': `required`,
            'durable': `optional`, default to ``False``.
            'auto_delete': `optional`, default to ``False``
            'internal': `optional`, default to ``False``
            `arguments`: `optional` additional exchange declaration arguments
        """
        if declare_exchange:
            assert isinstance(declare_kwargs, dict)
            assert 'type' in declare_kwargs
        self._exchange_binds.append((exchange_name, routing_key,
                                     declare_exchange, declare_kwargs))

    def add_handler(self, matcher_or_pattern, target, target_kwargs=None):
        # type: (Consumer, Matcher|str, Type[Handler]|Router, dict) -> None
        """Construct and add a routing rule for the provided handler.

        Handlers are added as routing rules.  A routing rule (:class:`.Rule`) is
        an object that contains a matcher (:class:`.Matcher`) instance, target
        (Handler subclass or a Router instance), and target kwargs.  A routing rule
        is constructed and added routing rules list.

        `matcher` is an instance of :class:`.Matcher` (can be a `str` explained later)
        which calling its :meth:`~.Matcher.match` method, passing :class:`.Envelope`
        instance, returns ``True`` or ``False`` indicating match or no match.

        `target` can be a :class:`.Handler` subclass or a :class:`.Router` instance.
        When a :class:`.Router` instance is provided as a target, it will act as
        a sub-router which has its own routing logic.  This way, a chain of
        routers can be constructed.

        `target_kwargs` is a dict passed to the handler
        :meth:`~.Handler.initialize` hook method.

        When finding a handler, the `Rule Router` (which the consumer is based on)
        goes through the list of rules sequentially in the order they were added,
        invoking the matcher's match method of the each rule.

        In case the router finds a match whose target is a router (sub-router)
        instance, its :meth:`~.Router.find_handler` is invoked it find handler

        In case, the match target is a subclass of :class:`.Handler`, the
        router creates a handler instance, invokes its
        :meth:`~.Handler.initialize` method passing the handler kwargs, then
        returns it.

        The router stops upon first match, returning the handler to the consumer.

        If a `default_handler` or `default_rule` is set, a default rule is added to the
        end of the routing rules list which will catch all unmatched messages.

        The router returns ``None`` when there is no match and no `default_rule`
        is set.

        Since the rules of the `Rule Router` (which the consumer is based on) are
        checked sequentially, more specific handlers should be added first, then
        generic ones later.

        When passed matcher is a string, the default matcher
        :class:`.MessageTypeMatches` is constructed and the passed string is its
        message type regular expression string

        :param Matcher|str matcher: a :class:`.Matcher` instance used to
            determin the match or a pattern string used for the default matcher
            type :class:`.MessageTypeMatches` as its message type regx pattern.

        :param Type[Handler]|Router target: a subclass of :class:`.Handler`
            or a :class:`.Router` instance.

        :param dict target_kwargs: a dict of kwargs that are passed to
            handler :meth:`~.Handler.initialize` method.  Only used when the target
            is a :class:`.Handler` subclass
        """
        # MessageTypeMatches is used when a string is provided
        if isinstance(matcher_or_pattern, str):
            rule = Rule(
                MessageTypeMatches(matcher_or_pattern), target, target_kwargs)
        else:
            rule = Rule(matcher_or_pattern, target, target_kwargs)
        self.add_rule(rule)

    def set_default_handler(self, default_target, default_target_kwargs=None):
        # type: (Consumer, Type[Handler]|Router, dict) -> None
        """Construct and add default routing rule for the given target

        This method constructs a routing rule for the given target and pass it to
        :meth:`~.RuleRouter.set_default_rule`.

        :param Type[Handler] default_target: Default target, which
            will catch all unmatched message. "None" means unset default target,
            unmached messages will be sent to dlx.

        :param dict default_target_kwargs: a dict of kwargs that are passed to
            handler :meth:`~.Handler.initialize` method.  Only used when the target
            is a :class:`.Handler` subclass
        """
        matcher = AnyMatches() if default_target else NoneMatches()
        default_target_kwargs = default_target_kwargs or {}
        rule = Rule(matcher, default_target, default_target_kwargs)
        self.set_default_rule(rule)

    def set_retry_policy(self, retry_policy):
        # type: (Consumer, RetryPolicy) -> None
        """Set retry policy.

        Retry policy can be an instance of any :class:`.RetryPolicy` subclass.

        :param RetryPolicy retry_policy: an instance of a retry policy
        """
        if retry_policy:
            assert isinstance(retry_policy, RetryPolicy)
        self._retry_policy = retry_policy

    def unset_retry_policy(self):
        # type: (Consumer) -> None
        """Unset retry policy."""
        self._retry_policy = None

    def _connect(self):
        # type: (Consumer) -> SelectConnection
        """ Connect to RabbitMQ

        :return pika.adapters.SelectConnection
        """
        param = pika.URLParameters(self.amqp_url)
        logger.info('Connecting to: {host}:{port}/{vhost}; SSL: {ssl}'.format(
            host=param.host,
            port=param.port,
            vhost=param.virtual_host,
            ssl=param.ssl))

        return SelectConnection(
            parameters=param,
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_open_connection_error,
            on_close_callback=self._on_connection_closed,
            stop_ioloop_on_close=False)

    def _on_connection_open(self, connection):
        # type: (Consumer, SelectConnection) -> None
        """Open connection success callback.

        This method is invoked by pika once the connection to RabbitMQ has
        been established.


        :param pika.adapters.SelectConnection connection: Connection instance
        """
        logger.info('Connection opened')
        self._open_channel()

    def _on_open_connection_error(self, connection, exception):
        # type: (Consumer, SelectConnection, str|Exception) -> None
        """Connection failure callback.

        This method is invoked by pika when open connection to RabbitMQ fail.
        Reconnect if auto_reconnect is set to True, otherwise shutdown the consumer
        and ioloop.


        :param pika.adapters.SelectConnection connection: Connection instance

        :param str|Exception exception: Exception
        """
        if self.auto_reconnect:
            self.auto_reconnect_delay
            logger.error('Connection error, reconnecting in {}s: {}'.format(
                self.auto_reconnect_delay, exception))
            self.connection.add_timeout(self.auto_reconnect_delay,
                                        self._reconnect)
        else:
            logger.error('Connection error: {}'.format(exception))
            self.connection.ioloop.stop()
            logger.info('Consumer stopped')

    def _on_connection_closed(self, connection, reason_code, reason_text):
        # type: (Consumer, SelectConnection, str, str) -> None
        """Connection closed callback.

        This method is invoked by pika when the connection to RabbitMQ is
        closed. Shutdown consumer and ioloop if closing. If not closing, reconnect
        if auto_reconnect is set to True otherwise stop consumer and ioloop


        :param pika.adapters.SelectConnection connection: Connection instance

        :param str reason_code: Close reason code coming from the server

        :param str reason_text: Close reason text coming from the server
        """
        self.channel = None
        if self._closing:
            self.connection.ioloop.stop()
        else:
            if self.auto_reconnect:
                logger.error(
                    'Connection closed, reconnecting in {}s: ({}) {}'.format(
                        self.auto_reconnect_delay, reason_code, reason_text))
                self.connection.add_timeout(self.auto_reconnect_delay,
                                            self._reconnect)
            else:
                logger.error('Connection closed: ({}) {}'.format(
                    reason_code, reason_text))
                self.connection.ioloop.stop()
                logger.info('Consumer stopped')

    def _reconnect(self):
        # type: (Consumer) -> None
        """Reconnect to broke.

        This method is invoked by consumer when auto_reconnect is set to True and the
        connection to RabbitMQ is closed due to a connection failure.
        """
        # This is the old connection IOLoop instance, stop its ioloop
        self.connection.ioloop.stop()

        if not self._closing:
            # Create a new connection
            self.connection = self._connect()
            # There is now a new connection, needs a new ioloop to run
            self.connection.ioloop.start()

    def _open_channel(self):
        # type: (Consumer) -> None
        logger.info('Creating a new channel')
        self.connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel):
        # type: (Consumer, Channel) -> None
        logger.info('Channel opened')
        self.channel = channel
        logger.info('Adding channel close callback')
        self.channel.add_on_close_callback(self._on_channel_closed)
        self._setup_exchanges()

    def _on_channel_closed(self, channel, reply_code, reply_text):
        # type: (Consumer, Channel, str, str) -> None
        logger.warning('Channel %i was closed: (%s) %s', channel, reply_code,
                       reply_text)
        self.connection.close()

    def _setup_exchanges(self):
        # type: (Consumer) -> None
        self._exchanges_gen = (exchange for exchange in self._exchange_binds)
        try:
            while True:
                name, _, declare, declare_kwargs = next(self._exchanges_gen)
                if declare:
                    break
            self._declare_exchange(name, declare_kwargs)
        except StopIteration:
            self._declare_consumer_queue()

    def _declare_exchange(self, name, declare_kwargs):
        # type: (Consumer, str, dict) -> None
        logger.info('Declaring exchange: "{}" of type: "{}"'.format(
            name, declare_kwargs['type']))

        self.channel.exchange_declare(
            callback=self._on_exchange_declareok,
            exchange=name,
            exchange_type=declare_kwargs['type'],
            durable=declare_kwargs.get('durable', False),
            auto_delete=declare_kwargs.get('auto_delete', False),
            internal=declare_kwargs.get('internal', False),
            arguments=declare_kwargs.get('arguments', None))

    def _on_exchange_declareok(self, frame):
        # type: (Consumer, pika.spec.Exchange.DeclareOk) -> None
        logger.info('Exchange declared')

        try:
            while True:
                name, _, declare, declare_kwargs = next(self._exchanges_gen)
                if declare:
                    break
            self._declare_exchange(name, declare_kwargs)
        except StopIteration:
            self._declare_consumer_queue()

    def _declare_consumer_queue(self):
        # type: (Consumer) -> None
        logger.info('Declaring queue: "{}"'.format(self.queue_name))
        args = None
        if self.dlx_name:
            args = {'x-dead-letter-exchange': self.dlx_name}
        self.channel.queue_declare(
            self._on_consumer_queue_declareok,
            queue=self.queue_name,
            durable=self.durable,
            exclusive=self.exclusive,
            arguments=args)

    def _on_consumer_queue_declareok(self, frame):
        # type: (Consumer, pika.spec.Queue.DeclareOk) -> None
        self._bind_consumer_queue_to_exchanges()

    def _bind_consumer_queue_to_exchanges(self):
        self._exchanges_gen = (exchange for exchange in self._exchange_binds)
        try:
            name, routing_key, _, _ = next(self._exchanges_gen)
            self._queue_bind(name, routing_key)
        except StopIteration:
            self._start_consuming()

    def _queue_bind(self, name, routing_key):
        # type: (Consumer, str, str) -> None
        logger.info('Binding {} to {} with {}'.format(name, self.queue_name,
                                                      routing_key))
        self.channel.queue_bind(self._on_bindok, self.queue_name, name,
                                routing_key)

    def _on_bindok(self, frame):
        # type: (Consumer, pika.spec.Exchange.BindOk) -> None
        logger.info('Queue bound')
        try:
            name, routing_key, _, _ = next(self._exchanges_gen)
            self._queue_bind(name, routing_key)
        except StopIteration:
            self._start_consuming()

    def _start_consuming(self):
        # type: (Consumer) -> None
        """Start consumer the queue."""
        logger.info('Start consuming')
        logger.info('Adding consumer cancellation callback')
        self.channel.add_on_cancel_callback(self._on_consumer_cancelled)
        self._consumer_tag = self.channel.basic_consume(self._on_message,
                                                        self.queue_name)

    def _on_consumer_cancelled(self, frame):
        # type: (Consumer, pika.spec.Basic.CancelOk) -> None
        logger.info('Consumer was cancelled remotely, shutting down: %r', frame)
        if self.channel:
            self.channel.close()

    def _on_message(self, channel, delivery_info, prop, payload):
        # type: (Consumer, Channel, Deliver, BasicProperties, str|unicode|bytes) -> None
        """Invoked by pika when a message is delivered from RabbitMQ.

        The channel is passed. The delivery_info object that is passed in carries the
        exchange, routing key, delivery tag and a redelivered flag for the message.
        The prop passed in is an instance of BasicProperties with the message
        properties and the payload is the message that was sent.

        Upon invoking this method, the consumer invokes :meth:`.Router.find_handler`
        method which returns a handler instance or ``None`` if no one is
        found. The consumer invokes the :func:`Handler.pre_handle()`,
        :func:`Handler.handle()`, and :func:`Handler.post_handle()`
        methods of the returned handler respectively.

        :param pika.channel.Channel channel: RabbitMQ channel

        :param pika.spec.Basic.Deliver delivery_info: Message delivery info

        :param pika.spec.BasicProperties prop: Message properties

        :param str, unicode, or bytes (python 3.x) payload: Message body
        """

        envelope = Envelope(prop, payload, delivery_info)
        app_id = envelope.app_id or 'Unknown app'
        m_type = envelope.type or 'Unknown type'
        logger.info('Received a message [{}] of type "{}" from "{}"'.format(
            envelope.message_id, m_type, app_id))
        try:
            handler = self.find_handler(envelope)
            if handler is None:
                self.abort('No handler to handle message [{}]'.format(
                    envelope.message_id))
            handler.pre_handle()
            handler.handle()
            handler.post_handle()
            self._ack_message(envelope)
        except SkipHandling as e:
            self._ack_message(envelope)
            logger.warning('Handling message [{}] is skipped. {}'.format(
                envelope.message_id, e.reason))
        except AbortHandling as e:
            self._reject_message(envelope)
            logger.warning('Message [{}] is rejected. {}'.format(
                envelope.message_id, e.reason))
        except HandlingError as e:
            logger.error('Error in handling message [{}]. Error: {}'.format(
                envelope.message_id, str(e)))
            if self._retry_policy:
                self._retry_policy.retry(envelope)
            else:
                self._reject_message(envelope)
        except AMQPConnectionError as e:
            logger.error('AMQP connection error: {}'.format(str(e)))
        except AMQPChannelError as e:
            logger.error('AMQP channel error: {}'.format(str(e)))
        except Exception as e:
            logger.error('Error in handling message [{}]. Error: {}'.format(
                envelope.message_id, e))
            if self._retry_policy:
                self._retry_policy.retry(envelope)
            else:
                self._reject_message(envelope)

    def _ack_message(self, envelope):
        # type: (Consumer, Envelope) -> None
        """ Acknowledge message.

        :param Envelope envelope: Message envelope
        """
        logger.info('Acknowledging message [%s] tag: %s', envelope.message_id,
                    envelope.delivery_tag)
        self.channel.basic_ack(envelope.delivery_tag)

    def _nack_message(self, envelope, requeue=False):
        # type: (Consumer, Envelope, bool) -> None
        """ Requeue, discarded or dead-lettered message.

        :param Envelope envelope: Message envelope

        :param bool requeue: If requeue is True, the server will attempt to requeue the
        message. If requeue is False or the requeue attempt fails the messages are
        discarded or dead-lettered.
        """
        logger.warning('Negative-Acknowledging message [%s] tag: %s',
                       envelope.message_id, envelope.delivery_tag)
        self.channel.basic_nack(envelope.delivery_tag, requeue=requeue)

    def _reject_message(self, envelope):
        # type: (Consumer, Envelope) -> None
        """ Discarded or dead-lettered message.

        :param Envelope envelope: Message envelope
        """
        logger.warning('Reject message [%s] tag: %s', envelope.message_id,
                       envelope.delivery_tag)
        self.channel.basic_reject(envelope.delivery_tag, requeue=False)

    def _stop_consuming(self):
        # type: (Consumer) -> None
        """Stop consuming message by issue cancel request."""
        if self.channel:
            logger.info('Sending a Basic.Cancel RPC command to broker')
            self.channel.basic_cancel(self._on_cancelok, self._consumer_tag)

    def _on_cancelok(self, frame):
        # type: (Consumer, Basic.CancelOk) -> None
        """Cancel consuming callback invoked when Basic.CancelOk response."""
        logger.info('Broker acknowledged the cancellation of the consumer')
        self._close_channel()

    def _close_channel(self):
        # type: (Consumer) -> None
        """Close channel."""
        logger.info('Closing the channel')
        self.channel.close()
