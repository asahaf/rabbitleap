# -*- coding: utf-8 -*-
"""
Noting is perfect, errors and timeouts may happen, and when such failures happen, the
consumer has to decide what to do with that. By default, the consumer would reject the
envelope (RabbitMQ message) when a failure happens. However, errors and timeouts
issues, unless there is a software bug, usually solved with retries. Just like the
routing, the consumer doesn't make the retry decision itself, the consumer delegates
it to a retry policy. Retry policy defines how the retry is performed. Retries
usually happens with back-offs to avoid worsening the situation by hammering other
services with more requests, especially if it was a timeout issue. The consumer can be
configured to use a retry policy by calling :meth:`.Consumer.set_retry_policy`, passing
an instance of :class:`.RetryPolicy`. When a retry policy is set, the consumer won't
reject messages, but rather, it send them to the retry policy to deal with the
situation by invoking :meth:`.RetryPolicy.retry` method. Based on it's implementation,
The retry policy decides how to do retries.

There are 4 different retry policies available:

1. :class:`.UnlimitedRetriesPolicy`, Unlimited retries policy
2. :class:`.LimitedRetriesPolicy`, Limited retries policy
3. :class:`.FixedDelayUnlimitedRetriesPolicy`, Fixed delay unlimited retries policy
4. :class:`.FixedDelayLimitedRetriesPolicy`, Fixed delay limited retries policy

Custom retry policies can be created by implementing the base class
:class:`.RetryPolicy`
"""
import logging

logger = logging.getLogger(__name__)


class RetryPolicy(object):
    """Base class for retry policies.

    Subclasses MUST implement :meth:`retry` method.
    """

    def __init__(self, **kwargs):
        # type: (RetryPolicy) -> None
        super(RetryPolicy, self).__init__()

    def retry(self, envelope):
        # type: (RetryPolicy, Envelope) -> None
        """This method is implemented by the subclass."""
        raise NotImplementedError()


class BaseRetryPolicy(RetryPolicy):
    """Base retry policy class for :class:`.UnlimitedRetriesPolicy` and
    :class:`.LimitedRetriesPolicy`.

    It has implementation for geting mesage death count and retry queue creation.
    """

    def __init__(self, consumer, retry_queue_suffix='retry', **kwargs):
        # type: (BaseRetryPolicy, Consumer, str) -> None
        """
        :param Consumer consumer: message consumer instance

        :param str retry_queue_suffix: Suffix used when creating retry queues. Retry
            queue names are constructed in this form "queue_name.<suffix>.<delay>".
            Optional, default to ``retry``
        """
        super(BaseRetryPolicy, self).__init__(**kwargs)
        retry_queue_suffix = retry_queue_suffix.strip()
        self.consumer = consumer
        assert len(retry_queue_suffix) > 0
        self.retry_queue_suffix = retry_queue_suffix
        # To avoid frequent retry queue create and destroy for low retry delays
        self.min_retry_queue_ttl = 20 * 1000  # 20 seconds

    def set_original_delivery_info_header(self, envelope):
        # type: (BaseRetryPolicy, Envelope) -> None
        """Save original message delivery infomation in a header."""
        if not envelope.get_header('x-original-delivery-info'):
            original_delivery_info = {
                'consumer_tag': envelope.delivery_info.consumer_tag,
                'delivery_tag': envelope.delivery_info.delivery_tag,
                'redelivered': envelope.delivery_info.redelivered,
                'exchange': envelope.delivery_info.exchange,
                'routing_key': envelope.delivery_info.routing_key
            }
            envelope.set_header('x-original-delivery-info',
                                original_delivery_info)

    def get_death_count(self, envelope):
        # type: (BaseRetryPolicy, Envelope) -> int
        """Return the death count of a message by examining "x-death" header.

        :param Envelope envelope: Message envelope

        :return int: death count
        """
        death_header = envelope.get_header('x-death')

        if death_header is None:
            return 0

        count = 0
        for death in death_header:
            if not death['queue'].startswith(self.consumer.queue_name):
                continue
            count += death.get('count', 1)
        return count

    def declare_retry_queue(self, delay):
        # type: (BaseRetryPolicy, int) -> str
        """Declare a retry queue for the provided delay.

        Each different delay has a different queue where all retry messages with the
        same delay will be sent to till they expire and get sent back to the original
        queue for handling retry.  The queue is declared with a TTL and automatically
        gets deleted.  The queue TTL is equal to the provided delay.  The retry
        queue's dead letter exchange is (default) direct exchange and the dead letter
        routing key is the original queue name where the messages originally
        came from.  The messages will be sent back to the original queue when they
        reach their TTL, for handling retry.

        The retry queue is redeclared before every a new message is sent to it.
        Redeclaration resets the queue's TTL, preventing it from being destroyed.


        :param int delay: Retry delay in seconds

        :return: retry queue name
        :rtype: str
        """

        delay_in_ms = int(delay * 1000)
        retry_queue_name = '{}.{}.{}'.format(
            self.consumer.queue_name, self.retry_queue_suffix, delay_in_ms)

        # To avoid frequent queue create and destroy for low retry delays
        queue_ttl = delay_in_ms * 2
        if queue_ttl < self.min_retry_queue_ttl:
            queue_ttl = self.min_retry_queue_ttl

        self.consumer.channel.queue_declare(
            callback=None,
            queue=retry_queue_name,
            durable=self.consumer.durable,
            nowait=True,
            arguments={
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': self.consumer.queue_name,
                'x-message-ttl': delay_in_ms,
                'x-expires': queue_ttl
            })
        logger.warning(
            'Retry queue "{}" is created/redeclared'.format(retry_queue_name))
        return retry_queue_name


class UnlimitedRetriesPolicy(BaseRetryPolicy):
    """Unlimited Retries Policy.

    This is an implementation of :class:`.RetryPolicy` which does incremental backoff,
    unlimited retries.

    :attr:`initial_delay`: is the initial/first backoff delay in seconds

    :attr:`delay_incremented_by`: is number of seconds the backoff should be incremented
    by after each death

    :attr:`max_delay`: is the final/maximum backoff delay in seconds that should net be
    exceeded
    """

    def __init__(self,
                 consumer,
                 initial_delay,
                 max_delay,
                 delay_incremented_by,
                 retry_queue_suffix='retry',
                 **kwargs):
        # type: (UnlimitedRetriesPolicy, Consumer, int, int, int, str) -> None
        """
        :param Consumer consumer: message consumer instance

        :param int initial_delay: `initial_delay` is the initial/first backoff delay
            in seconds.

        :param int max_delay: `max_delay` is the final/maximum backoff delay in seconds
            that should net be exceeded. When exceeded, this max is used.

        :param int delay_incremented_by: `delay_incremented_by` is number of seconds
            the backoff should be incremented by after each death.

        :param: str retry_queue_suffix: suffix used when naming retry queues.
        """
        super(UnlimitedRetriesPolicy,
              self).__init__(consumer, retry_queue_suffix, **kwargs)

        assert initial_delay >= 0
        assert delay_incremented_by >= 0
        assert max_delay >= initial_delay

        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.delay_incremented_by = delay_incremented_by

    def retry(self, envelope):
        # type: (UnlimitedRetriesPolicy, Envelope) -> None
        """Send message to retry queue to retry handling it later.

        Death count is calculated by examining 'x-death' header.  Based on the death
        count, the message is sent to a retry queue where it waits there till it
        expires and gets sent back to the original queue for handling retry.

        :param Envelope envelope: Message envelope
        """
        death_count = self.get_death_count(envelope)
        delay = self.initial_delay + (death_count * self.delay_incremented_by)

        if delay > self.max_delay:
            delay = self.max_delay

        retry_queue_name = self.declare_retry_queue(delay)

        # Save original delivery information
        if envelope.get_header('x-original-delivery-info') is None:
            self.set_original_delivery_info_header(envelope)

        self.consumer.channel.basic_publish(
            exchange='',
            routing_key=retry_queue_name,
            properties=envelope.properties,
            body=envelope.payload)

        self.consumer.channel.basic_ack(envelope.delivery_tag)
        logger.warning(
            'Retry handling message [{}] after {}s; death count: {}'.format(
                envelope.message_id, delay, death_count + 1))


class LimitedRetriesPolicy(BaseRetryPolicy):
    """Limited Retries Policy.

    This is an implementation of :class:`.RetryPolicy` which does incremental backoff,
    limited number of retries.

    :attr:`consumer`: message consumer instance

    :attr:`retry_delays`: immutable list of retry backoff delays in seconds. Message
    is sent to dlx when this list is exhausted. e.g ``(1, 5, 10, 60, 5 * 60)``

    :attr:`retry_queue_suffix`: suffix str used when naming retry queues.
    """

    def __init__(self,
                 consumer,
                 retry_delays,
                 retry_queue_suffix='retry',
                 **kwargs):
        # type: (LimitedRetriesPolicy, Consumer, Iterable[int], str) -> None
        """
        :param Consumer consumer: message consumer instance

        :param Iterable[int] retry_delays: Immutable list of retry backoff delays in
                seconds. Message is sent to dlx when this list is exhausted.
                e.g ``(1, 5, 10, 60, 5 * 60)``

        :param: str retry_queue_suffix: suffix used when naming retry queues.
        """
        assert len(retry_delays) > 0
        super(LimitedRetriesPolicy, self).__init__(consumer, retry_queue_suffix,
                                                   **kwargs)
        self.retry_delays = retry_delays

    def retry(self, envelope):
        # type: (LimitedRetriesPolicy, Envelope) -> None
        """Send message to retry queue to retry handling it later.

        Death count is calculated by examining 'x-death' header.  Based on the death
        count, the message is sent to a retry queue where it waits there till it
        expires and gets sent back to the original queue for handling retry.

        The death count is used as an index for `retry_delays` list. Where each
        item in the list represents a retry delay in seconds.

        The message will be rejected if the death count exceeded the length of
        `retry_delays` list.

        :param Envelope envelope: Message envelope
        """
        death_count = self.get_death_count(envelope)
        if death_count < len(self.retry_delays):
            delay = self.retry_delays[death_count]
            retry_queue_name = self.declare_retry_queue(delay)

            # Save original delivery information
            if envelope.get_header('x-original-delivery-info') is None:
                self.set_original_delivery_info_header(envelope)

            self.consumer.channel.basic_publish(
                exchange='',
                routing_key=retry_queue_name,
                properties=envelope.properties,
                body=envelope.payload)

            self.consumer.channel.basic_ack(envelope.delivery_tag)
            logger.warning(
                'Retry handling message [{}] after {}s; death count: {}'.format(
                    envelope.message_id, delay, death_count + 1))
        else:
            logger.warning(
                'Message [{}] exceeded retry limit; death count: {}'.format(
                    envelope.message_id, death_count + 1))
            self.consumer.channel.basic_reject(
                envelope.delivery_tag, requeue=False)
            logger.error('Message [{}] is rejected'.format(envelope.message_id))


class FixedDelayUnlimitedRetriesPolicy(UnlimitedRetriesPolicy):
    """Fixed delay unlimited retries policy.

    This is an implementation of :class:`.RetryPolicy` which does fix backoff delay,
    unlimited retries.

    :attr:`consumer`: consumer instance

    :attr:`delay`: retry delay in seconds

    :attr:`retry_queue_suffix`: suffix str used when naming retry queues.
    """

    def __init__(self, consumer, delay, retry_queue_suffix='retry', **kwargs):
        # type: (FixedDelayUnlimitedRetriesPolicy, Consumer, int, str) -> None
        """
        :param Consumer consumer: message consumer instance

        :param int delay: retry delay in seconds

        :param: str retry_queue_suffix: suffix used when naming retry queues.
        """
        super(FixedDelayUnlimitedRetriesPolicy, self).__init__(
            consumer=consumer,
            initial_delay=delay,
            max_delay=delay,
            delay_incremented_by=0,
            retry_queue_suffix=retry_queue_suffix,
            **kwargs)


class FixedDelayLimitedRetriesPolicy(LimitedRetriesPolicy):
    """Fixed delay limited retries policy.

    This is an implementation of :class:`.RetryPolicy` which does fix backoff delay,
    limited number of retries.

    :attr:`consumer`: consumer instance

    :attr:`delay`: retry delay in seconds.

    :attr:`retries_limit`: retries limit count.

    :attr:`retry_queue_suffix`: suffix str used when naming retry queues.
    """

    def __init__(self,
                 consumer,
                 delay,
                 retries_limit,
                 retry_queue_suffix='retry',
                 **kwargs):
        # type: (FixedDelayLimitedRetriesPolicy, Consumer, int, int, str) -> None
        """
        :param Consumer consumer: message consumer instance

        :param int delay: retry delay in seconds

        :param int retries_limit: retries limit count

        :param: str retry_queue_suffix: suffix used when naming retry queues.
        """
        assert retries_limit > 0
        retry_delays = tuple([delay] * retries_limit)
        super(FixedDelayLimitedRetriesPolicy, self).__init__(
            consumer=consumer,
            retry_delays=retry_delays,
            retry_queue_suffix=retry_queue_suffix,
            **kwargs)
