# -*- coding: utf-8 -*-


class Envelope(object):
    """Message envelope

    Message properties, payload, and delivery information are all contained in this
    Envelope class.  Envelope is what is passed to the handler instance for handling.
    """

    def __init__(self, properties, payload, delivery_info):
        # type: (Envelope, BasicProperties, str|unicode|bytes, Basic.Deliver) -> None
        """
        :param pika.spec.BasicProperties prop: Message properties.

        :param str, unicode, or bytes (python 3.x) payload: Message body.

        :param pika.spec.Basic.Deliver delivery_info: Message delivery info.
        """
        self.properties = properties
        self.payload = payload
        self.delivery_info = delivery_info

    @property
    def consumer_tag(self):
        # type: (Envelope) -> int
        """Retrun message consumer tag.

        This is a shortcut for `self.delivery_info.consumer_tag`.
        """
        return self.delivery_info.consumer_tag

    @property
    def delivery_tag(self):
        # type: (Envelope) -> int
        """Retrun message delivery tag.

        This is a shortcut for `self.delivery_info.delivery_tag`.
        """
        return self.delivery_info.delivery_tag

    @property
    def redelivered(self):
        # type: (Envelope) -> bool
        """Retrun message redelivered.

        This is a shortcut for `self.delivery_info.redelivered`.
        """
        return self.delivery_info.redelivered

    @property
    def exchange(self):
        # type: (Envelope) -> str
        """Retrun message exchange.

        This is a shortcut for `self.delivery_info.exchange`.
        """
        return self.delivery_info.exchange

    @property
    def routing_key(self):
        # type: (Envelope) -> str
        """Retrun message routing key.

        This is a shortcut for `self.delivery_info.routing_key`.
        """
        return self.delivery_info.routing_key

    @property
    def headers(self):
        # type: (Envelope) -> dict
        """Retrun message headers.

        This is a shortcut for `self.properties.headers`.
        """
        return self.properties.headers

    @property
    def content_type(self):
        # type: (Envelope) -> str
        """Retrun message content type.

        This is a shortcut for `self.properties.content_type`.
        """
        return self.properties.content_type

    @property
    def content_encoding(self):
        # type: (Envelope) -> str
        """Retrun message content encoding.

        This is a shortcut for `self.properties.content_encoding`.
        """
        return self.properties.content_encoding

    @property
    def delivery_mode(self):
        # type: (Envelope) -> int
        """Retrun message delivery mode.

        This is a shortcut for `self.properties.delivery_mode`.
        """
        return self.properties.delivery_mode

    @property
    def priority(self):
        # type: (Envelope) -> int
        """Retrun message priority.

        This is a shortcut for `self.properties.priority`.
        """
        return self.properties.priority

    @property
    def correlation_id(self):
        # type: (Envelope) -> str
        """Retrun message correlation id.

        This is a shortcut for `self.properties.correlation_id`.
        """
        return self.properties.correlation_id

    @property
    def reply_to(self):
        # type: (Envelope) -> str
        """Retrun message reply_to.

        This is a shortcut for `self.properties.reply_to`.
        """
        return self.properties.reply_to

    @property
    def expiration(self):
        # type: (Envelope) -> str
        """Retrun message expiration.

        This is a shortcut for `self.properties.expiration`.
        """
        return self.properties.expiration

    @property
    def message_id(self):
        # type: (Envelope) -> str
        """Retrun message message id.

        This is a shortcut for `self.properties.message_id`.
        """
        return self.properties.message_id

    @property
    def timestamp(self):
        # type: (Envelope) -> int
        """Retrun message timestamp.

        This is a shortcut for `self.properties.timestamp`.
        """
        return self.properties.timestamp

    @property
    def type(self):
        # type: (Envelope) -> str
        """Retrun message type.

        This is a shortcut for `self.properties.type`.
        """
        return self.properties.type

    @property
    def user_id(self):
        # type: (Envelope) -> str
        """Retrun user id.

        This is a shortcut for `self.properties.user_id`.
        """
        return self.properties.user_id

    @property
    def app_id(self):
        # type: (Envelope) -> str
        """Retrun message app id.

        This is a shortcut for `self.properties.app_id`.
        """
        return self.properties.app_id

    @property
    def cluster_id(self):
        # type: (Envelope) -> str
        """Retrun message cluster id.

        This is a shortcut for `self.properties.cluster_id`.
        """
        return self.properties.cluster_id

    def get_header(self, header):
        # type: (Envelope, str) -> Any
        """Get message header."""
        if not self.properties.headers:
            return None
        return self.properties.headers.get(header)

    def set_header(self, header, value):
        # type: (Envelope, str, Any) -> None
        """Set message header"""
        if self.headers is None:
            self.properties.headers = {}

        self.headers[header] = value
