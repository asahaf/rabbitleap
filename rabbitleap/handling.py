# -*- coding: utf-8 -*-
"""
Handlers what actually consume message envelopes. When the router routes an
envelope and returns a handler instance to the :class:`.Consumer`, the
:class:`.Consumer` executes the handler by invoking its :meth:`~.Handler.pre_handle`,
:meth:`~.Handler.handle`, and :meth:`~.Handler.post_handle` methods respectively.
"""

from .envelope import Envelope


class Handler(object):
    """Base class for envelope handlers.

    :attr:`envelope` is a reference to the given message envelope.

    Subclasses MUST implement :meth:`handle` method.

    Methods:

    :meth:`initialize`: initialization hook used to initialize the handler with
    kwargs.

    :meth:`pre_handle`: pre handling method invoked before :meth:`handle`.

    :meth:`handle`: to be implemented by the subclasses for the actual handling logic.

    :meth:`post_handle`: post handling method invoked after :meth:`handle`.
    """

    def __init__(self, envelope, **kwargs):
        # type: (Handler, Envelope) -> None
        super(Handler, self).__init__()
        self.envelope = envelope

    def initialize(self, **kwargs):
        # type: (Handler) -> None
        """Initialize handler.

        This method is an initialization hook for the handler.

        """
        pass

    def pre_handle(self):
        # type: (Handler) -> None
        """Pre handle message envelope.

        This method is invoked by the :class:`.Consumer` before invoking :meth:`handle`.
        It's meant for validation and preparation before the actual handling.

        :class:`.Handler` subclasses can override this method to add pre handling logic.
        """
        pass

    def handle(self):
        # type: (Handler) -> None
        """Handle message envelope.

        This method is invoked by the :class:`.Consumer` after invoking
        :meth:`pre_handle`. The actual envelope handling should happen in this method.

        :class:`.Handler` subclasses MUST implement this method.
        """
        raise NotImplementedError()

    def post_handle(self):
        # type: (Handler) -> None
        """Post handle message envelope.

        This method is invoked by the consumer after invoking :meth:`handle`.
        It's meant for clean up and logging after the actual handling

        :class:`Handler` subclasses can override this method to add post handling
        logic.
        """
        pass


class MessageHandler(Handler):
    """Message handler.

    This class extens the :class:`.Handler` class with methods used to reject and skip
    envelopes, also report handling error.  It hold a reference to:class:`.Consumer`
    instance which does the execution.

    Methods:

    :meth:`~.Handler.initialize`: initialization method, a hook initialize the handler
    with kwargs.

    :meth:`~.Handler.pre_handle`: pre handling method invoked before
    :meth:`~.Handler.handle`.

    :meth:`~.Handler.handle`: overridden by the subclass implementing the handling
    logic.

    :meth:`~.Handler.post_handle`: post handling method invoked after
    :meth:`~.Handler.handle`.

    :meth:`error`: a shortcut for :meth:`.Consumer.error` to raise
    :exc:`.HandlingError` exception.

    :meth:`abort`: a shortcut for :meth:`.Consumer.abort` to reject message.

    :meth:`skip`: a shortcut for :meth:`.Consumer.skip` to skip message handling.
    """

    def __init__(self, consumer, envelope, **kwargs):
        # type: (MessageHandler, Consumer, Envelope) -> None
        super(MessageHandler, self).__init__(envelope, **kwargs)
        self.consumer = consumer

    @property
    def channel(self):
        # type: (MessageHandler) -> pika.channel.Channel
        """Shortcut for self.consumer.channel."""
        return self.consumer.channel

    def error(self, error_msg=None):
        # type: (MessageHandler, str) -> None
        """Raise :exc:`.HandlingError` exception.

        This method is a shortcut for :meth:`.Consumer.error`.

        :raise: :exc:`.HandlingError` when called.

        NOTE: when called inside the handler, the handler should re-raise
        :exc:`.HandlingError` exception to the consumer, in case the exception is
        handled inside.

        :param str error_msg: Error message.
        """
        self.consumer.error(error_msg)

    def abort(self, reason=None):
        # type: (MessageHandler, str) -> None
        """Abort handling the message.

        This method is a shortcut for :meth:`.Consumer.abort`.

        NOTE: when called inside the handler, the handler should re-raise
        :exc:`.AbortHandling` exception to the consumer if the exception is handled
        inside it.

        :param str reason: Reason for aborting handling the message.
        """
        self.consumer.abort(reason=reason)

    def skip(self, reason=None):
        # type: (MessageHandler, str) -> None
        """Skip handling the message.

        This method is a shortcut for :meth:`.Consumer.skip`.

        NOTE: when called inside the handler, the handler should re-raise
        :exc:`.SkipHandling` exception to the consumer if the exception is handled
        inside it.

        :param str reason: Reason for skipping handling the message.
        """
        self.consumer.skip(reason=reason)
