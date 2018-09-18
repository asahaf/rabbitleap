# -*- coding: utf-8 -*-
class AbortHandling(Exception):
    """This exception is raised when :meth:`~.Consumer.abort` method is called.

    This exception is handled by the consumer to abort handling the message
    and reject it.
    """

    def __init__(self, reason=None):
        # type: (AbortHandling, str) -> None
        super(AbortHandling, self).__init__()
        self.reason = reason


class SkipHandling(Exception):
    """This exception is raised when :meth:`~.Consumer.skip` method is called.

    This exception is handled by the consumer to skip handling the message.
    """

    def __init__(self, reason=None):
        # type: (SkipHandling, str) -> None
        super(SkipHandling, self).__init__()
        self.reason = reason


class HandlingError(Exception):
    """This exception is raised when :meth:`~.Consumer.error` method is called.

    This exception is raise when :meth:`~.Consumer.error` method is called upon error
    in handling message.  The exception is handled by the consumer to retry the
    handling message if a retry policy is set, or reject it otherwise.
    """

    def __init__(self, error_msg=None):
        # type: (HandlingError, str) -> None
        super(HandlingError, self).__init__()
        self.error_msg = error_msg
