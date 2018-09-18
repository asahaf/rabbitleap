# -*- coding: utf-8 -*-
"""
When the :class:`.Consumer` receives a message from RabbitMQ, it prepares an
:class:`.Envelope` object of that message, for the handler.  However, the prepared
envelope somehow needs to be routed to its handler, where it’s actually consumed.
The envelope may be routed based on the message type, payload, or any other criteria;
It depends on the routing logic.  For this reason, the consumer doesn’t make the
routing decisions itself, it delegates the routing to a router.  The router sits
between the consumer and handlers. Its responsibility is, to route each incoming
envelope to its handler, returning the handler to the consumer for execution.
"""

import inspect
import re

from .handling import Handler


class Matcher(object):
    """Base class for matchers

    This is the base class for matcher. Matcher is used by :class:`Rule`
    to check if its target can handle the given :class:`.Envelope` or not

    Subclasses MUST implement :meth:`~.Matcher.match` method.
    """

    def __init__(self):
        # type: (Matcher) -> None
        super(Matcher, self).__init__()

    def match(self, envelope):
        # type: (Matcher, Envelope) -> bool
        """Return ``True`` or ``False`` indicating the target can handle the message.

        This method accepts an :class:`.Envelope` object as an argument and returns
        boolean, indicating whether the target can handle the envelope or not.

        Subclasses MUST implement this method

        :param Envelope envelope: Message envelope

        :return: can handle or not
        :rtype: bool
        """
        raise NotImplementedError()


class Router(object):
    """Base class for routers.

    Subclasses MUST implement :meth:`find_handler`
    """

    def __init__(self, **kwargs):
        # type: (Router) -> None
        super(Router, self).__init__()

    def find_handler(self, envelope):
        # type: (Router, Envelope) -> Handler|None
        """Find a handler

        This method expects an :class:`.Envelope` object as an argument and returns a
        :class:`.Handler` instance to its caller or ``None``, indicating the given
        envelope is unroutable.

        Subclasses routers MUST implement method.
        """
        raise NotImplementedError()


class Rule(object):
    """A matching rule.

    A rule (routing rule) is an object that links a matcher (:class:`.Matcher`)
    instance, to a target.

    The matcher is used to determines whether the target can handler the envelope.
    The target can be a :class:`.Handler` class or a :class:`Router` instance
    (sub-router). The target arguments is a dictionary passed to the newly created
    handler for initialization.
    """

    def __init__(self, matcher, target, target_kwargs=None):
        # type: (Rule, Matcher, Type[Handler]|Router, dict) -> None
        """Create a routing rule.

        :param Matcher matcher: a `Matcher` instance used to determin
        whether target can handler the envelope or not by invoking
        :meth:`~.Matcher.match` method

        :param target: a :class:`.Handler` class or a :class:`.Router`
            instance

        :param dict target_kwargs: a dict of arguments that are passed to
        :meth:`~.Handler.initialize` method of the handler instance
        """
        assert isinstance(matcher, Matcher)
        self.matcher = matcher
        if target:
            if inspect.isclass(target):
                assert issubclass(target, Handler)
            else:
                assert isinstance(target, Router)
        self.target = target
        target_kwargs = target_kwargs or {}
        assert isinstance(target_kwargs, dict)
        self.target_kwargs = target_kwargs


class RuleRouter(Router):
    """Rule router.

    Rule Router is an implementation of the base class :class:`Router`. It uses
    routing rules to route envelopes to handlers.  The rule router maintains a list
    routing rules, through which it goes sequentially to find a handler for a given
    envelope.  Rules added to the router are appended to the end of its rules list,
    and since the router goes through the routing rules sequentially in the order
    they’re added, more specific rules should be added first, then the general ones
    later.

    A routing rule is an instance of the class :class:`Rule`.  It has 3 fields, a
    :class:`Matcher`, target, and target arguments field.  The matcher is used to
    determines whether the target can handle the envelope.  The target can be a
    :class:`.Handler` class or a :class:`Router` instance (sub-router).
    The target arguments is a dictionary passed to the newly created handler for
    initialization.

    When the target is a :class:`.Handler` class,  the router creates an instance of
    it, then returns the instance to the caller. However, if the target is a
    :class:`Router` instance, it would act as a sub-router (child router).  The
    parent router delegates finding the handler to the child router.  The sub-router
    doesn’t have to be of the same type, it can be any :class:`Router` implementation.
    Chained routers let one router delegates the routing to the next one.

    The rule router returns ``None`` when the given envelope is unroutable
    (no handler can handle it).  However, the rule router may be configured with a
    default routing rule which catches all unroutable envelopes, check
    :meth:`set_default_rule`.

    Rule router always creates a new handler instance when its :meth:`find_handler`
    is called, even for the same envelope, except when the rule’s target is a
    :class:`Router` instance, which may have different implementation and may not
    return a new instance.

    Actually, the :class:`.Consumer` itself is a Rule Router. It implements extra stuff
    to communicate with RabbitMQ.
    """

    def __init__(self, consumer, default_rule=None):
        # type: (RuleRouter, Consumer, Type[Handler]|Router) -> None
        """
        :param Consumer consumer: message consumer instance

        :param Rule default_rule: default routing rule which catches all unmatched
        messages
        """
        super(RuleRouter, self).__init__()
        self.consumer = consumer
        self.rules = []
        default_rule = default_rule or Rule(NoneMatches(), None, None)
        assert isinstance(default_rule, Rule)
        self.add_rule(default_rule)

    def set_default_rule(self, rule):
        # type: (RuleRouter, Rule) -> None
        """ Set default rule.

        Default rule, when set, it catches all unroutable envelopes.

        :param Rule rule: default routing rule which catches all unmatched
            messages. ``None`` will unset default rule
        """
        rule = rule or Rule(NoneMatches(), None, None)
        # Default rule is always the last one
        self.rules[-1] = rule

    def add_rule(self, rule):
        # type: (RuleRouter, Rule) -> None
        """Add a routing rule to the routing rules list.

        A routing rule is added to the end of routing rules list, just before the
        default rule which is always the last one.

        A routing rule is an instance of the class :class:`Rule`.  It has 3 fields, a
        :class:`Matcher`, target, and target arguments field.  The matcher is used to
        determines whether the target can handler the envelope.  The target can be a
        :class:`.Handler` class or a :class:`Router` instance (sub-router).
        The target arguments is a dictionary passed to the newly created handler for
        initialization.

        Since the rules of the `Rule Router` are checked sequentially in the
        order they added, more specific rules should be added first, then generic
        ones later.

        :param Rule rule: routing rule instance.
        """
        # Always keep the last (default) rule in place
        self.rules.insert(-1, rule)

    def find_handler(self, envelope):
        # type: (RuleRouter, Envelope) -> Handler|None
        """Find and return a handler

        The router goes through the routing rules list sequentially to find a handler
        for the given envelope.

        When the target, in matched :class:`Rule`, is a :class:`.Handler` class, an
        instance of it is created and returned. However, if the target is a
        :class:`Router` instance, it would act as a sub-router.  The sub-router's
        :meth:`find_handler` is invoked to get a :class:`.Handler` instance.

        ``None`` is returned when the given envelope is unroutable (no handler can
        handle it).  However, if a default rule is set, its handler instance will be
        returned

        NOTE: The router always creates a new handler instance for each find handler
        call, even for the same message.

        :param Envelope envelope: Message envelope

        :return Handler: :class:`.Handler` instance
        """

        for rule in self.rules:
            match = rule.matcher.match(envelope)
            if match:
                if isinstance(rule.target, Router):
                    target = rule.target.find_handler(envelope)
                    if not target:
                        continue
                    return target

                target = rule.target(consumer=self.consumer, envelope=envelope)
                target.initialize(**rule.target_kwargs)
                return target
        return None


class AnyMatches(Matcher):
    """Match all messages macher

    This matcher matches nothing.  It always returns ``False``.  It's used in
    :class:`RuleRouter` when a default rule is set to catch all unroutable envelopes
    """

    def match(self, envelope):
        # type: (AnyMatches, Envelope) -> bool
        """Always return ``True``"""
        return True


class NoneMatches(Matcher):
    """Match noting macher

    This matcher matches nothing.  It always returns ``False``.
    """

    def match(self, envelope):
        # type: (NoneMatches, Envelope) -> bool
        """Always return ``False``"""
        return False


class MessageTypeMatches(Matcher):
    """Match messages based on message type macher

    This matcher does match based on the message type.

    The message type is provided as a regular expression string or a compiled
    ``re.Pattern``

    The matcher returns ``True`` when find a match in the message type, or
    ``False`` otherwise.
    """

    def __init__(self, message_type_pattern):
        # type: (MessageTypeMatches, str|re.Pattern) -> None
        self.message_type_pattern = message_type_pattern
        super(MessageTypeMatches, self).__init__()
        if isinstance(message_type_pattern, str):
            if not message_type_pattern.endswith('$'):
                message_type_pattern += '$'
            self.regex = re.compile(message_type_pattern)
        else:
            self.regex = message_type_pattern

    def match(self, envelope):
        # type: (MessageTypeMatches, Envelope) -> bool
        if envelope.type is None:
            return False
        match = self.regex.match(envelope.type)
        if match is None:
            return False
        return True
