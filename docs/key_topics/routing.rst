#######
Routing
#######

.. automodule:: rabbitleap.routing
    :noindex:

Router
======
.. autoclass:: Router
    :noindex:
.. automethod:: Router.find_handler
    :noindex:

Rule Router
===========
.. autoclass:: RuleRouter
    :show-inheritance:
    :noindex:
.. automethod:: RuleRouter.add_rule
    :noindex:
.. automethod:: RuleRouter.set_default_rule
    :noindex:
.. automethod:: RuleRouter.find_handler
    :noindex:

Routing Rule
------------
.. autoclass:: Rule
    :noindex:

Matchers
--------
.. autoclass:: Matcher
    :noindex:
.. automethod:: Matcher.match
    :noindex:

.. autoclass:: AnyMatches
    :show-inheritance:
    :noindex:
.. automethod:: AnyMatches.match
    :noindex:

.. autoclass:: NoneMatches
    :show-inheritance:
    :noindex:
.. automethod:: NoneMatches.match
    :noindex:

.. autoclass:: MessageTypeMatches
    :show-inheritance:
    :noindex:
.. automethod:: MessageTypeMatches.match
    :noindex:
