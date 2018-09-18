##############
Retry Policies
##############

.. automodule:: rabbitleap.retry_policies
    :noindex:
    
    Retry Policy
    ============
    .. autoclass:: RetryPolicy
        :noindex:

    Unlimited Retries Policy
    ^^^^^^^^^^^^^^^^^^^^^^^^
    .. autoclass:: UnlimitedRetriesPolicy
        :show-inheritance:
        :noindex:
    .. automethod:: UnlimitedRetriesPolicy.retry
        :noindex:

    Limited Retries Policy
    ^^^^^^^^^^^^^^^^^^^^^^
    .. autoclass:: LimitedRetriesPolicy
        :show-inheritance:
        :noindex:
    .. automethod:: LimitedRetriesPolicy.retry
        :noindex:

    Fixed Delay Unlimited Retries Policy
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .. autoclass:: FixedDelayUnlimitedRetriesPolicy
        :show-inheritance:
        :noindex:
    .. automethod:: FixedDelayUnlimitedRetriesPolicy.retry
        :noindex:

    Fixed Delay Limited Retries Policy
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    .. autoclass:: FixedDelayLimitedRetriesPolicy
        :show-inheritance:
        :noindex:
    .. automethod:: FixedDelayLimitedRetriesPolicy.retry
        :noindex:
