============
Contributing
============

Contributions are welcome, and they are greatly appreciated! Every
little bit helps, and credit will always be given.

You can contribute in many ways:

Types of Contributions
----------------------

Report Bugs
~~~~~~~~~~~

Report bugs at https://github.com/asahaf/rabbitleap/issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

Fix Bugs
~~~~~~~~

Look through the GitHub issues for bugs. Anything tagged with "bug"
is open to whoever wants to implement it.

Implement Features
~~~~~~~~~~~~~~~~~~

Look through the GitHub issues for features. Anything tagged with "feature"
is open to whoever wants to implement it.

Write Documentation
~~~~~~~~~~~~~~~~~~~

RabbitLeap could always use more documentation, whether as part of the
official RabbitLeap docs, in docstrings, or even on the web in blog posts,
articles, and such.

Submit Feedback
~~~~~~~~~~~~~~~

The best way to send feedback is to file an issue at https://github.com/asahaf/rabbitleap/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

Get Started!
------------

Ready to contribute? Here's how to set up `rabbitleap` for
local development.

1. Fork_ the `rabbitleap` repo on GitHub.
2. Clone your fork locally::

    $ git clone git@github.com:your_name_here/rabbitleap.git

3. Create a branch for local development::

    $ git checkout -b name-of-your-bugfix-or-feature

Now you can make your changes locally.

4. When you're done making changes, check that your changes pass style and unit
   tests, including testing other Python versions with tox::

    $ tox

To get tox, just pip install it.

5. Commit your changes and push your branch to GitHub::

    $ git add .
    $ git commit -m "Your detailed description of your changes."
    $ git push origin name-of-your-bugfix-or-feature

6. Submit a pull request through the GitHub website.

.. _Fork: https://github.com/asahaf/rabbitleap/fork

Pull Request Guidelines
-----------------------

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.rst.
3. The pull request should work for Python 3.5, 3.6, and 3.7.
   Check https://travis-ci.org/asahaf/rabbitleap
   under pull requests for active pull requests or run the ``tox`` command and
   make sure that the tests pass for all supported Python versions.

Add a New Test
---------------
When fixing a bug or adding features, it's good practice to add a test to demonstrate your fix or new feature behaves as expected. These tests should focus on one tiny bit of functionality and prove changes are correct.

To write and run your new test, follow these steps:

1. Add the new test to `tests/test_rabbitleap.py`. Focus your test on the specific bug or a small part of the new feature.

2. If you have already made changes to the code, stash your changes and confirm all your changes were stashed::

    $ git stash
    $ git stash list

3. Run your test and confirm that your test fails. If your test does not fail, rewrite the test until it fails on the original code::

    $ py.test ./tests

4. (Optional) Run the tests with tox to ensure that the code changes work with different Python versions::

    $ tox

5. Proceed work on your bug fix or new feature or restore your changes. To restore your stashed changes and confirm their restoration::

    $ git stash pop
    $ git stash list

6. Rerun your test and confirm that your test passes. If it passes, congratulations!
