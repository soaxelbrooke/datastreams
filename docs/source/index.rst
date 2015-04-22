.. datastreams documentation master file, created by
   sphinx-quickstart on Wed Apr 22 05:57:43 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

datastreams
***********

Efficient, concise stream data processing.

.. code-block:: python

    >>> from datastreams import DataStream

    >>> DataStream("Hello, gorgeous.")\
    ...     .filter(lambda char: char.isalpha())\
    ...     .map_method('lower')\
    ...     .count_frequency().to_list()
    [('e', 2), ('g', 2), ('h', 1), ('l', 2), ('o', 3), ('s', 1), ('r', 1), ('u', 1)]

Overview
========

Pipelining
----------

DataStreams are perfect for pipelined feature calculation:

.. code-block:: python

    def calc_name(user):
        user.first_name = user.name.split(' ')[0] if user.name else ''
        return user

    def calc_age(user):
       user.age = datetime.now() - user.birthday
       return user

    DataStream(users)\
        .map(calc_name)\
        .map(calc_age)\
        .for_each(User.save)\
        .execute() # <- Nothing happens till here


Theses calculations are efficient because streams are generators by default, and no memory is wasted on unnecessary intermediary collections.

This is even better when certain calculation steps are long, and must be wrapped in their own functions.  Of course, for brevity, you can use `set`:

.. code-block:: python

    DataStream(users)\
        .set('first_name', lambda user: user.name.split(' ')[0] if user.name else '')\
        .set('age', lambda user: datetime.now() - user.birthday)\
        .for_each(User.save)\
        .execute()


Joins
-----

You can join :py:class:`DataStream` - even streams of objects!

.. code-block:: python

    userstream = DataStream(users)
    transactstream = DataStream(transactions)

    user_spend = userstream.join('inner', 'user_id', transactstream)\
        .group_by('user_id')\
        .map(lambda usertrans: (usertrans[0], sum(tran.price for tran in usertrans[1])))\
        .to_dict()

    # {'47328129': 240.95, '48190234': 40.73, ...}



``where`` Clause
----------------

Chained ``filter`` s are a bit tiresome. ``where`` lets you perform simple filtering using more accessible language:

.. code-block:: python

    DataStream(users)\
        .where('age').gteq(18)\
        .where('age').lt(35)\
        .where('segment').is_in(target_segments)\
        .for_each(do_something).execute()

Instead of:

.. code-block:: python

    DataStream(users)\
        .filter(lambda user: user.age >= 18)\
        .filter(lambda user: user.age < 35)\
        .filter(lambda user: user.segment in target_segments)\
        .for_each(do_something).execute()

I bet you got tired just *reading* that many lambdas!

Apache Spark Integration
------------------------

Integrating with Apache Spark is easy - just use ``RddStream`` instead of ``DataStream`` or ``DataSet``, and pass it an RDD.  The rest of the API is the same!

.. code-block:: python

    RddStream(myrdd)\
        .where('age').gteq(18)\
        .where('age').lt(35)\
        .where('segment').is_in(target_segments)\
        .for_each(do_something).execute()

DataStream API
==============

.. py:module:: datastreams

.. autoclass:: DataStream
    :members:

DataSet API
===========

.. autoclass:: DataSet
    :members:


Etc
===

Check it out on github_!

This project is licensed under the MIT license.

.. _github: https://github.com/StuartAxelOwen/datastreams!


