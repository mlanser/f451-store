Installation
============

.. include:: ../README.rst
    :start-after: install-start
    :end-before: install-end

If you're using `Poetry <https://python-poetry.org/>`__ for dependency management, then you can add this module as follows to your project:

.. code:: console

   $ poetry add f451-store

Once installed, you can import the main ``Store`` module into your project as follows:

.. code-block::

    from f451_store.store import Store

    store = Store(<secrets>)
    store.save_data(<TBD>)

    data = store.get_data(<TBD>)

And while importing the main module usually is the best option for most use cases, it is also possible to import any of the sub-modules. The following example illustrates how you can import a specific sub-module. In this case only the ``<TBD>`` provider module is imported.

.. code-block::

    from src.f451_store.providers.<tbd> import <TBD>

    client = <TBD>(
                arg1="<TBD>",
                arg2="<TBD>",
                ...
            )
    response = client.save_data(<TBD>)
