"""Starter workflow modules.

Tasks are split across ``prep.py``, ``analysis.py`` and ``reporting.py``
because a task's cache identity covers the whole module it is defined in:
editing any task in a file invalidates the cached results of every task in
that file. Keep an expensive task in a module of its own.
"""
