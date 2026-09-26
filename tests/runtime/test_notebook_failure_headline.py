"""Root-cause headline extraction from failed papermill output (#297)."""

from __future__ import annotations

from ginkgo.runtime.task_runners.notebook import notebook_failure_headline

_CELL_FAILURE_OUTPUT = """\
Input Notebook:  analysis.ipynb
Output Notebook: task_0007.ipynb
Executing:  70%|███████████████         | 7/10 [00:02<00:00,  3.33cell/s]
Traceback (most recent call last):
  File "/opt/env/lib/python3.11/site-packages/papermill/execute.py", line 131, in execute_notebook
    raise_for_execution_errors(nb, output_path)
  File "/opt/env/lib/python3.11/site-packages/papermill/execute.py", line 251, in raise_for_execution_errors
    raise error
papermill.exceptions.PapermillExecutionError:
---------------------------------------------------------------------------
An Exception was encountered at "In [7]":
---------------------------------------------------------------------------
KeyError                                 Traceback (most recent call last)
Cell In[7], line 3
      1 df = compute_stats()
      2 print(df.columns)
----> 3 slope = df['slope_per_year']

File /opt/env/lib/python3.11/site-packages/pandas/core/frame.py:4102, in DataFrame.__getitem__(self, key)
   4100     if is_iterator(key):
   4101         key = list(key)
-> 4102     indexer = self.columns.get_loc(key)

KeyError: 'slope_per_year'
"""

_NO_CELL_FAILURE_OUTPUT = """\
Input Notebook:  broken.ipynb
Output Notebook: task_0003.ipynb
Traceback (most recent call last):
  File "/opt/env/lib/python3.11/site-packages/papermill/cli.py", line 90, in papermill
    execute_notebook(
  File "/opt/env/lib/python3.11/site-packages/papermill/execute.py", line 75, in execute_notebook
    nb = load_notebook_node(notebook_path)
  File "/opt/env/lib/python3.11/site-packages/papermill/iorw.py", line 460, in load_notebook_node
    nb_up.metadata.papermill["language"] = language
ValueError: No language found in notebook and no override provided.
"""


def test_a_cell_failure_names_the_cell_and_the_terminal_exception_line() -> None:
    headline = notebook_failure_headline(_CELL_FAILURE_OUTPUT)

    assert headline == "papermill failed executing cell 7: KeyError: 'slope_per_year'"


def test_a_failure_before_any_cell_runs_falls_back_to_the_traceback_tail() -> None:
    headline = notebook_failure_headline(_NO_CELL_FAILURE_OUTPUT)

    assert headline == (
        "notebook failed: ValueError: No language found in notebook and no override provided."
    )


def test_blank_output_has_no_headline() -> None:
    assert notebook_failure_headline("") is None
    assert notebook_failure_headline("   \n\n  ") is None


def test_trailing_whitespace_around_lines_is_ignored() -> None:
    output = 'Exception encountered at "In [2]":  \n  ValueError: boom   \n'

    assert (
        notebook_failure_headline(output) == "papermill failed executing cell 2: ValueError: boom"
    )


def test_a_later_cell_marker_wins_over_an_earlier_one() -> None:
    output = (
        'Exception encountered at "In [1]":\n'
        "some earlier retry noise\n"
        'Exception encountered at "In [9]":\n'
        "RuntimeError: final failure"
    )

    assert (
        notebook_failure_headline(output)
        == "papermill failed executing cell 9: RuntimeError: final failure"
    )
