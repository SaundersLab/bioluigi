import unittest
import luigi
import os

from bioluigi.notebook import NotebookTask

test_dir = os.path.split(__file__)[0]


class TestNB(NotebookTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.n_cpu = 1
        self.mem = 100
        self.partition = 'nbi-short'
        self.notebook = os.path.join(test_dir, 'notebooks', "test.ipynb")
        self.vars_dict = {"test": 'Hello World!'}

    def output(self):
        return luigi.LocalTarget(os.path.join(test_dir, 'scratch', "test.ipynb"))


class TestNotebookTask(unittest.TestCase):
    def test_Ok(self):
        task = TestNB()
        luigi.build([task], local_scheduler=True)
