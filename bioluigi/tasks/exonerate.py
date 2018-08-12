import os
import math
import luigi
from luigi import LocalTarget, Parameter


from ..slurm import SlurmExecutableTask
from ..utils import CheckTargetNonEmpty


class Protein2Genome(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "nbi-short"

    def work_script(self):
        return '''#!/bin/bash
                  source exonerate-2.2.0;
                  set -euo  pipefail

                  exonerate --model protein2genome {query} {target}  > {output}.temp

                  mv {output}.temp {output}
        '''.format(target=self.input()['target'].path,
                   query=self.input()['query'].path)
