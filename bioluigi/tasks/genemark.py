from luigi import LocalTarget, Parameter

from ..slurm import SlurmExecutableTask
from ..utils import CheckTargetNonEmpty
import os


class GenemarkESTrain(CheckTargetNonEmpty, SlurmExecutableTask):
    base_dir = Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 2000
        self.n_cpu = 8
        self.partition = "nbi-medium,RG-Diane-Saunders"

    def output(self):
        return LocalTarget(os.path.join(self.base_dir, 'genemark', 'run', 'ES_C_4', 'ES_C_4.mod'))

    def work_script(self):
        return '''#!/bin/bash
                  source genemark-4.33_ES_ET;
                  mkdir -p {output_dir}_temp
                  set -euo  pipefail

                  cd {output_dir}_temp;
                  gmes_petap.pl --ES \
                                --fungus \
                                --cores {n_cpu} \
                                -v \
                                --sequence {input}

                mv -T {output_dir}_temp {output_dir}
                '''.format(output_dir=os.path.join(self.base_dir, 'genemark'),
                           input=self.input().path,
                           n_cpu=self.n_cpu)
