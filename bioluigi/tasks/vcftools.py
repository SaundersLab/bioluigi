from luigi import LocalTarget
from ..slurm import SlurmExecutableTask
from ..utils import CheckTargetNonEmpty


class VCFIndex(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "tgac-short"

    def output(self):
        return LocalTarget(self.input().path + '.tbi')

    def work_script(self):
        return '''#!/bin/bash
                    source vcftools-0.1.13
                    set -euo pipefail

                    tabix -p vcf {vcf}

        '''.format(vcf=self.input().path)
