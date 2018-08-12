import os
from luigi import LocalTarget

from ..slurm import SlurmExecutableTask
from ..utils import CheckTargetNonEmpty, get_ext


class FastqDump(SlurmExecutableTask, CheckTargetNonEmpty):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 2000
        self.n_cpu = 10
        self.partition = 'nbi-medium'

    def output(self):
        return [LocalTarget(os.path.join(os.path.dirname(self.input().path), get_ext(self.input().path)[0] + "_1.fastq.gz")),
                LocalTarget(os.path.join(os.path.dirname(self.input().path), get_ext(self.input().path)[0] + "_2.fastq.gz"))]

    def work_script(self):
        return '''#!/bin/bash
        source sra-tools-2.8.2
        mkdir -p {output}/temp
        set -euo pipefail

        fastq-dump -I --split-files --gzip {input} --outdir {output}/temp

        mv {output}/temp/* {output}
        '''.format(input=self.input().path,
                   output=os.path.dirname(self.output()[0].path))
