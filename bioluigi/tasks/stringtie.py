from luigi import LocalTarget
from ..slurm import SlurmExecutableTask
from ..utils import CheckTargetNonEmpty, get_ext
from luigi.file import TemporaryFile


class StringTie(SlurmExecutableTask, CheckTargetNonEmpty):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 2000
        self.n_cpu = 4
        self.partition = "nbi-medium"

    def output(self):
        return LocalTarget(get_ext(self.input().path)[0] + '.stringtie.gtf')

    def work_script(self):
        return '''#!/bin/bash
        source stringtie-1.3.0;
        set -euo pipefail

        stringtie {input} -p {n_cpu} > {output}.temp

        mv {output}.temp {output}
        '''.format(input=self.input().path,
                   output=self.output().path,
                   n_cpu=self.n_cpu)


class StringTieMerge(SlurmExecutableTask, CheckTargetNonEmpty):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 2000
        self.n_cpu = 4
        self.partition = "nbi-medium"

    def output(self):
        return LocalTarget('stringtie.gtf')

    def work_script(self):
        self.temp = TemporaryFile()
        return '''#!/bin/bash
        source stringtie-1.3.0;
        set -euo pipefail

        echo '{input}' > {temp}
        stringtie  -p {n_cpu} --merge {temp} > {output}.temp

        mv {output}.temp {output}
        '''.format(input="\n".join([x.path for x in self.input()]),
                   output=self.output().path,
                   temp=self.temp.path,
                   n_cpu=self.n_cpu)
