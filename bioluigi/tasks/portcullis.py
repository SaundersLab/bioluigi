import os
from luigi import LocalTarget, Parameter
from luigi.file import TemporaryFile

from ..slurm import SlurmExecutableTask


class PortcullisPrep(SlurmExecutableTask):

    reference = Parameter()
    scratch_dir = Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 16000
        self.n_cpu = 1
        self.partition = "nbi-medium"

    def output(self):
        return LocalTarget(os.path.join(self.scratch_dir, self.task_id))

    def work_script(self):
        return '''#!/bin/bash
        source portcullis-1.1.0
        set -euo pipefail

        portcullis prep --output {output}_temp {reference} {input}

        mv {output}_temp {output}
        '''.format(input=self.input().path,
                   reference=self.reference,
                   output=self.output().path)


class PortcullisJunc(SlurmExecutableTask):
    scratch_dir = Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 6000
        self.n_cpu = 4
        self.partition = "nbi-medium"

    def output(self):
        return LocalTarget(os.path.join(self.scratch_dir, self.task_id))

    def work_script(self):
        return '''#!/bin/bash
        source portcullis-1.1.0
        set -euo pipefail

        portcullis junc --output {output}_temp/portcullis \
                        --orientation FR \
                        --strandedness unstranded \
                        --threads {n_cpu} \
                        {input}

        mv {output}_temp {output}
        '''.format(input=self.input().path,
                   output=self.output().path,
                   n_cpu=self.n_cpu)


class PortcullisFilter(SlurmExecutableTask):
    reference = Parameter()
    scratch_dir = Parameter()
    base_dir = Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "nbi-medium"

    def requires(self):
        return {'prep': self.clone(PortcullisPrep),
                'junc': self.clone(PortcullisJunc)}

    def output(self):
        return LocalTarget(os.path.join(self.base_dir, 'portcullis_filter'))

    def work_script(self):
        self.temp = TemporaryFile()
        return '''#!/bin/bash
        source portcullis-1.1.0
        set -euo pipefail

        portcullis filter --output {output}_temp/portcullis \
                          --intron_gff \
                          --threshold 0.85 \
                          {prep} {tab}

        mv {output}_temp {output}
        '''.format(prep=self.input()['prep'].path,
                   tab=os.path.join(self.input()['junc'].path, 'portcullis.junctions.tab'),
                   output=self.output().path)
