import luigi
from luigi import LocalTarget

from ..slurm import SlurmExecutableTask
from ..utils import CheckTargetNonEmpty


class BWAIndex(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "tgac-short"

    def output(self):
        return LocalTarget(self.input().path + '.bwt')

    def work_script(self):
        return '''#!/bin/bash
                    source bwa-0.7.13
                    set -euo pipefail

                    bwa index {fasta}

        '''.format(fasta=self.input().path)


class BWAMap(CheckTargetNonEmpty, SlurmExecutableTask):
    '''Runs BWAmem.
       Requires a dict with 'contig' pointing to the .bwt file
       of the BWA index and 'reads' pointing to the reads.
       Output is one unsorted BAM file'''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1000
        self.n_cpu = 4
        self.partition = "nbi-medium"

    def output(self):
        raise NotImplementedError("Output needs to be defined in the subclass")

    def work_script(self):
        if isinstance(self.input()['reads'], luigi.LocalTarget):
            reads = self.input()['reads'].path
        else:
            reads = ' '.join([x.path for x in self.input()['reads']])
        return '''#!/bin/bash
                    source bwa-0.7.13
                    source samtools-1.4
                    set -euo pipefail

                    bwa mem -t {n_cpu} {contigs} {reads} |
                    samtools view -Sb - > {output}.temp

                    mv {output}.temp {output}
        '''.format(contigs=self.input()['contigs'].path[:-4],
                   n_cpu=self.n_cpu - 1,
                   reads=reads,
                   output=self.output().path)
