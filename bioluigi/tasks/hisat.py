from luigi import LocalTarget

from ..slurm import SlurmExecutableTask
from ..utils import CheckTargetNonEmpty, get_ext
import os


class HISATIndexGenome(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 16000
        self.n_cpu = 2
        self.partition = "nbi-short"
        self.name = get_ext(os.path.basename(self.input().path))[0]

    def output(self):
        return LocalTarget(os.path.join(os.path.dirname(self.input().path), 'HISAT', self.name))

    def work_script(self):
        return '''#!/bin/bash
                  source HISAT-2.1.0;
                  set -euo pipefail

                  hisat2-build  {reference} {output}/{name} \
                                -p {n_cpu} \

                  echo "done" > {output}/{name}
                '''.format(n_cpu=self.n_cpu,
                           reference=self.reference,
                           output=os.path.split(self.output().path)[0],
                           name=self.name)


class HISAT(CheckTargetNonEmpty, SlurmExecutableTask):
    '''Runs HISAT2 to align to the reference '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 3000
        self.n_cpu = 4
        self.partition = "nbi-medium,RG-Diane-Saunders"
        self.dta_cufflinks = False

    def output(self):
        output = get_ext(self.input['reads'][0])
        output = output[:-3] if output[-3:] == '_R1' else output
        return {
            'hisat_bam': LocalTarget(output + '.bam'),
            'hisat_log': LocalTarget(output + '.hisat.log')
        }

    def work_script(self):
        return '''#!/bin/bash
                  source HISAT-2.1.0;
                  source samtools-1.7;
                  set -euo pipefail

                  hisat2 -t -p {n_cpu} \
                         -x {hisat_genome} \
                         -1 {R1} -2 {R2}  \
                         --new-summary \
                         {dta_cufflinks} \
                         --summary-file {hisat_log}.temp | samtools view -bS - > {hisat_bam}.temp

                  mv {hisat_log}.temp {hisat_log}
                  dt=$(date '+%d/%m/%Y %H:%M:%S');
                  echo "\ndatetime : $dt" >> {hisat_log}
                  mv {hisat_bam}.temp {hisat_bam}

                  '''.format(dta_cufflinks='--dta-cufflinks' if self.dta_cufflinks else '',
                             hisat_bam=self.output()['hisat_bam'].path,
                             hisat_log=self.output()['hisat_log'].path,
                             hisat_genome=self.input()['genome'].path,
                             n_cpu=self.n_cpu-1,
                             R1=self.input()['reads'][0].path,
                             R2=self.input()['reads'][1].path,)
