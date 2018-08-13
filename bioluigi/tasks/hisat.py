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
        self.n_cpu = 7
        self.partition = "nbi-medium,RG-Diane-Saunders"
        self.dta_cufflinks = False
        self.rg = None

    def output(self):
        output = get_ext(self.input['reads'][0])
        output = output[:-3] if output[-3:] == '_R1' else output
        return {
            'bam': LocalTarget(output + '.bam'),
            'log': LocalTarget(output + '.hisat.log')
        }

    def work_script(self):
        rg_commands = ''
        if self.rg:
            for k, v in self.rg.items():
                if k == 'ID':
                    rg_commands += '--rg-id {0} \\\n'.format(v)
                else:
                    rg_commands += '--rg {0}:{1} \\\n'.format(k, v)

        return '''#!/bin/bash
                  source HISAT-2.1.0;
                  source samtools-1.7;
                  rm {bam}.samsort*
                  set -euo pipefail

                  hisat2 -t -p {n_cpu} \
                         -x {hisat_genome} \
                         -1 {R1} -2 {R2}  \
                         --new-summary \
                         {dta_cufflinks} \
                         {rg_commands}  \
                         --summary-file {log}.temp |
                         samtools view -bS - |
                         samtools sort -m 2G - --threads 2 -T {bam}.samsort > {bam}.temp

                  mv {log}.temp {log}
                  dt=$(date '+%d/%m/%Y %H:%M:%S');
                  echo "\ndatetime : $dt" >> {log}
                  mv {bam}.temp {bam}

                  '''.format(dta_cufflinks='--dta-cufflinks' if self.dta_cufflinks else '',
                             bam=self.output()['bam'].path,
                             log=self.output()['log'].path,
                             hisat_genome=self.input()['genome'].path,
                             n_cpu=self.n_cpu-3,
                             rg_commands=rg_commands,
                             R1=self.input()['reads'][0].path,
                             R2=self.input()['reads'][1].path,)
