from luigi import LocalTarget

import os
from ..slurm import SlurmExecutableTask
from ..utils import CheckTargetNonEmpty

trimmomatic_path = "java -XX:+UseSerialGC -Xmx{mem}M -jar /tgac/software/testing/trimmomatic/0.36/x86_64/bin/trimmomatic-0.36.jar "


class Trimmomatic(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 2000
        self.n_cpu = 4
        self.partition = "nbi-medium"

    def output(self):
        return [LocalTarget(os.path.join(os.path.dirname(self.input()[0].path), self.library + "_filtered_R1.fastq.gz")),
                LocalTarget(os.path.join(os.path.dirname(self.input()[0].path), self.library + "_filtered_R2.fastq.gz")),
                LocalTarget(os.path.join(os.path.dirname(self.input()[0].path), self.library + "_trimmomatic.txt"))]

    def work_script(self):
        return '''#!/bin/bash
               source jre-8u92
               source trimmomatic-0.30
               set -euo pipefail

               cd {scratch_dir}
               trimmomatic='{trimmomatic}'
               $trimmomatic PE -threads 4 {R1_in} {R2_in} -baseout temp.fastq.gz \
               ILLUMINACLIP:{adapters}:2:30:10:4 SLIDINGWINDOW:4:20 MINLEN:50 \
               2>&1 | sed 's/raw_R1.fastq.gz/{library}.fastq.gz/' > {log}.temp

               mv temp_1P.fastq.gz {R1_out}
               mv temp_2P.fastq.gz {R2_out}
               mv {log}.temp {log}

                '''.format(scratch_dir=os.path.dirname(self.output()[0].path),
                           trimmomatic=trimmomatic_path.format(
                               mem=self.mem * self.n_cpu),
                           log=self.output()[2].path,
                           library=self.library,
                           R1_in=self.input()[0].path,
                           R2_in=self.input()[1].path,
                           adapters='/tgac/software/testing/trimmomatic/0.30/x86_64/bin/adapters/TruSeq.cat.fa',
                           R1_out=self.output()[0].path,
                           R2_out=self.output()[1].path)
