from luigi import LocalTarget

from ..slurm import SlurmExecutableTask
from ..utils import CheckTargetNonEmpty, get_ext
import os


class BuildDatabase(CheckTargetNonEmpty, SlurmExecutableTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 4000
        self.n_cpu = 1
        self.partition = "nbi-medium,RG-Diane-Saunders"
        self.name = get_ext(os.path.basename(self.input().path))[0]

    def output(self):
        return LocalTarget(os.path.join(os.path.dirname(self.input().path), 'repeatmodeler', self.name))

    def work_script(self):
        return '''#!/bin/bash
                  source repeatmodeller-1.0.8;
                  mkdir -p {output}
                  set -euo pipefail

                  cd {output}
                  BuildDatabase -name {name} -engine ncbi {fasta}

                  echo "done" > {output}/{name}
                  '''.format(fasta=self.input().path,
                             name=self.name,
                             output=os.path.split(self.output().path)[0])


class RepeatModeler(CheckTargetNonEmpty, SlurmExecutableTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 2000
        self.n_cpu = 12
        self.partition = "nbi-medium,RG-Diane-Saunders"

    def output(self):
        return [LocalTarget(self.input().path + "-families.fa"),
                LocalTarget(self.input().path + "-families.stk")]

    def work_script(self):
        return '''#!/bin/bash
                  source repeatmodeller-1.0.8;
                  mkdir -p {output_dir}_temp
                  set -euo pipefail

                  cd {output_dir}_temp
                  RepeatModeler -engine ncbi -pa {n_cpu} -database {input}

                  '''.format(input=self.input().path,
                             output_dir=os.path.join(os.path.dirname(self.output()[0].path), 'RM_run'),
                             n_cpu=self.n_cpu-1)


class RepeatMasker(CheckTargetNonEmpty, SlurmExecutableTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 2000
        self.n_cpu = 12
        self.partition = "nbi-medium,RG-Diane-Saunders"

    def output(self):
        return LocalTarget(os.path.join(os.path.dirname(self.input().path), 'repeatmasker', os.path.basename(self.input().path) + ".masked"))

    def work_script(self):
        return '''#!/bin/bash
                  source repeatmasker-4.0.7;
                  mkdir -p {output_dir}_temp
                  set -euo pipefail

                  RepeatMasker -engine ncbi \
                                -pa {n_cpu}  \
                                -xsmall \
                                -species fungi  \
                                -dir {output_dir}_temp \
                                {input}

                  mv {output_dir}_temp {output_dir}
                  '''.format(input=self.input().path,
                             #repeat_modeler=self.input()['repeat_modeler'].path,
                             output_dir=self.output().path,
                             n_cpu=self.n_cpu)
