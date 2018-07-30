from luigi import LocalTarget, Parameter

from ..slurm import SlurmExecutableTask
from ..utils import CheckTargetNonEmpty, get_ext
import os


class AddGenome(CheckTargetNonEmpty, SlurmExecutableTask):
    jbrowse_dir = Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "nbi-short"
        self.name = get_ext(os.path.basename(self.input().path))[0]

    def output(self):
        return LocalTarget(os.path.join(self.jbrowse_dir, "data", self.name))

    def complete(self):
        return os.path.exists(os.path.join(self.output().path, 'seq'))

    def work_script(self):
        return '''#!/bin/bash
                  source perl_activeperl-5.18.2.1802
                  source berkeleydb-4.3;
                  set -euo  pipefail

                  cd {jbrowse_dir}
                  bin/prepare-refseqs.pl --fasta {input} \
                                         --out {output_dir} \
                                         --trackConfig '{{"dataset_id": "{name}"}}'

                '''.format(output_dir=self.output().path,
                           input=self.input().path,
                           jbrowse_dir=self.jbrowse_dir,
                           name=self.name)


class AddGFF(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "nbi-short"
        self.name = get_ext(os.path.basename(self.input()['gff'].path))[0]

    def output(self):
        return LocalTarget(os.path.join(self.input()['genome'].path, 'tracks', self.name))

    def work_script(self):
        return '''#!/bin/bash
                  source perl_activeperl-5.18.2.1802
                  source berkeleydb-4.3;
                  set -euo  pipefail

                  cd {jbrowse_dir}
                  bin/flatfile-to-json.pl  --gff {gff} \
                                           --out {genome} \
                                           --trackLabel {name} \
                                           --trackType CanvasFeatures \
                                           --type gene,mRNA,exon,CDS

                '''.format(genome=self.input()['genome'].path,
                           gff=self.input()['gff'].path,
                           jbrowse_dir=self.jbrowse_dir,
                           name=self.name)
