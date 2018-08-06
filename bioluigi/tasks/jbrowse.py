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
                  export PATH=../node-v8.11.3-linux-x64/bin:$PATH
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


class AddPortcullis(SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 2000
        self.n_cpu = 1
        self.partition = "nbi-short"

    def output(self):
        return LocalTarget(os.path.join(self.input()['genome'].path, 'tracks', 'portcullis.gz'))

    def work_script(self):
        return '''#!/bin/bash
                  source perl_activeperl-5.18.2.1802
                  source berkeleydb-4.3;
                  export PATH=../node-v8.11.3-linux-x64/bin:$PATH
                  source portcullis-1.1.0
                  source vcftools-0.1.13
                  source bedtools-2.17.0
                  set -euo  pipefail

                  cd {portcullis_dir}
                  junctools convert -if bed -of ibed  -o portcullis.pass.junctions.ibed {input}
                  bedtools sort -i portcullis.pass.junctions.ibed | bgzip -c >  portcullis.pass.junctions.sorted.ibed.gz
                  tabix -p bed portcullis.pass.junctions.sorted.ibed.gz

                  cp portcullis.pass.junctions.sorted.ibed.gz {genome}/tracks/portcullis.gz
                  cp portcullis.pass.junctions.sorted.ibed.gz.tbi {genome}/tracks/portcullis.gz.tbi
                '''.format(genome=self.input()['genome'].path,
                           portcullis_dir=self.input()['portcullis'].path,
                           input=os.path.join(self.input()['portcullis'].path, 'portcullis.pass.junctions.bed'))



