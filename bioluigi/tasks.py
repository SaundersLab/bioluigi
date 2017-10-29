import luigi
from luigi import LocalTarget

from .slurm import SlurmExecutableTask
from .utils import value_it, structure_apply, CheckTargetNonEmpty, get_ext


class FetchFastqGZ(CheckTargetNonEmpty, SlurmExecutableTask):
    '''Fetches and concatenate the fastq.gz files for ``library`` from the /reads/ server
     :param str library: library name

     Set output to a list [R1, R2]'''

    library = luigi.Parameter()
    read_dir = luigi.Parameter(default="/tgac/data/reads/*DianeSaunders*", significant=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1000
        self.n_cpu = 1
        self.partition = "nbi-short"

    def work_script(self):
        return '''#!/bin/bash -e
                  set -euo pipefail

                  find {read_dir} -name "*{library}*_R1.fastq.gz" -type f  -print | sort | xargs cat  > {R1}.temp
                  find {read_dir} -name "*{library}*_R2.fastq.gz" -type f  -print | sort | xargs cat  > {R2}.temp

                  mv {R1}.temp {R1}
                  mv {R2}.temp {R2}
                 '''.format(read_dir=self.read_dir,
                            library=self.library,
                            R1=self.output()[0].path,
                            R2=self.output()[1].path)


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


class VCFIndex(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "tgac-short"

    def output(self):
        return LocalTarget(self.input().path + '.tbi')

    def work_script(self):
        return '''#!/bin/bash
                    source vcftools-0.1.13
                    set -euo pipefail

                    tabix -p vcf {vcf}

        '''.format(vcf=self.input().path)


class Unzip(CheckTargetNonEmpty, SlurmExecutableTask):

        @staticmethod
        def drop_gz(path):
            if not path[-3] == '.gz':
                raise Exception('File is not compressed!')
            return path[:-3]

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # Set the SLURM request params for this task
            self.mem = 3000
            self.n_cpu = 1
            self.partition = "tgac-medium"

        def output(self):
            return structure_apply(lambda x: LocalTarget(self.drop_gz(x.path)),
                                   self.input())

        def work_script(self):
            in_it = value_it(self.input())
            out_it = value_it(self.output()).values()

            gzip = ["gzip -cd < {} > {}.temp".format(x.path, y.path)
                    for x, y in zip(in_it, out_it)]
            mv = ["mv {}.temp {}".format(x.path, x.path)
                  for x, y in out_it]
            return ('''#!/bin/bash
                    set -euo pipefail''' +
                    '\n'.join(gzip) + '\n' +
                    '\n'.join(mv))


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


class SamtoolsSort(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "nbi-medium"

    def output(self):
        return LocalTarget(get_ext(self.input().path)[0] + '_sorted.bam')

    def work_script(self):
        return '''#!/bin/bash
               source samtools-1.4
               set -euo pipefail

               samtools sort --output-fmt BAM -o {output}.temp {input}

               mv {output}.temp {output}
                '''.format(input=self.input().path,
                           output=self.output().path)


class SamtoolsIndex(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "nbi-short"

    def output(self):
        return LocalTarget(get_ext(self.input().path)[0] + '.bai')

    def work_script(self):
        return '''#!/bin/bash
               source samtools-1.4
               set -euo pipefail

               samtools index {input} {output}.temp

               mv {output}.temp {output}
                '''.format(input=self.input().path,
                           output=self.output().path)


class SamtoolsSortName(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "nbi-medium"

    def output(self):
        return LocalTarget(get_ext(self.input().path)[0] + '_namesorted.bam')

    def work_script(self):
        return '''#!/bin/bash
               source samtools-1.4
               set -euo pipefail

               samtools sort -n --output-fmt BAM -o {output}.temp {input}

               mv {output}.temp {output}
                '''.format(input=self.input().path,
                           output=self.output().path)
