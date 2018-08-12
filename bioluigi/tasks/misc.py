import luigi
from luigi import LocalTarget

from ..slurm import SlurmExecutableTask
from ..utils import value_it, structure_apply, CheckTargetNonEmpty, get_ext


class GTFtoGFF3(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1000
        self.n_cpu = 1
        self.partition = "nbi-short"

    def output(self):
        return LocalTarget(get_ext(self.input().path)[0] + ".gff3")

    def work_script(self):
        return '''#!/bin/bash -e
                  source gffread-0.9.8
                  set -euo pipefail

                  gffread {input} -o- | grep -ve'#' > {output}.temp

                  mv {output}.temp {output}
                 '''.format(input=self.input().path,
                            output=self.output().path)


class ReferenceFasta(luigi.ExternalTask):
    reference = luigi.Parameter()

    def output(self):
        return LocalTarget(self.reference)


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


class Concatenate(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1000
        self.n_cpu = 1
        self.partition = "nbi-short"

    def work_script(self):
        return '''#!/bin/bash -e
                  set -euo pipefail

                  cat {inputs} > {output}.temp

                  mv {output}.temp {output}
                 '''.format(output=self.output().path,
                            inputs=' '.join([x.path for x in self.input()]))

