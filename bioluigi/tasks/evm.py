import os
import math
import luigi
from luigi import LocalTarget, Parameter

from ..slurm import SlurmExecutableTask
from ..utils import CheckTargetNonEmpty


class EVMProteinAligment(CheckTargetNonEmpty, SlurmExecutableTask):
    scratch_dir = Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 2000
        self.n_cpu = 8
        self.partition = "nbi-short"

    def work_script(self):
        return '''#!/bin/bash
                  source funannotate-1.4.2;
                  set -euo  pipefail

                  cd {scratch_dir}
                  export EVM_HOME=/nbi/software/testing/evm/1.1.1/x86_64/bin

                  funannotate util prot2genome \
                              --genome {genome} \
                              --proteins {proteins} \
                              --out {output}.temp\
                              --cpus  {n_cpu}

                  mv {output}.temp {output}
        '''.format(genome=self.input()['genome'].path,
                   proteins=self.input()['proteins'].path,
                   scratch_dir=self.scratch_dir,
                   n_cpu=self.n_cpu,
                   output=self.output().path)


class EVMConcatGFF(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "nbi-short"

    def work_script(self):
        return '''#!/bin/bash
                  set -euo  pipefail
                  cat {input} | sort -k1,1 -k4,4n  > {output}.temp
                  mv {output}.temp {output}
        '''.format(input=' '.join([x.path for x in self.input()]),
                   output=self.output().path)


class EVMWeights(CheckTargetNonEmpty, luigi.Task):
    base_dir = Parameter()

    def output(self):
        return LocalTarget(os.path.join(self.base_dir, 'EVM', 'weights.txt'))

    def run(self):
        with self.output().open('w') as fout:
            for w in self.weights:
                fout.write('\t'.join(w) + "\n")


class EVMPartition(CheckTargetNonEmpty, SlurmExecutableTask):
    base_dir = Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "nbi-short"

    def output(self):
        return LocalTarget(os.path.join(self.base_dir, 'EVM', 'EVMPartition', 'partitions_list.out'))

    def work_script(self):
        return '''#!/bin/bash
                  rm -r {output_dir}
                  mkdir -p {output_dir}
                  set -euo  pipefail

                  cd {output_dir}

                  EVM_HOME=/nbi/software/testing/evm/1.1.1/x86_64/bin

                  $EVM_HOME/EvmUtils/partition_EVM_inputs.pl \
                    --genome {reference} \
                    --gene_predictions {genes} \
                    {proteins} \
                    {transcripts} \
                    --segmentSize 500000 --overlapSize 50000 \
                    --partition_listing partitions_list.out
        '''.format(reference=self.reference,
                   genes=self.input()['genes'].path,
                   proteins='--protein_alignments ' + self.input()['proteins'].path if 'proteins' in self.input() else '',
                   transcripts='--transcript_alignments ' + self.input()['transcripts'].path if 'transcripts' in self.input() else '',
                   output_dir=os.path.dirname(self.output().path))


class EVMGenerate(CheckTargetNonEmpty, SlurmExecutableTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "nbi-short"

    def output(self):
        return LocalTarget(os.path.join(self.base_dir, 'EVM', 'commands.list'))

    def work_script(self):
        return '''#!/bin/bash
                  set -euo  pipefail
                  EVM_HOME=/nbi/software/testing/evm/1.1.1/x86_64/bin

                  $EVM_HOME/EvmUtils/write_EVM_commands.pl \
                    --weights {weights} \
                    --output_file_name evm.out  \
                    --partitions {partitions_list} \
                    --genome {reference} \
                    --gene_predictions {genes} \
                    {proteins} \
                    {transcripts} > {output}.temp


                  mv {output}.temp {output}

        '''.format(reference=self.reference,
                   genes=self.input()['genes'].path,
                   weights=self.input()['weights'].path,
                   partitions_list=self.input()['partitions_list'].path,
                   proteins='--protein_alignments ' + self.input()['proteins'].path if 'proteins' in self.input() else '',
                   transcripts='--transcript_alignments ' + self.input()['transcripts'].path if 'transcripts' in self.input() else '',
                   output=self.output().path)


class EVMscatter(luigi.Task):
    def run(self):
        with self.input().open() as fin:
            inp = fin.readlines()
        perfile = math.ceil(len(inp)/len(self.output()))
        for i, out in enumerate(self.output()):
            with out.open('w') as fout:
                fout.writelines(inp[i*perfile:(i+1)*perfile])


class EVMgather(SlurmExecutableTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "nbi-short"

    def work_script(self):
        return '''#!/bin/bash

                  set -euo  pipefail
                  EVM_HOME=/nbi/software/testing/evm/1.1.1/x86_64/bin


                  $EVM_HOME/EvmUtils/recombine_EVM_partial_outputs.pl \
                    --partitions {partitions_list} \
                    --output_file_name evm.out

                  $EVM_HOME/EvmUtils/convert_EVM_outputs_to_GFF3.pl  \
                      --partitions {partitions_list} \
                      --output evm.out  \
                      --genome {reference}

                  find {partition_dir} -regex ".*evm.out.gff3" -exec cat {{}} \; > {output}.temp

                  mv {output}.temp {output}
                  rm {inputs}
        '''.format(partitions_list=self.clone(EVMPartition).output().path,
                   partition_dir=os.path.dirname(self.clone(EVMPartition).output().path),
                   inputs=' '.join([x.path for x in self.input()]),
                   reference=self.reference,
                   output=self.output().path)


class EVMExecute(SlurmExecutableTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "nbi-medium"

    def output(self):
        return LocalTarget(os.path.join(self.base_dir, 'EVM', 'evm.gff'))

    def work_script(self):
        return '''#!/bin/bash
                  set -euo  pipefail
                  source {input}
                  echo 'Done' > {output}
                  '''.format(input=self.input().path,
                             output=self.output().path)
