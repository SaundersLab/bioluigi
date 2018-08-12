import os
import luigi
from luigi import LocalTarget, Parameter
from luigi.file import TemporaryFile

from ..slurm import SlurmExecutableTask
from ..utils import CheckTargetNonEmpty
from ..decorators import inherits, requires


class MikadoConfigure(SlurmExecutableTask, CheckTargetNonEmpty):

    blast_db = Parameter()
    base_dir = Parameter()
    scratch_dir = Parameter()
    reference = Parameter()
    data = luigi.DictParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1000
        self.n_cpu = 1
        self.partition = "nbi-short"

    def output(self):
        return LocalTarget(os.path.join(self.base_dir, 'mikado', 'configuration.yaml'))

    def list(self):
        print(self)
        '''Mikado requires a table of the transciptome assemblies to use.
           Construct this from the task input'''
        l = []
        for k, v in self.data.items():
            if k == 'cufflinks':
                l.append(v + "\tcu\tTrue")
            elif k == 'stringtie':
                l.append(v + "\tst\tTrue")
            elif k == 'class':
                l.append(v + "\tcl\tTrue")
            elif k == 'trinity':
                l.append(v + "\ttr\tFalse")
        return "\n".join(l)

    def work_script(self):
        self.temp = TemporaryFile()
        return '''#!/bin/bash
                   source /usr/users/JIC_a1/buntingd/GenomeAnnotation/annotation/bin/activate
                   set -euo pipefail

                   echo '{list}' > {temp}

                   mikado configure --list {temp} \
                                    --reference {reference} \
                                    --mode permissive \
                                    --scoring plants.yaml  \
                                    --junctions {portcullis} \
                                    -bt {db} \
                                    {output}.temp
                mv {output}.temp {output}
                '''.format(list=self.list(),
                           temp=self.temp.path,
                           reference=self.reference,
                           portcullis=os.path.join(self.data['portcullis'], 'portcullis.pass.junctions.bed'),
                           db=self.blast_db,
                           output=self.output().path)


@requires(MikadoConfigure)
class MikadoPrepare(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "nbi-short"

    def output(self):
        return {'gtf': LocalTarget(os.path.join(self.base_dir, 'mikado', 'mikado_prepared.gtf')),
                'fasta': LocalTarget(os.path.join(self.base_dir, 'mikado', 'mikado_prepared.fasta'))}

    def work_script(self):
        return '''#!/bin/bash
                  source /usr/users/JIC_a1/buntingd/GenomeAnnotation/annotation/bin/activate
                  set -euo pipefail

                  mikado prepare  --strip_cds  -o {gtf}.temp -of {fasta}.temp --json-conf {conf} --log /dev/stderr

                  mv {gtf}.temp {gtf}
                  mv {fasta}.temp {fasta}
                '''.format(gtf=self.output()['gtf'].path,
                           fasta=self.output()['fasta'].path,
                           conf=self.input().path)


@requires(MikadoPrepare)
class BLAST(SlurmExecutableTask, CheckTargetNonEmpty):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 750
        self.n_cpu = 12
        self.partition = "nbi-medium"

    def output(self):
        return LocalTarget(os.path.join(self.base_dir, 'mikado', 'blast.xml.gz'))

    def work_script(self):
        return '''#!/bin/bash
                  source ncbi_blast+-2.2.30;
                  set -euo pipefail

                  blastx -max_target_seqs 5 \
                         -num_threads {n_cpu} \
                         -query {input} \
                         -outfmt 5 \
                         -db {blast_db} \
                         -evalue 0.000001 | sed '/^$/d' | gzip -c  > {output}.temp

                mv {output}.temp {output}
               '''.format(n_cpu=self.n_cpu,
                          input=self.input()['fasta'].path,
                          blast_db=self.blast_db,
                          output=self.output().path)


@requires(MikadoPrepare)
class TransDecoder(SlurmExecutableTask, CheckTargetNonEmpty):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1000
        self.n_cpu = 4
        self.partition = "nbi-medium"

    def output(self):
        return LocalTarget(os.path.join(self.base_dir, 'mikado', 'orfs.bed'))

    def work_script(self):
        return '''#!/bin/bash
                  source transdecoder-3.0.0
                  set -euo pipefail
                  mkdir -p {scratch}/transdecoder
                  cd {scratch}/transdecoder

                  TransDecoder.LongOrfs -t {input} 2> lo_log.txt
                  TransDecoder.Predict -t {input} --cpu {n_cpu}  2> pred_log.txt

                   mv {scratch}/transdecoder/{prefix}.transdecoder.bed {output}
               '''.format(input=self.input()['fasta'].path,
                          output=self.output().path,
                          prefix=os.path.split(self.input()['fasta'].path)[1],
                          scratch=os.path.join(self.scratch_dir, 'mikado', 'transdecoder'),
                          n_cpu=self.n_cpu)


@requires(orfs=TransDecoder, blast=BLAST, conf=MikadoConfigure)
class MikadoSerialise(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "nbi-medium"

    def output(self):
        return LocalTarget(os.path.join(self.base_dir, 'mikado', 'mikado.db'))

    def work_script(self):
        return '''#!/bin/bash
                  source /usr/users/JIC_a1/buntingd/GenomeAnnotation/annotation/bin/activate
                  rm {output}.temp
                  set -euo pipefail
                  cd {output_dir}

                  mikado serialise  --orfs {orfs} \
                                    --xml {blast} \
                                    --blast_targets {blast_db} \
                                    --json-conf {conf} \
                                    {output}.temp

                  mv {output}.temp {output}
                '''.format(output_dir=os.path.split(self.output().path)[0],
                           orfs=self.input()['orfs'].path,
                           blast=self.input()['blast'].path,
                           output=self.output().path,
                           conf=self.input()['conf'].path,
                           blast_db=self.blast_db)


@requires(db=MikadoSerialise, prep=MikadoPrepare, conf=MikadoConfigure)
class MikadoPick(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1500
        self.n_cpu = 8
        self.partition = "nbi-medium"

    def requires(self):
        return {'conf': self.clone(MikadoConfigure),
                'db': self.clone(MikadoSerialise),
                'prep': self.clone(MikadoPrepare)}

    def output(self):
        return LocalTarget(os.path.join(self.base_dir, 'mikado', 'mikado.gff'))

    def work_script(self):
        return '''#!/bin/bash
                  source /usr/users/JIC_a1/buntingd/GenomeAnnotation/annotation/bin/activate
                  set -euo pipefail
                  cd {output_dir}

                  mikado pick -p {n_cpu} --json-conf {conf} -db {db} --loci_out {output}.temp

                  mv {output}.temp.gff3 {output}
                '''.format(n_cpu=self.n_cpu,
                           output_dir=os.path.split(self.output().path)[0],
                           gff=self.input()['prep']['gtf'].path,
                           db=self.input()['db'].path,
                           output=self.output().path,
                           conf=self.input()['conf'].path)


@inherits(MikadoConfigure, MikadoPrepare, BLAST,
          TransDecoder, MikadoSerialise, MikadoPick)
class Mikado(luigi.Task):
    base_dir = Parameter()
    scratch_dir = Parameter()
    data = None

    def run(self):

        yield self.clone(MikadoConfigure, data={k: v.path for k, v in self.input().items()})
        yield self.clone(MikadoPrepare)
        yield self.clone(BLAST)
        yield self.clone(TransDecoder)
        yield self.clone(MikadoSerialise)
        yield self.clone(MikadoPick)

    def output(self):
        return LocalTarget(os.path.join(self.base_dir, 'mikado', 'mikado.gff'))


class MikadoCompare(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1500
        self.n_cpu = 1
        self.partition = "nbi-short"

    def output(self):
        return LocalTarget(os.path.join(self.base_dir, VERSION, PIPELINE, self.output_prefix, 'mikado', 'mikado.gff'))

    def work_script(self):
        return '''#!/bin/bash
                  {python}
                  set -euo pipefail
                  cd {output_dir}

                  mikado pick -p {n_cpu} --json-conf {conf} -db {db} --loci_out {output}.temp --log /dev/stderr

                  mv {output}.temp.gff3 {output}
                '''.format(python=utils.python,
                           n_cpu=self.n_cpu,
                           output_dir=os.path.split(self.output().path)[0],
                           gff=self.input()['prep']['gtf'].path,
                           db=self.input()['db'].path,
                           output=self.output().path,
                           conf=self.input()['conf'].path)
