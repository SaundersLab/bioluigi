import luigi
from luigi import LocalTarget, Task, WrapperTask
from luigi.file import TemporaryFile

from ..slurm import SlurmExecutableTask
from ..utils import CheckTargetNonEmpty, get_ext
import os


class MergeBam(SlurmExecutableTask, CheckTargetNonEmpty):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 16000
        self.n_cpu = 3
        self.partition = "nbi-medium"

    def work_script(self):
        self.temp = TemporaryFile()
        return '''#!/bin/bash
        source samtools-1.3;
        set -euo pipefail

        echo '{input}' > {temp}
        samtools merge -f  {output}.temp.bam -b {temp} --threads 2

        mv {output}.temp.bam {output}
        '''.format(input="\n".join([x.path for x in self.input()]),
                   output=self.output().path,
                   temp=self.temp.path)


class TrinityGG(SlurmExecutableTask, CheckTargetNonEmpty):

    scratch_dir = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 2000
        self.n_cpu = 10
        self.partition = 'RG-Diane-Saunders,nbi-long'

    def output(self):
        return LocalTarget(os.path.join(os.path.dirname(self.input().path), 'trinity', 'Trinity-GG.fasta'))

    def work_script(self):
        return '''#!/bin/bash
        source trinityrnaseq-2.3.2;
        source bowtie-2.2.6
        mkdir -p {scratch}
        set -euo pipefail

        cd {scratch}

        Trinity --genome_guided_bam {input} \
                --max_memory {mem}G \
                --genome_guided_max_intron 10000 \
                --output {scratch} \
                --jaccard_clip \
                --full_cleanup  \
                --CPU {n_cpu}

        mv {scratch}/Trinity-GG.fasta {output}
        '''.format(input=self.input().path,
                   output=self.output().path,
                   n_cpu=self.n_cpu,
                   mem=int(0.95 * self.mem * self.n_cpu / 1000),
                   scratch=os.path.join(self.scratch_dir, self.task_id))


class Trinity(SlurmExecutableTask, CheckTargetNonEmpty):

    scratch_dir = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 2000
        self.n_cpu = 20
        self.partition = 'RG-Diane-Saunders,nbi-long'

    def output(self):
        return LocalTarget(os.path.join(os.path.dirname(self.input().path), 'trinity', 'Trinity.fasta'))

    def work_script(self):
        return '''#!/bin/bash
        source trinityrnaseq-2.3.2;
        source bowtie-2.2.6
        mkdir -p {scratch}
        set -euo pipefail

        cd {scratch}

        Trinity --seqType fq \
                --left {R1} \
                --right {R2} \
                --max_memory {mem}G \
                --output {scratch} \
                --jaccard_clip \
                --full_cleanup  \
                --CPU {n_cpu}

        mv {scratch}/Trinity-GG.fasta {output}
        '''.format(input=self.input().path,
                   output=self.output().path,
                   n_cpu=self.n_cpu,
                   mem=int(0.95 * self.mem * self.n_cpu / 1000),
                   scratch=os.path.join(self.scratch_dir, self.task_id))


class TrinityNormalise(SlurmExecutableTask, CheckTargetNonEmpty):

    scratch_dir = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 2000
        self.n_cpu = 10
        self.partition = 'RG-Diane-Saunders,nbi-long'

    def output(self):
        return LocalTarget(os.path.join(os.path.dirname(self.input().path), 'trinity', 'Trinity-GG.fasta'))

    def work_script(self):
        return '''#!/bin/bash
        source trinityrnaseq-2.3.2;
        source bowtie-2.2.6
        mkdir -p {scratch}
        set -euo pipefail

        cd {scratch}

        $TRINITY_HOME/util/insilico_read_normalization.pl  \
                --seqType fq \
                --JM  {mem}G \
                --CPU {n_cpu} \
                --max_cov 50 \
                --PARALLEL_STATS \
                --left  {R1} \
                --right {R2}


        #mv {scratch}/Trinity-GG.fasta {output}
        '''.format(R1=self.input()[0].path,
                   R2=self.input()[0].path,
                   output=self.output().path,
                   n_cpu=self.n_cpu,
                   mem=int(0.95 * self.mem * self.n_cpu / 1000),
                   scratch=os.path.join(self.scratch_dir, self.task_id))


class GMAP(SlurmExecutableTask, CheckTargetNonEmpty):

    gmap_reference_name = luigi.Parameter(default='PST130')
    gmap_reference_path = luigi.Parameter(default='/tgac/workarea/collaborators/saunderslab/FP_pipeline/reference/gmap')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mem = 4000
        self.n_cpu = 1
        self.partition = 'nbi-medium'

    def output(self):
        return LocalTarget(os.path.join(self.base_dir, VERSION, PIPELINE, self.output_prefix, 'trinity.gff3'))

    def work_script(self):
        return '''#!/bin/bash
                source gmap-20160923;
                set -euo pipefail

                gmap -n 0 -D {db_path} -d {db} {fasta} -f gff3_gene > {output}.temp
                mv {output}.temp {output}

                '''.format(db_path=self.gmap_reference_path,
                           db=self.gmap_reference_name,
                           output=self.output().path,
                           fasta=self.input().path)
