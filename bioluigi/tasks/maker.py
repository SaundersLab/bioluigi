from ..slurm import SlurmExecutableTask
from ..utils import CheckTargetNonEmpty, get_ext
import luigi
import os
import time
from luigi import LocalTarget


maker_opts_keys = ['genome', 'organism_type', 'maker_gff', 'est_pass', 'altest_pass',
                   'protein_pass', 'rm_pass', 'model_pass', 'pred_pass', 'other_pass', 'est',
                   'altest', 'est_gff', 'altest_gff', 'protein', 'protein_gff', 'model_org',
                   'rmlib', 'repeat_protein', 'rm_gff', 'prok_rm', 'softmask', 'snaphmm',
                   'gmhmm', 'augustus_species', 'fgenesh_par_file', 'pred_gff', 'model_gff',
                   'est2genome', 'protein2genome', 'trna', 'snoscan_rrna', 'unmask', 'other_gff',
                   'alt_peptide', 'cpus', 'max_dna_len', 'min_contig', 'pred_flank',
                   'pred_stats', 'AED_threshold', 'min_protein', 'alt_splice', 'always_complete',
                   'map_forward', 'keep_preds', 'split_hit', 'single_exon', 'single_length',
                   'correct_est_fusion', 'tries', 'clean_try', 'clean_up', 'TMP']

# TODO make the MAKER pipeline use base_dir parameter   to be consistent with other tasks


class MAKER(SlurmExecutableTask):

    base_dir = luigi.Parameter(significant=True)

    maker_opts = luigi.DictParameter(significant=True)
    maker_prefix = luigi.Parameter(significant=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 2000
        self.n_cpu = 12
        self.partition = "nbi-medium,RG-Diane-Saunders"
        self.log = os.path.join(self.base_dir, 'maker',
                                self.maker_prefix + ".maker.output",
                                'maker_' + str(hash(self.maker_opts)) + ".log")

        self.maker_opts_path = os.path.join(self.base_dir, 'maker',
                                            self.maker_prefix + ".maker.output", "maker_opts.ctl")

    def output(self):
        return LocalTarget(os.path.join(self.base_dir, 'maker', self.maker_prefix + ".maker.output",
                                        self.maker_prefix + "_master_datastore_index.log"))

    def complete(self):
        time.sleep(1)
        if not os.path.exists(self.log):
            return False

        with open(self.log) as flog:
            log = flog.readlines()
            for l in log[-20:]:
                if l == 'Maker is now finished!!!\n':
                    return True
            return False

    def write_maker_opts(self):
        os.makedirs(os.path.dirname(self.maker_opts_path), exist_ok=True)
        with open(self.maker_opts_path, 'w') as fopts:
            for k, v in self.maker_opts.items():
                if v:
                    fopts.write("{0}={1}\n".format(k, v))
            for k, v in self.input().items():
                fopts.write("{0}={1}\n".format(k, v.path))

    def work_script(self):
        self.write_maker_opts()

        return '''#!/bin/bash
                  source openmpi-1.8.4;
                  source maker-2.31.8
                  source genemark-4.33_ES_ET;
                  set -euo pipefail

                  cd {dir}
                  mpirun -n {n_cpu} maker {maker_opts} -base {base} &> {log}

        '''.format(maker_opts=self.maker_opts_path,
                   dir=os.path.split(os.path.dirname(self.output().path))[0],
                   base=self.maker_prefix,
                   log=self.log,
                   n_cpu=self.n_cpu)


class FastaMerge(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1000
        self.n_cpu = 1
        self.partition = "nbi-short"
        self.dir = os.path.dirname(self.input().path)
        self.name = os.path.basename(self.input().path)[:-27]

    def output(self):
        return {'proteins': LocalTarget(os.path.join(self.dir, self.name + ".all.maker.proteins.fasta")),
                'transcripts': LocalTarget(os.path.join(self.dir, self.name + ".all.maker.transcripts.fasta"))}

    def work_script(self):
        return '''#!/bin/bash -e
                  source maker-2.31.8
                  set -euo pipefail

                  cd {dir}
                  fasta_merge -d {input} -o {prefix}.temp

                  mv {prefix}.temp.all.maker.proteins.fasta {prefix}.all.maker.proteins.fasta
                  mv {prefix}.temp.all.maker.transcripts.fasta {prefix}.all.maker.transcripts.fasta
                 '''.format(input=self.input().path,
                            prefix=self.name,
                            dir=self.dir)


class GFFMerge(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1000
        self.n_cpu = 1
        self.partition = "nbi-short"
        self.dir = os.path.dirname(self.input().path)
        self.name = os.path.basename(self.input().path)[:-27]

    def output(self):
        return LocalTarget(os.path.join(self.dir, self.name + ".all.gff"))

    def work_script(self):
        return '''#!/bin/bash -e
                  source maker-2.31.8
                  set -euo pipefail

                  gff3_merge -n -d {input} -o {output}.temp

                  mv {output}.temp {output}
                 '''.format(input=self.input().path,
                            output=self.output().path)


class Maker2ZFF(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1000
        self.n_cpu = 1
        self.partition = "nbi-short"
        self.dir = os.path.dirname(self.input().path)
        self.name = os.path.basename(self.input().path)[:-27]

    def output(self):
        return {'ann': LocalTarget(os.path.join(self.dir, self.name + ".ann")),
                'dna': LocalTarget(os.path.join(self.dir, self.name + ".dna"))}

    def work_script(self):
        return '''#!/bin/bash -e
                  source maker-2.31.8
                  set -euo pipefail
                  cd {dir}

                  maker2zff -n -d {input}

                  mv  genome.ann {name}.ann
                  mv  genome.dna {name}.dna
                 '''.format(input=self.input().path,
                            dir=self.dir,
                            name=self.name)


class FathomStats(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1000
        self.n_cpu = 1
        self.partition = "nbi-short"
        self.dir = os.path.dirname(self.input()['ann'].path)
        self.name = get_ext(os.path.basename(self.input()['ann'].path))[0]

    def output(self):
        return {'stats': LocalTarget(os.path.join(self.dir, "SNAP", "gene-stats.log")),
                'validate': LocalTarget(os.path.join(self.dir, "SNAP", "validate.log"))}

    def work_script(self):
        return '''#!/bin/bash -e
                  source snap-16.2.2013
                  set -euo pipefail

                  fathom {ann} {dna} -gene-stats > {stats}.temp 2>&1
                  fathom {ann} {dna} -validate > {validate}.temp 2>&1

                  mv {stats}.temp {stats}
                  mv {validate}.temp {validate}
                 '''.format(ann=self.input()['ann'].path,
                            dna=self.input()['dna'].path,
                            stats=self.output()['stats'].path,
                            validate=self.output()['validate'].path,
                            dir=self.dir)


class TrainSNAP(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1000
        self.n_cpu = 1
        self.partition = "nbi-short"
        self.dir = os.path.dirname(self.input()['ann'].path)
        self.name = get_ext(os.path.basename(self.input()['ann'].path))[0]

    def output(self):
        return LocalTarget(os.path.join(self.dir, "SNAP", os.path.split(self.dir)[1][:-13] + ".hmm"))

    def work_script(self):
        return '''#!/bin/bash -e
                  source snap-16.2.2013
                  mkdir -p {dir}/SNAP
                  mkdir -p {dir}/SNAP/params
                  set -euo pipefail

                  cd {dir}/SNAP

                  fathom {ann} {dna} -categorize 1000
                  fathom uni.ann uni.dna -export 1000 -plus

                  cd params
                  forge ../export.ann ../export.dna
                  cd ..
                  hmm-assembler.pl {name} params > {name}.hmm

                 '''.format(ann=self.input()['ann'].path,
                            dna=self.input()['dna'].path,
                            name=os.path.split(self.dir)[1][:-13],
                            dir=self.dir)


class Maker2Jbrowse(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 8000
        self.n_cpu = 1
        self.partition = "nbi-medium"
        self.name = os.path.basename(self.input().path)[:-27]

    def output(self):
        return LocalTarget(os.path.join("/usr/users/JIC_a1/buntingd/GenomeAnnotation/Jbrowse/JBrowse-1.15.0/data", self.name))

    def work_script(self):
        return '''#!/bin/bash -e
                  source maker-2.31.8
                  source perl_activeperl-5.18.2.1802
                  source berkeleydb-4.3;
                  set -euo pipefail
                  export PATH=/usr/users/JIC_a1/buntingd/GenomeAnnotation/Jbrowse/JBrowse-1.15.0/bin:$PATH
                  cd /usr/users/JIC_a1/buntingd/GenomeAnnotation/Jbrowse/JBrowse-1.15.0

                  maker2jbrowse -d {input} -o {output}_temp

                  mv {output}_temp {output}
                 '''.format(input=self.input().path,
                            output=self.output().path)


class AEDDist(CheckTargetNonEmpty, SlurmExecutableTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1000
        self.n_cpu = 1
        self.partition = "nbi-short"

    def output(self):
        return LocalTarget(os.path.join(get_ext(self.input().path)[0] + ".aed"))

    def work_script(self):
        return '''#!/bin/bash -e
                  source maker-2.31.8
                  set -euo pipefail

                  /nbi/software/testing/maker/2.31.8/src/maker/bin/AED_cdf_generator.pl -b 0.01 {input} > {output}.temp

                  mv {output}.temp {output}

                 '''.format(input=self.input().path,
                            output=self.output().path)
