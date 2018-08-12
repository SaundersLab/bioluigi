import os
from luigi import LocalTarget, Parameter
from ..slurm import SlurmExecutableTask, SlurmTask
from ..utils import CheckTargetNonEmpty, get_ext
from luigi.file import TemporaryFile


class Cufflinks(SlurmExecutableTask, CheckTargetNonEmpty):

    base_dir = Parameter()
    scratch_dir = Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 4000
        self.n_cpu = 1
        self.partition = "nbi-medium"

    def output(self):
        return LocalTarget(get_ext(self.input().path)[0] + '.cufflinks.gtf')

    def work_script(self):
        return '''#!/bin/bash
        source cufflinks-2.2.1;
        set -euo pipefail

        cufflinks {input} --quiet -o {temp_dir}

        mv {temp_dir}/transcripts.gtf {output}
        '''.format(input=self.input().path,
                   output=self.output().path,
                   temp_dir=os.path.join(self.scratch_dir, self.library, 'cufflinks_temp'))


class CuffMerge(SlurmExecutableTask, CheckTargetNonEmpty):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 2000
        self.n_cpu = 4
        self.partition = "nbi-medium"

    def output(self):
        return LocalTarget('cufflinks_raw.gtf')

    def work_script(self):
        self.temp = TemporaryFile()
        return '''#!/bin/bash
        source cufflinks-2.2.1;
        source python-2.7.10;

        set -euo pipefail

        mkdir -p {out_dir}

        echo '{input}' > {temp}
        cd {out_dir}
        cuffmerge  -p {n_cpu}  -o {out_dir} {temp}

        mv {out_dir}/merged.gtf {output}
        '''.format(input="\n".join([x.path for x in self.input()]),
                   output=self.output().path,
                   out_dir=os.path.join(self.scratch_dir, self.task_id, 'cufflinks_merge'),
                   temp=self.temp.path,
                   n_cpu=self.n_cpu)


class AddTranscripts(SlurmTask):
    '''The gtf file produced by cuffmerge has no transcript features, not sure why??!
        This task reconstructs the transcript features use the transcript_id tag of the exons
        and taking the start/stop of the first/last exons with a given transcript_id is the start/stop '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 4000
        self.n_cpu = 1
        self.partition = "nbi-short"

    def output(self):
        return LocalTarget(os.path.join(get_ext(self.input().path)[0] + ".transcript.gtf"))

    def work(self):
        import re
        with self.input().open() as fin, self.output().open('w') as fout:
            current_tid = None
            tid = re.compile('transcript_id "(\S+)";')
            gid = re.compile('gene_id "(\S+)";')
            exons, start, stop = [], float('inf'), 0

            for line in fin:
                exon_start, exon_stop = line.split('\t')[3:5]
                tran = tid.search(line).groups()[0]

                if tran != current_tid:
                    if current_tid is not None:

                        # Flush previous transcript
                        template = exons[0].split('\t')
                        gene = gid.search(exons[0]).groups()[0]
                        fout.write('\t'.join(template[:2]) +
                                   '\ttranscript\t{0}\t{1}\t.\t{2}\t.\tgene_id "{3}"; transcript_id "{4}";\n'.format(
                                   start, stop, template[6], gene, current_tid))
                        fout.writelines(exons)

                    # And start new one
                    current_tid = tran
                    exons, start, stop = [], float('inf'), 0

                exons.append(line)
                start = min(start, int(exon_start))
                stop = max(stop, int(exon_stop))

            # Final flush
            template = exons[0].split('\t')
            gene = gid.search(exons[0]).groups()[0]
            fout.write('\t'.join(template[:2]) +
                       '\ttranscript\t{0}\t{1}\t.\t{2}\t.\tgene_id "{3}"; transcript_id "{4}";\n'.format(
                       start, stop, template[6], gene, current_tid))
            fout.writelines(exons)
