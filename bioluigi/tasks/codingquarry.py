from luigi import LocalTarget, Task, WrapperTask

from ..slurm import SlurmExecutableTask
from ..utils import CheckTargetNonEmpty, get_ext
import os


class CodingQuarry(CheckTargetNonEmpty, SlurmExecutableTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1000
        self.n_cpu = 12
        self.partition = "nbi-medium,RG-Diane-Saunders"

    def output(self):
        return LocalTarget(os.path.join(os.path.dirname(self.input()['fasta'].path)), 'CodingQuarry')

    def work_script(self):
        return '''#!/bin/bash
                  source CodingQuarry-2.0;
                  mkdir -p {output}_temp
                  set -euo pipefail

                  cd {output}_temp
                  CodingQuarry -d -f {fasta} -t {gff} -d -p 12

                  mv {output}_temp {output}
                  '''.format(fasta=self.input()['fasta'].path,
                             gff=self.input()['gff'].path,
                             output=self.output().path,
                             n_cpu=self.n_cpu)


class CodingQuarryPM(CheckTargetNonEmpty, SlurmExecutableTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the SLURM request params for this task
        self.mem = 1000
        self.n_cpu = 12
        self.partition = "nbi-medium,RG-Diane-Saunders"

    def output(self):
        return LocalTarget(os.path.join(os.path.dirname(self.input()['fasta'].path)), 'CodingQuarry')

    def work_script(self):
        return '''#!/bin/bash
                  source CodingQuarry-2.0;
                  mkdir -p {output}_temp
                  set -euo pipefail

                  cd {output}_temp
                  /nbi/software/testing/CodingQuarry/2.0/x86_64/run_CQ-PM_unstranded.sh {fasta} {gff}

                  mv {output}_temp {output}
                  '''.format(fasta=self.input()['fasta'].path,
                             gff=self.input()['gff'].path,
                             output=self.output().path,
                             n_cpu=self.n_cpu)


class GetPassed(WrapperTask):
    def output(self):
        return LocalTarget(os.path.join(self.input().path, 'out', "PredictedPass.gff3"))


class ConcatGFF(Task):
    def output(self):
        return LocalTarget(os.path.join(self.input().path, 'out', 'CodingQuarry.merged.gff3'))

    def run(self):
        with self.output().open('w') as fout:
            with open(os.path.join(self.input().path, 'out', 'PredictedPass.gff3'), 'r') as fin:
                for l in fin:
                        fout.write(l)

            with open(os.path.join(self.input().path, 'out', 'PGN_predictedPass.gff3'), 'r') as fin:
                for l in fin:
                        fout.write(l)


class FixGFF(Task):
    '''From https://github.com/nextgenusfs/funannotate/blob/dc35a27b8928523e1b8164e31853783d285f091d/util/codingquarry2gff3.py
    '''

    def output(self):
        return LocalTarget(get_ext(self.input().path)[0] + ".fixed.gff3")

    def run(self):
        GeneCount, exonCounts = 1, {}
        with self.input().open('r') as infile, self.output().open('w') as fout:

            fout.write(("##gff-version 3\n"))

            for line in infile:
                line = line.strip()
                contig, source, feature, start, end, score, strand, phase, attributes = line.split('\t')
                ID, Parent, Name = (None,)*3
                info = attributes.split(';')
                for x in info:
                    if x.startswith('ID='):
                        ID = x.replace('ID=', '')
                    elif x.startswith('Parent='):
                        Parent = x.replace('Parent=', '')
                    elif x.startswith('Name='):
                        Name = x.replace('Name=', '')
                if ID and ' ' in ID:
                    ID = ID.split(' ')[0]
                if Parent and ' ' in Parent:
                    Parent = Parent.split(' ')[0]
                if feature == 'gene':
                    geneID = 'gene_'+str(GeneCount)
                    transID = 'transcript_'+str(GeneCount)+'-T1'
                    #if not ID in geneRef:
                    #   geneRef[ID] = (geneID, transID)
                    fout.write('{:}\t{:}\t{:}\t{:}\t{:}\t{:}\t{:}\t{:}\tID={:};Name={:};Alias={:};\n'.format(contig,source,feature, start, end, score, strand, phase, geneID, geneID, ID))
                    fout.write('{:}\t{:}\t{:}\t{:}\t{:}\t{:}\t{:}\t{:}\tID={:};Parent={:};Alias={:};\n'.format(contig,source,'mRNA', start, end, '.', strand, '.',transID, geneID, ID))
                    GeneCount += 1
                elif feature == 'CDS':
                    trimID = ID.replace('CDS:', '')
                    #if trimID in geneRef:
                    #   geneID,transID = geneRef.get(trimID)
                    if not transID in exonCounts:
                        exonCounts[transID] = 1
                    else:
                        exonCounts[transID] += 1
                    num = exonCounts.get(transID)
                    fout.write('{:}\t{:}\t{:}\t{:}\t{:}\t{:}\t{:}\t{:}\tID={:}.exon{:};Parent={:};\n'.format(contig,source,'exon', start, end, '.', strand, '.',transID, num, transID))
                    fout.write('{:}\t{:}\t{:}\t{:}\t{:}\t{:}\t{:}\t{:}\tID={:}.cds;Parent={:};\n'.format(contig,source,feature, start, end, score, strand, phase, transID, transID))



