import luigi
import luigi.task_register
from luigi import Target, LocalTarget
from luigi.util import inherits
from .utils import get_ext
import multiprocessing_on_dill as multiprocessing


def indextarget(struct, idx):
    """
    Maps all Targets in a structured output to an indexed temporary file
    """
    if isinstance(struct, Target):
        base, ext = get_ext(struct.path)
        return LocalTarget(base + "_" + str(idx) + ext)
#    elif isinstance(struct, list):
#        indextarget(struct[0], idx)
    else:
        raise NotImplemented()


class ScatterGather():
    '''Decorator to transparently add Scatter-Gather parallelism to a Luigi task
    :param scatterTask must inherit and implement a run() method which maps
           a single input() file to an array of output() files
    :param scatterTask must inherit and implement a run() method which maps
           an array of input() files to a single output() file
    :param N the number of parts to scatter into

    Example
    =======

    class scatter(luigi.Task):
        def run(self):
            with self.input().open() as fin:
                inp = fin.readlines()
            perfile = math.ceil(len(inp)/len(self.output()))
            for i,out in enumerate(self.output()):
                with out.open('w') as fout:
                    fout.writelines(inp[i*perfile:(i+1)*perfile])


    class gather(luigi.Task):
        def run(self):
            with self.output().open('w') as fout:
                for i in self.input():
                    with i.open('r') as fin:
                        fout.write(fin.read())


    @ScatterGather(scatter, gather, 10)
    class ToBeScattered(luigi.Task):
        def run(self):
            with self.input().open('r') as fin:
                with self.output().open('w') as fout:
                    for l in fin:
                        fout.write("Done! " + l)

    '''

    @staticmethod
    def metaProgScatter(decorator, scattertask):

        @inherits(decorator.workTask)
        class Scatter(scattertask):

            def requires(self):
                wt_req = decorator.workTask.requires(self)
                return wt_req[0] if isinstance(wt_req, list) else wt_req

            def output(self):
                return [indextarget(decorator.workTask.input(self), i) for i in range(decorator.N)]

            def to_str_params(self, only_significant=False):
                sup = super().to_str_params(only_significant)
                extras = {'input': self.input().path, 'N': str(decorator.N)}
                return dict(list(sup.items()) + list(extras.items()))

        Scatter.clone_parent = decorator.workTask.clone_parent
        Scatter.__name__ = decorator.workTask.__name__ + 'Scatter'
        return Scatter

    @staticmethod
    def metaProgWork(decorator, worktask):
        Scatter = ScatterGather.metaProgScatter(decorator, decorator.scatterTask)

        class Work(worktask):
            SG_index = luigi.IntParameter()

            @property
            def task_family(self):
                return worktask.task_family

            def requires(self):
                s = self.clone(Scatter)
                return [s] + super().requires()[1:] if isinstance(super().requires(), list) else [s]

            def input(self):
                inp = super().input()
                scattered = inp[0][self.SG_index]
                return [scattered] + inp[1:] if isinstance(super().requires(), list) else scattered

            def output(self):
                s = self.clone(decorator.workTask)
                return indextarget(decorator.workTask.output(s), self.SG_index)

        return Work

    @staticmethod
    def metaProgGather(decorator, gathertask):
        Work = ScatterGather.metaProgWork(decorator, decorator.workTask)

        @inherits(decorator.workTask)
        class Gather(gathertask):
            SG_index = None

            def requires(self):
                return [self.clone(Work, SG_index=i)
                        for i in range(decorator.N)]

            def output(self):
                return decorator.workTask.output(self.clone(decorator.workTask))

            def to_str_params(self, only_significant=False):
                sup = super().to_str_params(only_significant)
                extras = {'N': str(decorator.N), 'output': self.output().path}
                return dict(list(sup.items()) + list(extras.items()))

            def __reduce__(self):
                def mkGather(d, t):
                    return ScatterGather.metaProgGather(d, t)()
                return mkGather, (decorator, gathertask)

        Gather.__name__ = decorator.workTask.__name__ + 'Gather'
        return Gather

    def __init__(self, scatterTask, gatherTask, N):
        self.scatterTask = scatterTask
        self.gatherTask = gatherTask
        self.N = N

    def __call__(self, workTask):
        self.workTask = workTask
        return ScatterGather.metaProgGather(self, self.gatherTask)
