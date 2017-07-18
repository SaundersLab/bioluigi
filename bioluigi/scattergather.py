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

        def requires(self):
            wt_req = self.clone(decorator.workTask).requires()
            return wt_req[0] if isinstance(wt_req, list) else wt_req

        def output(self):
            wt_inp = self.clone(decorator.workTask).input()
            wt_inp = wt_inp[0] if isinstance(wt_inp, list) else wt_inp
            return [indextarget(wt_inp, i) for i in range(decorator.N)]

        def __reduce__(self):
            return lambda d,t:ScatterGather.metaProgScatter(d, t)(), (decorator, scattertask)
        
        scatter_cls =  type(decorator.workTask.__name__ + 'Scatter', 
                           (scattertask, ), 
                           {'clone_parent':decorator.workTask.clone_parent, 'requires':requires, 'output':output,
                             '__module__': '__main__'})
        return inherits(decorator.workTask)(scatter_cls)

    @staticmethod
    def metaProgWork(decorator, worktask):
        Scatter = ScatterGather.metaProgScatter(decorator, decorator.scatterTask)

        def requires(self):
            s = self.clone(Scatter)
            return [s] + super(self.__class__, self).requires()[1:] if isinstance(super(self.__class__, self).requires(), list) else [s]

        def input(self):
            inp = super(self.__class__, self).input()
            scattered = inp[0][self.SG_index]
            return [scattered] + inp[1:] if isinstance(super(self.__class__, self).requires(), list) else scattered

        def output(self):
            return indextarget(self.clone(decorator.workTask).output(), self.SG_index)
        
        def __reduce__(self):
            return lambda d,t: ScatterGather.metaProgWork(d, t)(), (decorator, worktask)
                            
        work_cls =  type(decorator.workTask.__name__ + 'Work', 
                         (worktask, ), 
                         {'SG_index':luigi.IntParameter(), 'input':input, 'output':output,
                          'requires':requires, '__module__': '__main__'})
        return work_cls

    @staticmethod
    def metaProgGather(decorator, gathertask):
        Work = ScatterGather.metaProgWork(decorator, decorator.workTask)
        def requires(self):
            return [self.clone(Work, SG_index=i)
                    for i in range(decorator.N)]

        def output(self):
            return self.clone(decorator.workTask).output()

        def __reduce__(self):
            return lambda d,t:ScatterGather.metaProgGather(d, t)(), (decorator, gathertask)
              
        gather_cls = type(decorator.workTask.__name__ + 'Gather', 
                          (gathertask, ), 
                          {'SG_index':None, 'requires':requires, 'output':output, 
                            '__module__': '__main__' })
        
        return inherits(decorator.workTask)(gather_cls)

    def __init__(self, scatterTask, gatherTask, N):
        self.scatterTask = scatterTask
        self.gatherTask = gatherTask
        self.N = N

    def __call__(self, workTask):
        self.workTask = workTask
        return ScatterGather.metaProgGather(self, self.gatherTask)
