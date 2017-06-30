import luigi
import luigi.util


class inherits(object):

    def __init__(self, *args, **kwargs):
        super(inherits, self).__init__()
        if len(args) > 0 and len(kwargs) > 0:
            raise Exception("Cannot combine dictionary and list type requirements!")

        self.tasks_to_inherit = list(args) if len(args) > 0 else kwargs

    def __call__(self, task_that_inherits):
        tasks = self.tasks_to_inherit if isinstance(self.tasks_to_inherit, list) else self.tasks_to_inherit.values()
        for task_to_inherit in tasks:
            for param_name, param_obj in task_to_inherit.get_params():
                if not hasattr(task_that_inherits, param_name):
                    setattr(task_that_inherits, param_name, param_obj)

        # Modify task_that_inherits by subclassing it and adding methods
        @luigi.task._task_wraps(task_that_inherits)
        class Wrapped(task_that_inherits):
            def clone_parent(_self, **args):
                if isinstance(self.tasks_to_inherit, dict):
                    return {k: _self.clone(cls=v, **args) for k, v in self.tasks_to_inherit.items()}
                elif len(self.tasks_to_inherit) == 1:
                    return _self.clone(cls=self.tasks_to_inherit[0], **args)
                else:
                    return [_self.clone(cls=x, **args) for x in self.tasks_to_inherit]

        return Wrapped


class requires(object):

    def __init__(self, *args, **kwargs):
        super(requires, self).__init__()
        self.inherit_decorator = inherits(*args, **kwargs)

    def __call__(self, task_that_requires):
        task_that_requires = self.inherit_decorator(task_that_requires)

        # Modify task_that_requires by subclassing it and adding methods
        @luigi.task._task_wraps(task_that_requires)
        class Wrapped(task_that_requires):
            def requires(_self):
                return _self.clone_parent()
        return Wrapped
