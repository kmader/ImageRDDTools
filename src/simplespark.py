import time
import inspect
from itertools import chain
from utils import TypeTool
from collections import defaultdict

class LocalRDD(object):
    def __init__(self, items, prev, command, code='', calc_time=None, **args):
        self.items = list(items)
        self.prev = prev
        self.command_args = (command, args)
        self.code = code
        if calc_time is not None: self.calc_time = calc_time

    def first(self):
        return self.items[0]

    def collect(self):
        return self.items

    def count(self):
        return len(self.items)

    def _transform(self, op_name, in_func, lapply_func, **args):
        """
        Args:
            in_func is the function the user supplied
            lapply_func is the function to actually apply
        """
        try:
            trans_func_code = inspect.getsourcelines(in_func)
        except:
            trans_func_code = ''
        stime = time.time()
        new_list = lapply_func(self.items)
        etime = time.time() - stime
        return LocalRDD(new_list, [self], op_name, apply_func=in_func,
                        calc_time=etime,
                        code=trans_func_code, **args)

    def map(self, apply_func):
        return self._transform('map', apply_func,
                               lapply_func=lambda x_list: [apply_func(x) for x in x_list])

    def mapValues(self, apply_func):
        return self._transform('mapValues', apply_func,
                               lapply_func=lambda x_list: [(k, apply_func(v)) for (k, v) in x_list])

    def flatMap(self, apply_func):
        return self._transform('flatMap', apply_func,
                               lapply_func=lambda x_list: cflatten([apply_func(x) for x in x_list]))

    def flatMapValues(self, apply_func):
        return self._transform('flatMapValues', apply_func,
                               lapply_func=lambda x_list: cflatten(
                                   [[(k, y) for y in apply_func(v)] for (k, v) in x_list]))

    def groupBy(self, apply_func):
        def gb_func(x_list):
            o_dict = defaultdict(list)
            for i in x_list:
                o_dict[apply_func(i)] += [i]
            return list(o_dict.items())

        return self._transform('groupBy', apply_func,
                               lapply_func=gb_func)

    def saveAsPickleFile(self, filename):
        return self._transform('saveAsPickleFile', NamedLambda('SaveFileShard', lambda x: x),
                               lapply_func=lambda x_list: [(x, filename) for x in x_list])

    def repartition(self, *args):
        return self

    @property
    def type(self):
        return TypeTool.info(self.first())

    @property
    def key_type(self):
        return TypeTool.info(self.items[0][0])

    @property
    def value_type(self):
        return TypeTool.info(self.items[0][1])

    def id(self):
        return (str(self.items), tuple(self.prev), str(self.command_args)).__hash__()


class LocalSparkContext(object):
    def __init__(self):
        pass

    def parallelize(self, in_list):
        return LocalRDD(in_list, [], 'parallelize', in_list=in_list)
