"""
A collection of utility functions and classes, particulaly for accounting for different
python, numpy, and spark versions
"""
from collections import namedtuple as _invisiblenamedtuple
from itertools import chain
append_dict = lambda i_dict, **kwargs: dict(list(i_dict.items()) + list(kwargs.items()))
append_dict_raw = lambda i_dict, o_dict: dict(list(i_dict.items()) + list(o_dict.items()))
cflatten = lambda x: list(chain(*x))

def namedtuple(*args, **kwargs):
    """
    namedtuple with a global presence for using the pickler
    :param args:
    :param kwargs:
    :return:
    """
    nt_class = _invisiblenamedtuple(*args, **kwargs)
    # put it on the global scale so it can be tupled correctly
    globals()[nt_class.__name__] = nt_class
    return nt_class

class TypeTool(object):
    """
    For printing type outputs with a nice format
    """
    @staticmethod
    def info(obj):
        """
        Produces pretty text descriptions of type information from an object
        :param obj:
        :return: str for type
        """
        if type(obj) is tuple:
            return '({})'.format(', '.join(map(TypeTool.info,obj)))
        elif type(obj) is list:
            return 'List[{}]'.format(TypeTool.info(obj[0]))
        else:
            ctype_name = type(obj).__name__
            if ctype_name == 'ndarray': return '{}[{}]{}'.format(ctype_name,obj.dtype, obj.shape)
            elif ctype_name == 'str': return 'string'
            elif ctype_name == 'bytes': return 'List[byte]'
            else: return ctype_name

class NamedLambda(object):
    """
    allows the use of named anonymous functions for arbitrary calculations
    """

    def __init__(self, code_name, code_block, **add_args):
        """
        Create a namedlambda function
        :param code_name: str the name/description of the code to run
        :param code_block: the function to call when the namedlambda is called
        :param add_args: the additional arguments to give to it
        """
        self.code_name = code_name
        self.code_block = code_block
        self.add_args = add_args
        self.__name__ = code_name

    def __call__(self, *cur_objs, **kwargs):
        return self.code_block(*cur_objs, **append_dict_raw(kwargs,self.add_args))

    def __repr__(self):
        return self.code_name

    def __str__(self):
        return self.__repr__()


class FieldSelector(object):
    """
    allows the use of named anonymous functions for selecting fields (makes dag's more readable)
    """

    def __init__(self, field_name):
        self.field_name = field_name
        self.__name__ = "Select Field: {}".format(self.field_name)

    def __call__(self, cur_obj):
        try:
            return cur_obj[self.field_name]
        except:
            return cur_obj._asdict()[self.field_name]

    def __repr__(self):
        return __name__

    def __str__(self):
        return self.__repr__()
