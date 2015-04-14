__author__ = 'stuart'

from collections import namedtuple


def JoinedObject(left, right):
    ldict = left.__dict__ if hasattr(left, '__dict__') else {}
    rdict = right.__dict__ if hasattr(right, '__dict__') else {}
    names = filter(lambda name: not name.startswith('_'),
                   set(['left', 'right'] + ldict.keys() + rdict.keys()))
    joined_class = namedtuple(
        left.__class__.__name__ + right.__class__.__name__, names)
    items = dict(filter(lambda p: p[0] not in ['left', 'right'],
                        rdict.items() + ldict.items()))
    return joined_class(left=left, right=right, **items)

# class JoinedObject(object):
#     def __init__(self, left, right):
#         print("\n\n\n\n{}\n\n\n\n".format(left))
#         print("\n\n\n\n{}\n\n\n\n".format(right))
#         self.left = left
#         self.right = right
#         print(self.__dict__)
#
#     def __getattr__(self, attr):
#         if attr == 'left':
#             return self.left
#         elif attr == 'right':
#             return self.right
#         else:
#             return self.get_from_sources(attr)
#
#     def __repr__(self):
#         return '<{} object at {}>'.format(
#             self.left.__class__.__name__ + self.right.__class__.__name__,
#             id(self))
#
#     def __dir__(self):
#         attrs = list(set(dir(self.left) + dir(self.right) + ['left', 'right']))
#         return sorted(attrs)
#
#     def get_from_sources(self, attr):
#         if hasattr(self.left, attr):
#             return getattr(self.left, attr)
#         elif hasattr(self.right, attr):
#             return getattr(self.right, attr)
#         else:
#             raise AttributeError(
#                 "Neither of joined object's parents ({}, {}), have attribute "
#                 "'{}'".format(self.left.__class__.__name__,
#                               self.right.__class__.__name__, attr))
