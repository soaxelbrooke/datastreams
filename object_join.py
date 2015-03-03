__author__ = 'stuart'


class JoinedObject(object):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __getattr__(self, attr):
        if attr == 'left':
            return self.left
        elif attr == 'right':
            return self.right
        else:
            return self.get_from_sources(attr)

    def __repr__(self):
        return '<{} object at {}>'.format(
            self.left.__class__.__name__ + self.right.__class__.__name__,
            id(self))

    def get_from_sources(self, attr):
        if hasattr(self.left, attr):
            return getattr(self.left, attr)
        elif hasattr(self.right, attr):
            return getattr(self.right, attr)
        else:
            raise AttributeError(
                "Neither of joined object's parents ({}, {}), have attribute "
                "'{}'".format(self.left.__class__.__name__,
                              self.left.__class__.__name__, attr))
