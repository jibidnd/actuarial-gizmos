class DotDict(dict):
    
    def __getattr__(self, key):
        if (ret := self.get(key)) is not None:
            return ret
        else:
            # Raise an error
            raise AttributeError(f'Attribute {key} does not exist.')
    
    def __setattr__(self, key, value):
        self[key] = value
        return

class FancyDict(DotDict):
    """A dictionary-like class that allow attribute access.
        Adding a class to allow a unified API.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get(self, key: str, default = None):
        # `get` will allow a recursive search.

        # First check if any key is equal to `key`
        try:
            if (res := super().get(key)) is not None:
                return res
        except TypeError:
            # e.g. a list is passed. The top level dict
            # may not take a list as a key but maybe a lower
            # level item would be a core.Book.
            pass

        # Otherwise loop through each item
        # to see if we can `get` the requested key
        for val in self.values():
            try:
                if (ret := val.get(key)) is not None:
                    return ret
            except (AttributeError, TypeError):
                pass
        
        # If we get here that means we didn't find anything
        # Return the default value
        return default

    def register(self, **kwargs) -> None:
        # Override to add checks
        super().update(kwargs)

class AccessLogger:
    """Logs the attributes accessed of the object.
    Any attribute accessed will be stored in the form of
        a tuple, with each element containing the complete
        path of access from the root object.
    """    
    
    def __init__(self, accessed = None, root = None):
        self.accessed = accessed or set()
        self.root = root or tuple()
    
    def __getattr__(self, key: str):
        return self.get(key)
    
    def __getitem__(self, key: str):
        return self.get(key)

    def __call__(self, *args, **kwargs):
        return self
    
    # overload the operators user can add/subtract/multiply/etc the logger
    def __add__(self, other):
        return self
    
    def __sub__(self, other):
        return self
    
    def __mul__(self, other):
        return self
    
    def __pow__(self, other):
        return self
    
    def __truediv__(self, other):
        return self
    
    def __floordiv__(self, other):
        return self
    
    def __mod__(self, other):
        return self
    
    def __lshift__(self, other):
        return self
    
    def __rshift__(self, other):
        return self
    
    def __and__(self, other):
        return self
    
    def __or__(self, other):
        return self
    
    def __xor__(self, other):
        return self
    
    def __invert__(self):
        return self

    def get(self, key: str):
        # complete path to current item
        path_to_current = self.root + (key,)
        # add full path to "accessed" list
        self.accessed |= {path_to_current}
        # return an AccessLogger object with reference to the original list
        return AccessLogger(self.accessed, path_to_current)
