class FancyDict(dict):
    """A dictionary-like class that allow attribute access.
        Adding a class to allow a unified API.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def __getattr__(self, name: str):

        if (ret := self.get(name)) is not None:
            return ret
        else:
            # Raise an error
            raise AttributeError(f'Attribute {name} does not exist.')

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