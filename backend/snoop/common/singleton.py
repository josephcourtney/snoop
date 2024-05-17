class Singleton(type):
    _instance = None  # This is an attribute of the class

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance

    def clear(cls):
        cls._instance = None
