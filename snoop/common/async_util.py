class AsyncInit(type):
    async def __call__(cls, *args, **kwargs):
        instance = cls.__new__(cls, *args, **kwargs)
        await instance.__init__(*args, **kwargs)
        return instance
