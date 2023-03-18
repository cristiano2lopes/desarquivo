from .core import *
from .radio_comercial import *


def setup_extractors():
    def get_subclasses(cls):
        for subclass in cls.__subclasses__():
            yield from get_subclasses(subclass)
            yield subclass

    all_subclasses = get_subclasses(Extractor)
    return list(all_subclasses)
