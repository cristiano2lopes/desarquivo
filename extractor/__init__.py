from .core import *
from .radio_comercial import *
from .rtp import *
from .publico import *
from .tmdb import *
from .record import *
from .ojogo import *


def setup_extractors():
    def get_subclasses(cls):
        for subclass in cls.__subclasses__():
            yield from get_subclasses(subclass)
            yield subclass

    all_subclasses = get_subclasses(Extractor)
    return list(all_subclasses)
