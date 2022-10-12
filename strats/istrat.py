from abc import ABCMeta, abstractmethod

## sources https://realpython.com/python-interface/
## https://docs.python.org/3/library/abc.html
class IStrat(metaclass=ABCMeta):
    @classmethod
    def __subclasshook_(cls,subclass):
        return (hasattr(subclass,"check_factors") and callable(subclass.check_factors))
    
    @abstractmethod
    def check_factors(self):
        raise NotImplementedError("must define check_factors")
    
    # @abstractmethod
    # def create_sim(self):
    #     raise NotImplementedError("must define create_sim")