from abc import ABC, abstractmethod

class CombustiblesValidosRepository(ABC):
    @abstractmethod
    def processDataCombustiblesValidos(self):
        pass