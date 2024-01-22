from abc import ABC

from abc import ABC, abstractmethod
class PreciosMayoristasRepository(ABC):
    @abstractmethod
    def saveDataMayoristas(self):
        pass