from abc import ABC, abstractmethod

class IndicadorRepository(ABC):
    @abstractmethod
    def saveAndProcessData(self):
        pass