from abc import ABC, abstractmethod
class PetroperuRepository(ABC):
    
    @abstractmethod
    def saveData(self):
        ...