from abc import ABC, abstractmethod
class MayoristaPetroperuRepository(ABC):
    
    @abstractmethod
    def saveData(self):
        ...