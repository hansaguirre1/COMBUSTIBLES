from abc import ABC, abstractmethod
class MinoristaRepository(ABC):
    @abstractmethod
    def saveDataBase(self):
        pass
    @abstractmethod
    def saveDataToDta(self):
        pass