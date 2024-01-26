from abc import ABC, abstractmethod

class LatLngMayoristaRepository(ABC):
    @abstractmethod
    def saveDataLatLng(self):
        pass