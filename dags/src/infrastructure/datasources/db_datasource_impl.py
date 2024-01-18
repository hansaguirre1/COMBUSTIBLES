from pandas import DataFrame
from src.infrastructure.models.relapasa_model import RelapasaModel
from src.infrastructure.models.price_referencia_model import PriceReferenciaModel
from src.infrastructure.models.planta_model import PlantaModel
from src.infrastructure.models.codigoosinerg_model import CodigoosinergModel
from src.infrastructure.models.price_model import PriceModel
from src.infrastructure.models.actividad_model import ActividadModel
from src.infrastructure.models.direccion_model import DireccionModel
from src.infrastructure.models.producto_model import ProductoModel
from src.infrastructure.models.ubicacion_model import UbicacionModel
from src.infrastructure.models.razon_social_model import RazonSocialModel
from src.infrastructure.models.marcador_model import MarcadorModel
from src.infrastructure.models.price_mayoristas_model import PricesMayoristasModel
from src.domain.datasources.db_datasource import DbDatasource
import pandas as pd
from contextlib import AbstractContextManager
from typing import Callable, List
import unicodedata
import src.infrastructure.datasources.process.util_process as util_process
import re

from sqlalchemy.orm import Session
from sqlalchemy import text, select, func
pathMinorista = 'data/minoristas'

class DbDatasourceImpl(DbDatasource):
    def __init__(self, session_factory: Callable[..., AbstractContextManager[Session]]) -> None:
        self.session_factory = session_factory
         
    def savePlanta(self, petroperuDataframe: DataFrame):
        newPlantasList: List[PlantaModel] = []
        
        with self.session_factory() as session:
            allPlanta = session.query(PlantaModel).all()
            
        petroperuDataframe = petroperuDataframe.drop_duplicates(subset=['PLANTAS'], keep='first')

        for index, row in petroperuDataframe.iterrows():
            planta_name = row.get('PLANTAS', '')

            
            planta_name = planta_name if not pd.isna(planta_name) else None
            
            results = list(filter(
                lambda planta: (
                    planta.planta == planta_name 
                ),
                allPlanta
            ))
            if not results:
                plantaModel = PlantaModel(
                    planta = planta_name,
                )
                newPlantasList.append(plantaModel)
                
        with self.session_factory() as session:
            session.add_all(newPlantasList)
            session.commit()
            
            
    def saveMayoristaPetroperu(self, petroperuDataframe: pd.DataFrame):
        newProductsList: List[str] = []
        newPetroperuList: List[PricesMayoristasModel] = []
        with self.session_factory() as session:
            allPetroperu = session.query(PricesMayoristasModel).all()
        for index, row in petroperuDataframe.iterrows():
            precio_con_impuesto = row.get('Precios', '')
            combustible = row.get('Combustible', '')
            planta = row.get('PLANTAS', '')
            fecha = row.get('Fecha', '')
            
            combustibleOnlyLetterNumber = re.sub(r'[^a-zA-Z0-9]', '', combustible).lower()
            plantaDb= session.query(PlantaModel).filter(PlantaModel.planta == planta).first()
            combustibleDb= session.query(ProductoModel).filter(ProductoModel.nom_prod == combustibleOnlyLetterNumber).first()
            combustibleId = combustibleDb.id if combustibleDb else 0
            if(combustibleId == 0):
                datos_homogeneizado = self.homogenizar_datos(combustible)
                if(datos_homogeneizado == None) :
                    newProductsList.append(combustible)
                    datos_homogeneizado = combustible
                combustibleOnlyLetterNumber = re.sub(r'[^a-zA-Z0-9]', '', datos_homogeneizado).lower()
                combustibleDb= session.query(ProductoModel).filter(ProductoModel.nom_prod == datos_homogeneizado).first()
                
            combustibleId = combustibleDb.id if combustibleDb else 0
            plantaId = plantaDb.id if plantaDb else 0
            
            precio_con_impuesto = str(precio_con_impuesto) if not pd.isna(precio_con_impuesto) else None
            fecha = str(fecha) if not pd.isna(fecha) else None
            
            results = list(filter(
                lambda petroperu: (
                    petroperu.precio_con_impuesto == precio_con_impuesto and
                    petroperu.producto_id == combustibleId and
                    petroperu.planta_id == plantaId
                ),
                allPetroperu
            ))
            if not results:
                petroperuEntity = PricesMayoristasModel(
                    precio_con_impuesto = precio_con_impuesto,
                    fecha = fecha,
                    producto_id = combustibleId,
                    planta_id = plantaId
                )
                if(combustibleId > 0 and plantaId > 0):
                    newPetroperuList.append(petroperuEntity)
                
        with self.session_factory() as session:
            session.add_all(newPetroperuList)
            session.commit()
        self.addNewProduct(newProductsList=newProductsList)
    
    def saveReferenciaOsinergmin(self, df: DataFrame):
        newReferenciaList: List[PriceReferenciaModel] = []
        newProductsList: List[str] = []

        with self.session_factory() as session:
            allReferencia = session.query(PriceReferenciaModel).all()
            allProducts = session.query(ProductoModel).all()
            
        def mi_filtro(producto, valor_a_comparar):
            valor_a_comparar = re.sub(r'[^a-zA-Z0-9]', '', valor_a_comparar)

            findProduct =  producto.nom_prod == valor_a_comparar.replace(' ','').strip().lower()
            if(findProduct) : return producto
            else: return None
            
        for index, row in df.iterrows():
            precio = row.get('Precio', '')
            productoRef = row.get('Producto', '')
            fecha = row.get('Fecha', '')
            
            productoDb = next((producto for producto in allProducts if mi_filtro(producto, productoRef)), None)

            if not productoDb:
                responseProductoRef = self.homogenizar_datos(productoRef)
                if(responseProductoRef == None) :
                    newProductsList.append(productoRef)
                    responseProductoRef = productoRef
                productoDb = next((producto for producto in allProducts if mi_filtro(producto, responseProductoRef)), None)

            productoId = productoDb.id if productoDb else 0
            
            precio = str(precio) if not pd.isna(precio) else None
            fecha = str(fecha) if not pd.isna(fecha) else None
            results = list(filter(
                lambda referencia: (
                    referencia.precio == precio and
                    referencia.producto_id == productoId and 
                    referencia.fecha_registro == fecha
                ),
                allReferencia
            ))
            if not results:
                priceEntity = PriceReferenciaModel(
                    precio = precio,
                    fecha_registro = fecha,
                    producto_id = productoId,
                )
                if(productoId > 0):
                    newReferenciaList.append(priceEntity)
                
        with self.session_factory() as session:
            session.add_all(newReferenciaList)
            session.commit()
        self.addNewProduct(newProductsList=newProductsList)
    
    def saveRelapasa(self, df_combinado: DataFrame) -> DataFrame:
        relapasaDataframe = df_combinado
        newRelapasaList: List[RelapasaModel] = []
        relapasaListToSaveCsv = []

        with self.session_factory() as session:
            allRelapasa = session.query(RelapasaModel).all()
        for index, row in relapasaDataframe.iterrows():
            id_dir = row.get('ID_DIR', '')
            precio_venta = row.get('PRECIOVENTA', '')
            cod_prod = row.get('COD_PROD', '')
            fecha = row.get('FECHADEREGISTRO', '')
            
            id_dir = self.validate_and_convert_to_int(id_dir)
            if(id_dir == 0):
                continue
            dirExist = session.query(DireccionModel).filter(
                DireccionModel.id == id_dir,
                DireccionModel.codigoosinerg_id == 9926,
            ).first()
            
            if not dirExist:
                continue
            else:
                cod_prod = self.validate_and_convert_to_int(cod_prod)
                precio_venta = str(precio_venta) if not pd.isna(precio_venta) else None
                fecha = str(fecha) if not pd.isna(fecha) else None
                
                results = list(filter(
                    lambda relapasa: (
                        relapasa.producto_id == cod_prod and
                        relapasa.precio == precio_venta and
                        relapasa.fecha_registro == fecha
                    ),
                    allRelapasa
                ))
                if not results:
                    if(cod_prod > 0):
                        marcadorModel = RelapasaModel(
                            producto_id = cod_prod,
                            precio = precio_venta,
                            fecha_registro = fecha,
                        )
                        newRelapasaList.append(marcadorModel)
                        dataNewRelapasa = {
                            "ID_DIR" : id_dir,
                            "PRECIOVENTA" : precio_venta,
                            "COD_PROD" : cod_prod,
                            "FECHADEREGISTRO" : fecha,
                        }
                        relapasaListToSaveCsv.append(dataNewRelapasa)
                
        with self.session_factory() as session:
            session.add_all(newRelapasaList)
            session.commit()
        
        return pd.DataFrame(relapasaListToSaveCsv)
    
    def saveMarcadores(self, marcadoresDataframe: pd.DataFrame):
        newMarcadorList: List[MarcadorModel] = []
        with self.session_factory() as session:
            allMarcadores = session.query(MarcadorModel).all()
        for index, row in marcadoresDataframe.iterrows():
            tipo_cambio = row.get('TC', '')
            wti = row.get('WTI', '')
            mont_belvieu = row.get('MontBelvieu', '')
            fecha = row.get('Fecha', '')
            
            tipo_cambio = str(tipo_cambio) if not pd.isna(tipo_cambio) else None
            wti = str(wti) if not pd.isna(wti) else None
            mont_belvieu = str(mont_belvieu) if not pd.isna(mont_belvieu) else None
            fecha = str(fecha) if not pd.isna(fecha) else None
            
            results = list(filter(
                lambda marcador: (
                    marcador.tipo_cambio == tipo_cambio and
                    marcador.wti == wti and
                    marcador.mont_belvieu == mont_belvieu and
                    marcador.fecha == fecha
                ),
                allMarcadores
            ))
            if not results:
                marcadorModel = MarcadorModel(
                    tipo_cambio = tipo_cambio,
                    wti = wti,
                    mont_belvieu = mont_belvieu,
                    fecha = fecha,
                )
                newMarcadorList.append(marcadorModel)
                
        with self.session_factory() as session:
            session.add_all(newMarcadorList)
            session.commit()
        
    def saveCodigoOsinerg(self):
        coDataframe = pd.read_csv(f"{pathMinorista}/df_codigoosinerg.csv")
        coDataframe = coDataframe.drop_duplicates(subset=['ID_COD'], keep='first')
        with self.session_factory() as session:
            
            for index, row in coDataframe.iterrows():
                codigo_osinerg = row.get('CODIGOOSINERG', '')
                id = row.get('ID_COD', '')
                
                codigo_osinerg = codigo_osinerg if not pd.isna(codigo_osinerg) else None
                id = id if not pd.isna(id) else 0
                
                id = self.validate_and_convert_to_int(id)
                
                coModel = CodigoosinergModel(
                    id = id,
                    codigo_osinerg = codigo_osinerg,
                )
                if (coModel.id > 0):
                    session.merge(coModel)
            session.commit()
            
    def saveRazonSocial(self):
        rsDataframe = pd.read_csv(f"{pathMinorista}/df_razon_social.csv")
        rsDataframe = rsDataframe.drop_duplicates(subset=['ID_RS'], keep='first')
        with self.session_factory() as session:
            for index, row in rsDataframe.iterrows():
                razon_social = row.get('RAZONSOCIAL', '')
                id = row.get('ID_RS', '')
                
                razon_social = razon_social if not pd.isna(razon_social) else None
                id = id if not pd.isna(id) else 0
                
                id = self.validate_and_convert_to_int(id)
                
                razonSocialModel = RazonSocialModel(
                    razon_social = razon_social,
                    id = id,
                )
                if (razonSocialModel.id > 0):
                    session.merge(razonSocialModel)
                    
            session.commit()
                    
    def saveActivity(self):
        activityDataframe = pd.read_csv(f"{pathMinorista}/df_actividad.csv")
        activityDataframe = activityDataframe.drop_duplicates(subset=['COD_ACT'], keep='first')
        with self.session_factory() as session:
            
            for index, row in activityDataframe.iterrows():
                actividad = row.get('ACTIVIDAD', '')
                id = row.get('COD_ACT', '')
                
                actividad = actividad if not pd.isna(actividad) else None
                id = id if not pd.isna(id) else 0
                
                id = self.validate_and_convert_to_int(id)
                
                activityModel = ActividadModel(
                        actividad = actividad,
                        id = id,
                    )
                if (activityModel.id > 0):        
                    session.merge(activityModel)
            session.commit()
                    
    def saveUbication(self):
        ubigeoDataframe = pd.read_csv(f"{pathMinorista}/df_ubicacion.csv")
        ubigeoDataframe = ubigeoDataframe.drop_duplicates(subset=['ID_DPD'], keep='first')
        with self.session_factory() as session:
            
            for index, row in ubigeoDataframe.iterrows():
                id =row.get('ID_DPD', '')
                departamento =row.get('departamento', '')
                provincia =row.get('provincia', '')
                distrito =row.get('distrito', '')
                dpd =row.get('DPD', '')
                ubigeo =row.get('UBIGEO', '')
                
                id = id if not pd.isna(id) else 0
                departamento = departamento if not pd.isna(departamento) else None
                provincia = provincia if not pd.isna(provincia) else None
                distrito = distrito if not pd.isna(distrito) else None
                dpd = dpd if not pd.isna(dpd) else None
                ubigeo = ubigeo if not pd.isna(ubigeo) else None
                
                id = self.validate_and_convert_to_int(id)
                
                ubicationModel = UbicacionModel(
                    id = id,
                    departamento = departamento,
                    provincia = provincia,
                    distrito = distrito,
                    dpd = dpd,
                    ubigeo = ubigeo,
                    )
                if (ubicationModel.id > 0) :
                    session.merge(ubicationModel)
            session.commit()
        
                
    def saveDirection(self):
        directionDataframe = pd.read_csv(f"{pathMinorista}/df_direccion.csv")
        directionDataframe = directionDataframe.drop_duplicates(subset=['ID_DIR'], keep='first')
        with self.session_factory() as session:
            
            for index, row in directionDataframe.iterrows():
                id = row.get('ID_DIR', '')
                direccion_name = row.get('DIRECCION', '')
                latitude = row.get('lat', '')
                longitude = row.get('lon', '')
                razon_id = row.get('ID_RS', '')
                actividad_id = row.get('COD_ACT', '')
                ubicacion_id = row.get('ID_DPD', '')
                codigoosinerg_id = row.get('ID_COD', '')
                
                id = id if not pd.isna(id) else 0
                direccion_name = direccion_name if not pd.isna(direccion_name) else None
                latitude = latitude if not pd.isna(latitude) else None
                longitude = longitude if not pd.isna(longitude) else None
                razon_id = razon_id if not pd.isna(razon_id) else None
                actividad_id = actividad_id if not pd.isna(actividad_id) else None
                ubicacion_id = ubicacion_id if not pd.isna(ubicacion_id) else None
                codigoosinerg_id = codigoosinerg_id if not pd.isna(codigoosinerg_id) else None
                
                id = self.validate_and_convert_to_int(id)
                
                directionModel = DireccionModel(
                    id = id,
                    direccion_name = direccion_name,
                    latitude = latitude,
                    longitude = longitude,
                    razon_id = razon_id,
                    actividad_id = actividad_id,
                    ubicacion_id = ubicacion_id,
                    codigoosinerg_id = codigoosinerg_id,
                )
                if (directionModel.id > 0):    
                    session.merge(directionModel)
            session.commit()
        
    def saveProduct(self):
        productDataframe = pd.read_csv(f"{pathMinorista}/df_producto.csv")
        with self.session_factory() as session:            
            productDataframe = productDataframe.drop_duplicates(subset=['COD_PROD'], keep='first')
            
            for index, row in productDataframe.iterrows():
                id = row.get("COD_PROD", '')
                nom_prod = row.get("NOM_PROD", '')
                unidad = row.get("UNIDAD", '')
                
                id = id if not pd.isna(id) else 0
                nom_prod = nom_prod if not pd.isna(nom_prod) else None
                unidad = unidad if not pd.isna(unidad) else None
                
                id = self.validate_and_convert_to_int(id)
                nom_prod = self.quitar_tildes(nom_prod).strip().lower()
                nom_prod = re.sub(r'[^a-zA-Z0-9]', '', nom_prod)
                
                productModel = ProductoModel(
                    id = id,
                    nom_prod = nom_prod,
                    unidad = unidad,
                )
                if (productModel.id > 0):    
                    session.merge(productModel)
            session.commit()
    
    def savePrice(self, data: DataFrame):
        
        with self.session_factory() as session:

            for index, row in data.iterrows():
                
                producto_id = row.get('COD_PROD', '')
                direccion_id = row.get('ID_DIR', '')
                fecha_registro = row.get('FECHADEREGISTRO', '')
                hora_registro = row.get('HORADEREGISTRO', '')
                precio = row.get('PRECIOVENTA', '')
                
                producto_id = producto_id if not pd.isna(producto_id) else 0
                direccion_id = direccion_id if not pd.isna(direccion_id) else 0
                fecha_registro = str(fecha_registro) if not pd.isna(fecha_registro) else None
                hora_registro = str(hora_registro) if not pd.isna(hora_registro) else None
                precio = str(precio) if not pd.isna(precio) else None
                
                
                producto_id = self.validate_and_convert_to_int(producto_id)
                direccion_id = self.validate_and_convert_to_int(direccion_id)
                
                existe = session.query(PriceModel).filter(
                    PriceModel.producto_id == producto_id,
                    PriceModel.direccion_id == direccion_id,
                    PriceModel.fecha_registro == fecha_registro,
                    PriceModel.hora_registro == hora_registro,
                    PriceModel.precio == precio
                ).first()
                
                if not existe:
                    priceModel = PriceModel(
                        producto_id = producto_id,
                        direccion_id = direccion_id,
                        fecha_registro = fecha_registro,
                        hora_registro = hora_registro,
                        precio = precio,
                    )
                    if(priceModel.producto_id > 0 and priceModel.direccion_id > 0):
                        session.add(priceModel)
            session.commit()
        
    def validate_and_convert_to_int(self,value):
        try:
            return int(value)
        except ValueError:
            print('error transform string to int validate_and_convert_to_int()')
            return 0
    def quitar_tildes(self, texto):
        return ''.join(
            c for c in unicodedata.normalize('NFD', texto)
            if unicodedata.category(c) != 'Mn'
        )
    
    def homogenizar_datos(self, dato):
        conversiones = {
            "GLP-E SOLES/KG": "GLP - E",
            "GLP-G SOLES/KG": "GLP - G",
            "PETRÓLEO INDUSTRIAL 6 G. E.": "PETROLEO INDUSTRIAL Nº 6 GE",
            "PETROLEO INDUSTRIAL 500": "PETROLEO INDUSTRIAL Nº 500",
            "PETROPERU INDUSTRIAL 500": "PETROLEO INDUSTRIAL Nº 500",
            "PETROPERU INDUSTRIAL Nº 6": "PETROLEO INDUSTRIAL Nº 6",
            "GASOHOL 84": "GASOHOL 84 PLUS",
            "GASOHOL 90": "GASOHOL 90 PLUS",
            "GASOHOL 95": "GASOHOL 95 PLUS",
            "GASOHOL 97": "GASOHOL 97 PLUS",
            "GASOHOL 98": "GASOHOL 98 PLUS",
            "GASOLINA 84 SP": "GASOLINA 84",
            "GASOLINA SUPER 90 SP": "GASOLINA 90",
            "GASOLINA SUPER 90": "GASOLINA 90",
            "GASOLINA SUPER 95 SP": "GASOLINA 95",
            "GASOLINA SUPER EXTRA 97 SP": "GASOLINA 97",
            "DIESEL B5 S-50 (**)": "Diesel B5 S-50",
            "DIESEL B5 UV S-50 (**)": "Diesel B5 S-50 UV",
            "DIESEL B5 UV S-50": "Diesel B5 S-50 UV",
            "DIESEL B5 S-50": "Diesel B5 S-50",
            "DIESEL B5 S-50 G. E.": "Diesel B5 S-50 GE",
            "DIESEL B5 G. E.": "DIESEL B5 GE",
            "DIESEL 2 S-50 UV": "Diesel 2 S-50 UV",
            "DIESEL 2 S-50 S-50": "Diesel 2 S-50",
            "DIESEL 2": "DIESEL 2 UV",
            "Gasohol84": "GASOHOL 84 PLUS",
            "Gasohol90": "GASOHOL 90 PLUS",
            "Gasohol95": "GASOHOL 95 PLUS",
            "Gasohol97": "GASOHOL 97 PLUS",
            "Gasohol98": "GASOHOL 98 PLUS",
            "DieselB5UVS-50": "Diesel B5 S-50 UV",
            "DieselB5UVS‐50": "Diesel B5 S-50 UV",
        }

        return conversiones.get(dato, None)
    
    def addNewProduct(self, newProductsList: List[str]):
        with self.session_factory() as session:
            consulta = select([func.max(ProductoModel.id)])
            max_id = session.execute(consulta).scalar()
            newProductsList = list(set(newProductsList))
            for newProduct in newProductsList:
                max_id += 1
                value = newProduct.strip().lower()
                productNewValue = re.sub(r'[^a-zA-Z0-9]', '', value)
                productModel = ProductoModel(
                    id = max_id,
                    nom_prod = productNewValue
                )
                session.add(productModel)
            session.commit()
