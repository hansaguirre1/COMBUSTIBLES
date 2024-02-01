import hashlib

from pandas import DataFrame
from src.infrastructure.models.mayorista_model import MayoristaModel
from src.infrastructure.models.minorista_model import MinoristaModel
from src.infrastructure.models.may_min_geo_model import MayMinGeoModel
from src.infrastructure.models.combustible_valido_model import CombustibleValidoModel
from src.infrastructure.models.relapasa_model import RelapasaModel
from src.infrastructure.models.price_referencia_model import PriceReferenciaModel
from src.infrastructure.models.planta_model import PlantaModel
from src.infrastructure.models.codigoosinerg_model import CodigoosinergModel
from src.infrastructure.models.indicadores_model import IndicadoresModel
from src.infrastructure.models.actividad_model import ActividadModel
from src.infrastructure.models.direccion_model import DireccionModel
from src.infrastructure.models.producto_model import ProductoModel
from src.infrastructure.models.ubicacion_model import UbicacionModel
from src.infrastructure.models.razon_social_model import RazonSocialModel
from src.infrastructure.models.marcador_model import MarcadorModel
from src.infrastructure.models.price_mayoristas_petroperu_model import PricesMayoristasPetroperuModel
from src.domain.datasources.db_datasource import DbDatasource
import pandas as pd
from contextlib import AbstractContextManager
from typing import Callable, List
import unicodedata
import src.infrastructure.datasources.process.util_process as util_process
import re
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy import text, select, func, desc
pathProcessed = 'data/processed'

class DbDatasourceImpl(DbDatasource):
    def __init__(self, session_factory: Callable[..., AbstractContextManager[Session]]) -> None:
        self.session_factory = session_factory
         
    def savePlanta(self, petroperuDataframe: DataFrame):
            
        petroperuDataframe = petroperuDataframe.drop_duplicates(subset=['PLANTAS'], keep='first')
        with self.session_factory() as session:

            for index, row in petroperuDataframe.iterrows():
                planta_name = row.get('PLANTAS', '')

                hash_id = hashlib.sha256(planta_name.encode()).hexdigest()
                
                results = session.query(PlantaModel).get(hash_id)
            
                if not results:
                    plantaModel = PlantaModel(
                        id = hash_id,
                        planta = planta_name,
                    )
                    session.add(plantaModel)
                else:
                    results.updated_at = datetime.now()
                session.commit()
            
            
    def saveMayoristaPetroperu(self, petroperuDataframe: pd.DataFrame):
        newProductsList = []
        product_dict = {}

        with self.session_factory() as session:
            product_data = session.query(ProductoModel).all()
            for product in product_data:
                nomProd = self.convertOnlyLettersAndNumbers(product.nom_prod)
                product_dict[nomProd] = int(product.id)

            for index, row in petroperuDataframe.iterrows():
                precio_con_impuesto = row.get('Precios', '')
                combustible = row.get('Combustible', '')
                planta = row.get('PLANTAS', '')
                fecha = row.get('Fecha', '')
                
                combustibleOnlyLetterNumber = self.convertOnlyLettersAndNumbers(combustible)
                plantaDb= session.query(PlantaModel).filter(PlantaModel.planta == planta).first()
                combustibleId= product_dict.get(combustibleOnlyLetterNumber, 0)
                if(combustibleId == 0):
                    datos_homogeneizado = self.homogenizar_datos(combustible)
                    combustibleOnlyLetterNumber = self.convertOnlyLettersAndNumbers( datos_homogeneizado)
                    combustibleId= product_dict.get(combustibleOnlyLetterNumber, 0)
                    
                if(combustibleId == 0):
                    newProductsList.append(combustible)
                plantaId = plantaDb.id if plantaDb else 0
                
                precio_con_impuesto = str(precio_con_impuesto) if not pd.isna(precio_con_impuesto) else None
                fecha = str(fecha) if not pd.isna(fecha) else None
                
                campos = [
                    str(precio_con_impuesto),
                    str(combustible),
                    str(planta),
                    str(fecha),
                ]
                        
                cadena_unica = '|'.join(campos)

                hash_id = hashlib.sha256(cadena_unica.encode()).hexdigest()
                
                results = session.query(PricesMayoristasPetroperuModel).get(hash_id)
                
                if not results:
                    petroperuEntity = PricesMayoristasPetroperuModel(
                        id = hash_id,
                        precio_con_impuesto = precio_con_impuesto,
                        fecha = fecha,
                        producto_id = combustibleId,
                        planta_id = plantaId
                    )
                    session.add(petroperuEntity)
                else:
                    results.updated_at = datetime.now()
                session.commit()
            print('Productos no encontrados')
            print(newProductsList)
        # self.addNewProduct(newProductsList=newProductsList)
    
    def saveReferenciaOsinergmin(self, df: DataFrame):

        with self.session_factory() as session:
            allProducts = session.query(ProductoModel).all()
                
            products_dict = {self.convertOnlyLettersAndNumbers(producto.nom_prod):producto.id for producto in allProducts}

            for index, row in df.iterrows():
                precio = row.get('Precio', '')
                productoRef = row.get('Producto', '')
                fecha = row.get('Fecha', '')
                
                productoRefOnlyLettersAndNumber = self.convertOnlyLettersAndNumbers(productoRef)
                productoDb = products_dict.get(productoRefOnlyLettersAndNumber, None)

                productoId = productoDb if productoDb else 0
                if not productoDb:
                    responseProductoRef = self.homogenizar_datos(productoRef)
                    # if(responseProductoRef == None) : TODO: por si no encuentra estos combustibles en la tabla producto y se quiere agregar luego.
                    #     newProductsList.append(productoRef)
                    productoRefOnlyLettersAndNumber = self.convertOnlyLettersAndNumbers(responseProductoRef)
                    productoDb = products_dict.get(productoRefOnlyLettersAndNumber, None)

                productoId = productoDb if productoDb else 0
                
                precio = str(precio) if not pd.isna(precio) else None
                fecha = str(fecha) if not pd.isna(fecha) else None
                
                campos = [
                    str(precio),
                    str(productoRef),
                    str(fecha),
                ]
                        
                cadena_unica = '|'.join(campos)

                hash_id = hashlib.sha256(cadena_unica.encode()).hexdigest()
                
                results = session.query(PriceReferenciaModel).get(hash_id)
                
                if not results:
                    priceEntity = PriceReferenciaModel(
                        id = hash_id,
                        precio = precio,
                        fecha_registro = fecha,
                        producto_id = productoId,
                    )
                    session.add(priceEntity)
                else:
                    results.updated_at = datetime.now()
                session.commit()
                
                
            # self.addNewProduct(newProductsList=newProductsList)
    
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
        with self.session_factory() as session:
            for index, row in marcadoresDataframe.iterrows():
                tipo_cambio = row.get('TC', '')
                wti = row.get('WTI', '')
                mont_belvieu = row.get('MontBelvieu', '')
                fecha = row.get('Fecha', '')
                
                tipo_cambio = str(tipo_cambio) if not pd.isna(tipo_cambio) else None
                wti = str(wti) if not pd.isna(wti) else None
                mont_belvieu = str(mont_belvieu) if not pd.isna(mont_belvieu) else None
                fecha = str(fecha) if not pd.isna(fecha) else None
                
                campos = [
                    str(tipo_cambio),
                    str(wti),
                    str(mont_belvieu),
                    str(fecha),
                ]
                        
                cadena_unica = '|'.join(campos)

                hash_id = hashlib.sha256(cadena_unica.encode()).hexdigest()
                
                results = session.query(MarcadorModel).get(hash_id)
                
                if not results:
                    marcadorModel = MarcadorModel(
                        id = hash_id,
                        tipo_cambio = tipo_cambio,
                        wti = wti,
                        mont_belvieu = mont_belvieu,
                        fecha = fecha,
                    )
                    session.add(marcadorModel)
                else:
                    results.updated_at = datetime.now()
                session.commit()
            
        
    def saveCodigoOsinerg(self):
        coDataframe = pd.read_csv(f"{pathProcessed}/df_codigoosinerg.csv", sep=';')
        coDataframe = coDataframe.drop_duplicates(subset=['ID_COD'], keep='first')
        with self.session_factory() as session:
            
            for index, row in coDataframe.iterrows():
                codigo_osinerg = row.get('CODIGOOSINERG', '')
                id = row.get('ID_COD', '')
                
                codigo_osinerg = codigo_osinerg if not pd.isna(codigo_osinerg) else None
                id = id if not pd.isna(id) else 0
                
                id = self.validate_and_convert_to_int(id)
                
                results = session.query(CodigoosinergModel).get(id)
                if not results:
                    coModel = CodigoosinergModel(
                        id = id,
                        codigo_osinerg = codigo_osinerg,
                    )
                    if (coModel.id > 0):
                        session.add(coModel)
                else:
                    results.updated_at = datetime.now()
                session.commit()
            
    def saveRazonSocial(self):
        rsDataframe = pd.read_csv(f"{pathProcessed}/df_razon_social.csv", sep=';')
        rsDataframe = rsDataframe.drop_duplicates(subset=['ID_RS'], keep='first')
        with self.session_factory() as session:
            for index, row in rsDataframe.iterrows():
                
                razon_social = row.get('RAZONSOCIAL', '')
                id = row.get('ID_RS', '')
                
                razon_social = razon_social if not pd.isna(razon_social) else None
                id = id if not pd.isna(id) else 0
                
                id = self.validate_and_convert_to_int(id)
                
                results = session.query(RazonSocialModel).get(id)
                if not results:
                    razonSocialModel = RazonSocialModel(
                        id = id,
                        razon_social = razon_social,
                    )
                    if (razonSocialModel.id > 0):
                        session.add(razonSocialModel)
                else:
                    results.updated_at = datetime.now()
                session.commit()
                
                    
    def saveActivity(self):
        activityDataframe = pd.read_csv(f"{pathProcessed}/df_actividad.csv", sep=';')
        activityDataframe = activityDataframe.drop_duplicates(subset=['COD_ACT'], keep='first')
        with self.session_factory() as session:
            
            for index, row in activityDataframe.iterrows():
                actividad = row.get('ACTIVIDAD', '')
                id = row.get('COD_ACT', '')
                
                actividad = actividad if not pd.isna(actividad) else None
                id = id if not pd.isna(id) else 0
                
                id = self.validate_and_convert_to_int(id)
                
                results = session.query(ActividadModel).get(id)
                
                if not results:
                    activityModel = ActividadModel(
                        actividad = actividad,
                        id = id,
                    )
                    if (activityModel.id > 0):        
                        session.add(activityModel)
                else:
                    results.updated_at = datetime.now()

                session.commit()
                    
    def saveUbication(self):
        ubigeoDataframe = pd.read_csv(f"{pathProcessed}/df_ubicacion.csv", sep=';')
        ubigeoDataframe = ubigeoDataframe.drop_duplicates(subset=['ID_DPD'], keep='first')
        with self.session_factory() as session:
            
            for index, row in ubigeoDataframe.iterrows():
                id =row.get('ID_DPD', '')
                ubigeo =row.get('UBIGEO', '')
                departamento =row.get('departamento', '')
                provincia =row.get('provincia', '')
                distrito =row.get('distrito', '')
                dpd =row.get('DPD', '')
                ubi =row.get('UBI', '')
                p_urban =row.get('pUrban', '')
                rural =row.get('rural', '')
                capital =row.get('Capital', '')
                
                id = id if not pd.isna(id) else 0
                ubigeo = ubigeo if not pd.isna(ubigeo) else None
                departamento = departamento if not pd.isna(departamento) else None
                provincia = provincia if not pd.isna(provincia) else None
                distrito = distrito if not pd.isna(distrito) else None
                dpd = dpd if not pd.isna(dpd) else None
                ubi = ubi if not pd.isna(ubi) else None
                p_urban = p_urban if not pd.isna(p_urban) else None
                rural = rural if not pd.isna(rural) else None
                capital = capital if not pd.isna(capital) else None
                
                id = self.validate_and_convert_to_int(id)
                results = session.query(UbicacionModel).get(id)
                
                if not results:
                    ubicationModel = UbicacionModel(
                        id = id,
                        ubigeo = ubigeo,
                        departamento = departamento,
                        provincia = provincia,
                        distrito = distrito,
                        dpd = dpd,
                        ubi = ubi,
                        p_urban = p_urban,
                        rural = rural,
                        capital = capital,
                    )
                    if (ubicationModel.id > 0) :
                        session.add(ubicationModel)     
                else:
                    results.updated_at = datetime.now()

                session.commit()
                
        
                
    def saveDirection(self):
        directionDataframe = pd.read_csv(f"{pathProcessed}/df_direccion.csv", sep=';')
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
                
                id = self.validate_and_convert_to_int(id)
                results = session.query(DireccionModel).get(id)
                
                if not results:
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
                        session.add(directionModel)
                else:
                    results.updated_at = datetime.now()

                session.commit()
        
    def saveProduct(self):
        productDataframe = pd.read_csv(f"{pathProcessed}/df_producto.csv", sep=';')
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
                # nom_prod = self.quitar_tildes(nom_prod).strip().lower()
                # nom_prod = re.sub(r'[^a-zA-Z0-9]', '', nom_prod)
                
                results = session.query(ProductoModel).get(id)
                
                if not results:
                    productModel = ProductoModel(
                        id = id,
                        nom_prod = nom_prod,
                        unidad = unidad,
                    )
                    if (productModel.id > 0):    
                        session.add(productModel)
                else:
                    results.updated_at = datetime.now()

                session.commit()
    
    def saveIndicadores(self):
        chunksize = 10000
        with self.session_factory() as session:
            fecha_mas_reciente = session.query(IndicadoresModel.fecha_stata).order_by(desc(IndicadoresModel.fecha_stata)).first()
        
        dfIndicadores = pd.read_csv(f"{pathProcessed}/df_indicadores_sm.csv", sep=';', chunksize=chunksize)
        if fecha_mas_reciente:
            fecha_mas_reciente = fecha_mas_reciente[0]
        else :
            fecha_mas_reciente = datetime(2000, 1, 1)

        print(f'la fecha más reciente es {fecha_mas_reciente}')
        
        with self.session_factory() as session:  
            
            for dfIndicadores_chunk in dfIndicadores:
                dfIndicadores_chunk['fecha_stata'] = pd.to_datetime(dfIndicadores_chunk['fecha_stata'])

                dfIndicadores_chunk = dfIndicadores_chunk[dfIndicadores_chunk['fecha_stata'] > fecha_mas_reciente]
                
                print(f'dfIndicadores_chunk {len(dfIndicadores_chunk)} ')

                for index, row in dfIndicadores_chunk.iterrows():
                        
                    id_dir = row.get('ID_DIR', '')
                    fecha_stata = row.get('fecha_stata', '')
                    precioventa = row.get('PRECIOVENTA', '')
                    precioventa_ = row.get('PRECIOVENTA_', '')
                    dias_faltantes = row.get('dias_faltantes', '')
                    cod_prod = row.get('COD_PROD', '')
                    id_col = row.get('ID_COL', '')
                    dprecioventa = row.get('dPRECIOVENTA', '')
                    dvarprecioventa = row.get('dvarPRECIOVENTA', '')
                    raro = row.get('raro', '')
                    raro2 = row.get('raro2', '')
                    departamento = row.get('DEPARTAMENTO', '')
                    mirar = row.get('mirar', '')
                    markup_mm = row.get('markup_mm', '')
                    precioventa_may = row.get('PRECIOVENTA_may', '')
                    
                    fecha_stata = pd.to_datetime(fecha_stata, errors='coerce')
                    indicadorModel = IndicadoresModel(
                        id_dir = id_dir,
                        fecha_stata = fecha_stata,
                        precioventa = precioventa,
                        precioventa_ = precioventa_,
                        dias_faltantes = dias_faltantes,
                        cod_prod = cod_prod,
                        id_col = id_col,
                        dprecioventa = dprecioventa,
                        dvarprecioventa = dvarprecioventa,
                        raro = raro,
                        raro2 = raro2,
                        departamento = departamento,
                        mirar = mirar,
                        markup_mm = markup_mm,
                        precioventa_may = precioventa_may,
                    )
                    session.add(indicadorModel)
                session.commit()
                    
            
    def validate_and_convert_to_int(self,value):
        try:
            return int(value)
        except ValueError:
            print(f'error transform string to int ({value}) validate_and_convert_to_int()')
            return 0
    def quitar_tildes(self, texto):
        return ''.join(
            c for c in unicodedata.normalize('NFD', texto)
            if unicodedata.category(c) != 'Mn'
        )
    
    def convertOnlyLettersAndNumbers(self, value: str) -> str: 
        
        if not isinstance(value, str):
            return ''
        if(value == ''):
            return ''
        newValue = self.quitar_tildes(value).strip().lower()
        newValue = re.sub(r'[^a-zA-Z0-9]', '', newValue)
        
        return newValue
        
    def convertSqlOnlyLettersAndNumbers(self, column):
        # Implementa la lógica de convertOnlyLettersAndNumbers aquí
        
        # Eliminar espacios en blanco
        column = func.replace(column, ' ', '')
        # Quiter tildes
        column = func.translate(column, "áéíóúüñÁÉÍÓÚÜÑ", "aeiouunAEIOUUN")

        # Eliminar caracteres no alfanuméricos
        column = func.translate(column, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

        # Convertir a minúsculas
        column = func.lower(column)

        return column
    
    def homogenizar_datos(self, dato):
        conversiones = {
            "GLP-E SOLES/KG": "GLP - E",
            "GLP-G SOLES/KG": "GLP - G",
            "PETRÓLEO INDUSTRIAL 6 G. E.": "PETROLEO INDUSTRIAL Nº 6 GE",
            "PETROLEO INDUSTRIAL 500": "PETROLEO INDUSTRIAL Nº 500",
            "PETROPERU INDUSTRIAL 500": "PETROLEO INDUSTRIAL Nº 500",
            "PETROLEO INDUSTRIAL Nº 6": "PETROLEO INDUSTRIAL Nº 6",
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
        print(f'new products {newProductsList}')
        with self.session_factory() as session:
            consulta = select([func.max(ProductoModel.id)])
            max_id = session.execute(consulta).scalar()
            newProductsList = list(set(newProductsList))
            for newProduct in newProductsList:
                max_id += 1
                productNewValue = self.convertOnlyLettersAndNumbers(newProduct)
                productModel = ProductoModel(
                    id = max_id,
                    nom_prod = productNewValue
                )
                session.add(productModel)
            session.commit()
    def saveCombustibleValido(self):
        cvDataframe = pd.read_csv(f"{pathProcessed}/df_validos_dpt.csv")
        with self.session_factory() as session:            
            
            for index, row in cvDataframe.iterrows():
                id_cv = row.get("ID", '')
                anio = row.get("AÑO", '')
                departamento = row.get("DEPARTAMENTO", '')
                producto_id = row.get("COD_PROD", '')
                ok = row.get("ok", '')
                
                id_cv = str(id_cv) if not pd.isna(id_cv) else ''
                anio = str(anio) if not pd.isna(anio) else ''
                departamento = str(departamento) if not pd.isna(departamento) else ''
                producto_id = producto_id if not pd.isna(producto_id) else 0
                ok = str(ok) if not pd.isna(ok) else ''
                
                if (producto_id == 0):    
                    continue
                producto_id = str(producto_id).replace('.0', '')
                campos = [
                    str(id_cv),
                    str(anio),
                    str(departamento),
                    str(producto_id),
                    str(ok),
                ]
                    
                cadena_unica = '|'.join(campos)

                hash_id = hashlib.sha256(cadena_unica.encode()).hexdigest()
                
                results = session.query(CombustibleValidoModel).get(hash_id)
                
                producto_id = self.validate_and_convert_to_int(producto_id)
                if not results:
                    combustibleValidoModel = CombustibleValidoModel(
                        id = hash_id,
                        id_cv = id_cv,
                        anio = anio,
                        departamento = departamento,
                        ok = ok,
                        producto_id = producto_id,
                    )
                    session.add(combustibleValidoModel)
                else:
                    results.updated_at = datetime.now()
            session.commit()
            
    def saveMayMinGeo(self):
        mmDataframe = pd.read_csv(f"{pathProcessed}/df_may_min_geo.csv", sep=';')
        with self.session_factory() as session:            
            
            for index, row in mmDataframe.iterrows():
                
                id_cod = row.get('ID_COD', '')
                id_rs = row.get('ID_RS', '')
                cod_act = row.get('COD_ACT', '')
                id_dpd = row.get('ID_DPD', '')
                direccion = row.get('DIRECCION', '')
                lat = row.get('lat', '')
                lon = row.get('lon', '')
                ruc = row.get('RUC', '')
                razonsocial_geo = row.get('RAZONSOCIAL_geo', '')
                minorista = row.get('minorista', '')
                id_dir = row.get('ID_DIR', '')
                codigoosinerg2 = row.get('CODIGOOSINERG2', '')
                cod_prod = row.get('COD_PROD', '')
                ruc_prov = row.get('RUC-prov', '')
                
                campos = [
                    str(id_cod),
                    str(id_rs),
                    str(cod_act),
                    str(id_dpd),
                    str(direccion),
                    str(lat),
                    str(lon),
                    str(ruc),
                    str(razonsocial_geo),
                    str(minorista),
                    str(id_dir),
                    str(codigoosinerg2),
                    str(cod_prod),
                    str(ruc_prov),
                ]
                    
                cadena_unica = '|'.join(campos)

                hash_id = hashlib.sha256(cadena_unica.encode()).hexdigest()
                
                results = session.query(MayMinGeoModel).get(hash_id)
                
                if not results:
                    mayMinModel = MayMinGeoModel(
                        id = hash_id,
                        id_cod = id_cod,
                        id_rs = id_rs,
                        cod_act = cod_act,
                        id_dpd = id_dpd,
                        direccion = direccion,
                        lat = lat,
                        lon = lon,
                        ruc = ruc,
                        razonsocial_geo = razonsocial_geo,
                        minorista = minorista,
                        id_dir = id_dir,
                        codigoosinerg2 = codigoosinerg2,
                        cod_prod = cod_prod,
                        ruc_prov = ruc_prov,
                    )
                    session.add(mayMinModel)
                else:
                    results.updated_at = datetime.now()
            session.commit()
            
    def saveMinorista(self):
        chunksize = 10000
        
        dfMinorista = pd.read_csv(f"{pathProcessed}/df_minorista.csv", sep=';', chunksize=chunksize)
        
        with self.session_factory() as session:
            for dfMinorista_chunk in dfMinorista:
                print(f'dfMinorista_chunk_chunk {len(dfMinorista_chunk)} ')

                for index, row in dfMinorista_chunk.iterrows():

                    id = row.get('ID_fin', '')
                    id_dir = row.get('ID_DIR', '')
                    cod_prod = row.get('COD_PROD', '')
                    fecha_stata = row.get('fecha_stata', '')
                    precio_venta = row.get('PRECIOVENTA', '')
                    
                    minoristaModel = MinoristaModel(
                        id = id,
                        id_dir = id_dir,
                        cod_prod = cod_prod,
                        fecha_stata = fecha_stata,
                        precio_venta = precio_venta,
                    )
                    session.merge(minoristaModel)
                session.commit()
                    # results = session.query(MinoristaModel).get(id)
                    
                    # if not results:
                    #     minoristaModel = MinoristaModel(
                    #         id = id,
                    #         id_dir = id_dir,
                    #         cod_prod = cod_prod,
                    #         fecha_stata = fecha_stata,
                    #         precio_venta = precio_venta,
                    #     )
                    #     session.add(minoristaModel)
                    # else:
                    #     results.updated_at = datetime.now()
                    # session.commit()
        
    def saveMayorista(self):
        chunksize = 10000
        
        dfMayorista = pd.read_csv(f"{pathProcessed}/df_mayorista.csv", sep=';', chunksize=chunksize)
        
        with self.session_factory() as session:
            for dfMayorista_chunk in dfMayorista:
                print(f'dfMayorista_chunk_chunk {len(dfMayorista_chunk)} ')

                for index, row in dfMayorista_chunk.iterrows():

                    id = row.get('ID_fin', '')
                    
                    id_dir = row.get('ID_DIR', '')
                    cod_prod = row.get('COD_PROD', '')
                    fecha_stata = row.get('fecha_stata', '')
                    precio_venta_may = row.get('PRECIOVENTA_may', '')
                    
                    mayoristaModel = MayoristaModel(
                        id = id,
                        id_dir = id_dir,
                        cod_prod = cod_prod,
                        fecha_stata = fecha_stata,
                        precio_venta_may = precio_venta_may,
                    )
                    session.merge(mayoristaModel)
                session.commit()
                    # results = session.query(MayoristaModel).get(id)
                    
                    # if not results:
                    #     minoristaModel = MayoristaModel(
                    #         id = id,
                    #         precio_venta_may = precio_venta_may,
                    #     )
                    #     session.add(minoristaModel)
                    # else:
                    #     results.updated_at = datetime.now()
                    # session.commit()