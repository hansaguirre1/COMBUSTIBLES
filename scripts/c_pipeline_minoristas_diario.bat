@echo off


call C:\ProgramData\anaconda3\Scripts\activate.bat


echo Proceso minorista diario

echo Ejecutando: "min0_A2_descarga de precios diarios.py"
python ".\c_minorista_diario\min0_A2_descarga de precios diarios.py"

echo Ejecutando: "min0_A2_Tablas_relacionales.py"
python ".\c_minorista_diario\min0_A2_Tablas_relacionales.py"

echo Ejecutando: "min1_A2_data_quality_imputacion.py"
python ".\c_minorista_diario\min1_A2_data_quality_imputacion.py"

#merge mayorista minorista

echo Ejecutando: "min4_A2_merge diario copy.py"
python ".\c_minorista_diario\min4_A2_merge diario copy.py"


echo Proceso completado.
pause