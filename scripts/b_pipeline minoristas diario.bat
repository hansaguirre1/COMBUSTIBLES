@echo off

cd E:\GitHub\COMBUSTIBLES\scripts

call C:\ProgramData\anaconda3\Scripts\activate.bat

#Proceso minorista diario

echo Ejecutando: "min0_A2_descarga de precios diarios.py"
python "min0_A2_descarga de precios diarios.py"

echo Ejecutando: "min0_A2_Tablas_relacionales.py"
python "min0_A2_Tablas_relacionales.py"

echo Ejecutando: "min1_A2_data_quality_imputacion.py"
python "min1_A2_data_quality_imputacion.py"

#Proceso georderenciación

echo Ejecutando: "dis3_distancias.py"
python "dis3_distancias.py"


echo Proceso completado.
pause