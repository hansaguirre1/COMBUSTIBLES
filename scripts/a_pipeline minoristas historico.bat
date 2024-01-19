@echo off

cd E:\GitHub\COMBUSTIBLES\scripts

call C:\ProgramData\anaconda3\Scripts\activate.bat


echo Ejecutando: "min0_A1_Tablas_relacionales.py"
python "min0_A1_Tablas_relacionales.py"

echo Ejecutando: "min1_A1_data_quality_imputacion.py"
python "min1_A1_data_quality_imputacion.py"

echo Proceso completado.
pause