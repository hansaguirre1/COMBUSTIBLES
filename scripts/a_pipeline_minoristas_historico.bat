@echo off



call C:\ProgramData\anaconda3\Scripts\activate.bat

# UBIGEO SIMPLE

echo Ejecutando: "ubi0_simple.py"
python "./a_minorista_historico/ubi0_simple.py"

# Minoristas historico
echo Ejecutando: "min0_A1_Tablas_relacionales.py"
python  "./a_minorista_historico/min0_A1_Tablas_relacionales.py"

echo Ejecutando: "min1_A1_data_quality_imputacion.py"
python  "./a_minorista_historico/min1_A1_data_quality_imputacion.py"

echo Proceso completado.
pause