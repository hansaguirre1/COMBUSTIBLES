@echo off


call C:\ProgramData\anaconda3\Scripts\activate.bat


echo Proceso minorista diario

echo Ejecutando: "min0_A2_descarga de precios diarios.py"
python ".\c_minorista_diario\min0_A2_descarga de precios diarios.py"

echo Ejecutando: "min0_A2_Tablas_relacionales.py"
python ".\c_minorista_diario\min0_A2_Tablas_relacionales.py"

echo Ejecutando: "min1_A2_data_quality_imputacion.py"
python ".\c_minorista_diario\min1_A2_data_quality_imputacion.py"

echo Proceso georderenciación

echo Ejecutando: "dis3_distancias.py"
python ".\b_mayoristas y merge\dis3_distancias.py"

echo #merge mayorista minorista

echo Ejecutando: "min4_A2_merge diario copy.py"
python ".\c_minorista_diario\min4_A2_merge diario copy.py"


echo separación en tablñas independientes

echo Ejecutando: "minfut4_separacion.py"
python ".\c_minorista_diario\minfut4_separacion.py"

echo Proceso completado.
pause