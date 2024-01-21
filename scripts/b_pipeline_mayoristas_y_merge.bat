cd E:\GitHub\COMBUSTIBLES\scripts

call C:\ProgramData\anaconda3\Scripts\activate.bat

#Proceso Mayoristas




echo Ejecutando: "ma1_ join y limpieza de informacion.py"
python "ma1_ join y limpieza de informacion.py"

echo Ejecutando: "ma2_ imputación de precios.py"
python "ma2_ imputación de precios.py"

#Proceso georderenciación

echo Ejecutando: "dis3_distancias.py"
python "dis3_distancias.py"

#merge mayorista minorista

echo Ejecutando: "min4_A1_merges.py"
python "min4_A1_merge.py"


echo Proceso completado.
pause