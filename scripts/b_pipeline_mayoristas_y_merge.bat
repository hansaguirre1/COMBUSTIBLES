

call C:\ProgramData\anaconda3\Scripts\activate.bat

#Proceso Mayoristas




echo Ejecutando: "ma1_ join y limpieza de informacion.py"
python ".\b_mayoristas y merge\ma1_ join y limpieza de informacion.py"

echo Ejecutando: "ma2_ imputación de precios.py"
python ".\b_mayoristas y merge\ma2_ imputación de precios.py"

#Proceso georderenciación

echo Ejecutando: "dis3_distancias.py"
python ".\b_mayoristas y merge\dis3_distancias.py"

#merge mayorista minorista

echo Ejecutando: "min4_A1_merges.py"
python ".\b_mayoristas y merge\min4_A1_merge.py"


echo Proceso completado.
pause