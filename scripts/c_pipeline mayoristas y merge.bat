cd E:\GitHub\COMBUSTIBLES\scripts

call C:\ProgramData\anaconda3\Scripts\activate.bat

#Proceso Mayoristas

echo Ejecutando: "ma0_ descarga de información mayoristas.py"
python "ma0_ descarga de información mayoristas.py"

echo Ejecutando: "ma1_ join y limpieza de información.py"
python "ma1_ join y limpieza de información.py"

echo Ejecutando: "ma2_ imputación de precios.py"
python "ma2_ imputación de precios.py"

#merge mayorista minorista

echo Ejecutando: "min4_A1_merges.py"
python "min4_A1_merge.py"
