cd E:\GitHub\COMBUSTIBLES\scripts

call C:\ProgramData\anaconda3\Scripts\activate.bat

#Combustibles validos

echo Ejecutando: "cv0_ Descarga de información .py"
python "cv0_ Descarga de información .py"

echo Ejecutando: "cv1_ Lectura join y limpieza de nuevos valores.py"
python "cv1_ Lectura join y limpieza de nuevos valores.py"

echo Ejecutando: "cv2_comb_validos.py"
python "cv2_comb_validos.py"
