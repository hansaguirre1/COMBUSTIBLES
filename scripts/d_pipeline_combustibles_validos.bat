

call C:\ProgramData\anaconda3\Scripts\activate.bat

echo Combustibles validos

echo Ejecutando: "cv0_ Descarga de información .py"
python ".\d_combustibles validos\cv0_ Descarga de información .py"

echo Ejecutando: "cv1_ Lectura join y limpieza de nuevos valores.py"
python ".\d_combustibles validos\cv1_ Lectura join y limpieza de nuevos valores.py"

echo Ejecutando: "cv2_comb_validos.py"
python ".\d_combustibles validos\cv2_comb_validos.py"


echo Proceso completado.
pause