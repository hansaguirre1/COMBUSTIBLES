import pandas as pd

def verifyNameProductsNotMatchesByReference():
    # Read the CSV files
        referencia_excel = pd.read_excel('data/referencia/Precios_referencia.xls')
        producto_df = pd.read_csv('data/minoristas/df_producto.csv')

        combustibles_excel = set(referencia_excel['Combustible'])
        nombres_prod_csv = set(producto_df['NOM_PROD'])
        valores_faltantes = combustibles_excel - nombres_prod_csv

        valores_faltantes_lista = list(valores_faltantes)

        print('---non_matches--')
        print(valores_faltantes_lista)

def verifyNameProductsNotMatchesByPetroperu():
    # Read the CSV files
    petroperu_df = pd.read_csv('data/petroperu/Petroperu_Lista.csv', sep=';')
    producto_df = pd.read_csv('data/minoristas/df_producto.csv')

    # Compare the 'Combustible' column of petroperu_df with the 'NOM_PROD' column of producto_df
    # non_matches = petroperu_df[petroperu_df['Combustible'].isin(producto_df['NOM_PROD'])]
    merged_df = pd.merge(petroperu_df, producto_df, left_on='Combustible', right_on='NOM_PROD', how='left')
    non_matches = merged_df[merged_df['NOM_PROD'].isnull()]

    non_matches = non_matches.drop_duplicates(subset=['Combustible'])
    print('---non_matches--')
    print(non_matches['Combustible'])
    print('---non_matches--')