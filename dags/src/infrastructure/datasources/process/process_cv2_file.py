
def combse(df,cod):
    df_mean = df.groupby(['DEPARTAMENTO', 'AÑO'])['VOLUMENES'].mean().reset_index()
    df_mean = df_mean[df_mean['AÑO'] > 2017]
    df_mean = df_mean.dropna(subset=['VOLUMENES'])
    df_mean["COD_PROD"] = cod
    df_mean = df_mean[df_mean["VOLUMENES"]>1000]
    df_mean.drop(columns=["VOLUMENES"],inplace=True)
    return df_mean
