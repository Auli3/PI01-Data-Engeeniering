from sqlalchemy import create_engine
import pandas as pd
import numpy as np

conexion = 'mysql://root:1234@localhost:3306'
Database = 'Base_de_datos'
engine = create_engine(conexion + '/' + Database, echo=True) # I connect to the database

def correcion_index(df): # I create a function to correct the columns order of prices
    index = ['precio', 'sucursal_id', 'producto_id']
    if df.columns.all != index:
        df = df[index]
    return df

class carga_precio(): # I create the class to make the incremental load of prices when the database is already created

    def __init__(self):
        self

    def cargar_precios(self):
        respuesta = input('Desea cargar nuevos datos?:') 
        if respuesta in ['si', 'Si', 'SI', 's', 'S']:
                df_path = input('Ingrese un path:') 
                encoding = input('Ingrese un encoding:') 
                sep = input('Ingrese un separador:') 
                precios_nuevos = pd.read_csv(df_path, encoding=encoding, sep=sep, index_col=False) 
                precios_nuevos.drop(precios_nuevos.columns[0], axis=1) 
                precios_nuevos = correcion_index(precios_nuevos) 
                precios_nuevos['fecha'] = df_path[-12:-4] 
                precios_nuevos['indice'] = range(0,len(precios_nuevos))
                precios_nuevos.indice = precios_nuevos.indice.astype(str) 
                precios_nuevos['indice_id'] = precios_nuevos.indice + precios_nuevos.fecha
                precios_nuevos.indice_id = precios_nuevos.indice_id.astype(np.int64) 
                precios_nuevos.set_index('indice_id', inplace=True) 
                precios_nuevos.drop(columns=('indice'), inplace=True) 
                precios_nuevos.precio = precios_nuevos.precio.astype(str) 
                precios_nuevos.sucursal_id = precios_nuevos.sucursal_id.astype(str)
                precios_nuevos.sucursal_id = precios_nuevos.sucursal_id.str.replace('00:00:00', '') 
                precios_nuevos.producto_id = precios_nuevos.producto_id.astype(str) 
                precios_nuevos.sucursal_id = precios_nuevos.sucursal_id.str.replace('-', '') 
                precios_nuevos.producto_id = precios_nuevos.producto_id.str.replace('-', '') 
                precio_sin_sucursalid = precios_nuevos[precios_nuevos.producto_id == 'nan'].index 
                precios_nuevos.drop(precio_sin_sucursalid, inplace=True) 
                precio_sin_productoid = precios_nuevos[precios_nuevos.producto_id == 'nan'].index 
                precios_nuevos.drop(precio_sin_productoid, inplace=True) 
                precios_sin_precio = precios_nuevos[precios_nuevos.precio == 'nan'].index 
                precios_nuevos.drop(precios_sin_precio, inplace=True) 
                precios_nuevos.sucursal_id = precios_nuevos.sucursal_id.astype(float) 
                precios_nuevos.producto_id = precios_nuevos.producto_id.astype(float)
                precios_nuevos.sucursal_id = precios_nuevos.sucursal_id.astype(np.int64) 
                precios_nuevos.producto_id = precios_nuevos.producto_id.astype(np.int64) 
                precios_nuevos.precio = precios_nuevos.precio.astype(float) 
                precios_nuevos.drop_duplicates(inplace=True) 
                precios_nuevos.to_sql(con=engine, name='precio', if_exists='append') 
        else: print('Vuelva a ejecutar mas tarde si desea cargar nuevos datos')

cargar_precio = carga_precio() # I instance and object of the class
cargar_precio.cargar_precios() # I use the "cargar_precios" method of the class