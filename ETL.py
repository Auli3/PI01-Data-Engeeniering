from sqlalchemy import create_engine # I import from sqlalchemy the method for the connection
from sqlalchemy.ext.declarative import declarative_base # I import from sqlalchemy the method to declare the base for my tables
from sqlalchemy import Column, String, Integer, Float, ForeignKey, BigInteger # I export from sqlalchemy all the dtype I'll be using
from sqlalchemy.orm import relationship # I import from sqlalchemy the method to make relations between tables
import pandas as pd # I import pandas to work the datasets as dataframes
import numpy as np # I import numpy to work with big numbers and numerical transformations

base = declarative_base() # I instance the base to create my classes(tables)
conexion = 'mysql://root:1234@localhost:3306'
Database = 'Base_de_datos'
engine = create_engine(conexion) # I connect to the server
engine.execute("CREATE DATABASE " + Database) # I create the database

engine = create_engine(conexion + '/' + Database, echo=True) # I connect to the database


def correcion_index(df): # I create a function to correct the columns order of prices
    index = ['precio', 'sucursal_id', 'producto_id']
    if df.columns.all != index:
        df = df[index]
    return df

def nomalizacion_precios(df_path, encoding, sep): # I create a function to normalize the .csv with new prices
        precios_nuevos = pd.read_csv(df_path, encoding=encoding, sep=sep, index_col=False) # I pass the function the file path, the encoding, and the separator to read it
        precios_nuevos.drop(precios_nuevos.columns[0], axis=1) # I drop the unnamed column that comes with the .csv files
        precios_nuevos = correcion_index(precios_nuevos) # I use the columns order function
        precios_nuevos['fecha'] = df_path[-12:-4] # I create the date column from the file name to tell them apart from each other across time
        precios_nuevos['indice'] = range(0,len(precios_nuevos)) # I create an id column
        precios_nuevos.indice = precios_nuevos.indice.astype(str) # I convert the column to string
        precios_nuevos['indice_id'] = precios_nuevos.indice + precios_nuevos.fecha # I create the "indice_id" to prevent problemns when creating the primary keys in the database
        precios_nuevos.indice_id = precios_nuevos.indice_id.astype(np.int64) # I convert the "indice_id" column to integer
        precios_nuevos.set_index('indice_id', inplace=True) # I set the "indice_id" as index
        precios_nuevos.drop(columns=('indice'), inplace=True) # I drop the "indice" column since I don´t need it
        precios_nuevos.precio = precios_nuevos.precio.astype(str) # I convert the price column to string
        precios_nuevos.sucursal_id = precios_nuevos.sucursal_id.astype(str) # I convert the "sucursal_id" column to string
        precios_nuevos.producto_id = precios_nuevos.producto_id.astype(str) # I convert the "producto_id" column to string
        precios_nuevos.sucursal_id = precios_nuevos.sucursal_id.str.replace('-', '') # I erase the middle dash from the "sucursal_id" column
        precios_nuevos.sucursal_id = precios_nuevos.sucursal_id.str.replace('00:00:00', '') # If the "sucursal_id" column has timestamp values I erase them
        precios_nuevos.producto_id = precios_nuevos.producto_id.str.replace('-', '') # I erase the middle dash from the "producto_id" column
        precio_sin_sucursalid = precios_nuevos[precios_nuevos.producto_id == 'nan'].index # I create an index of the "sucursal_id" column missing values
        precios_nuevos.drop(precio_sin_sucursalid, inplace=True) # I use the index to drop some entries
        precio_sin_productoid = precios_nuevos[precios_nuevos.producto_id == 'nan'].index # I create an index of the "producto_id" column missing values
        precios_nuevos.drop(precio_sin_productoid, inplace=True) # I use the index to drop some entries
        precios_sin_precio = precios_nuevos[precios_nuevos.precio == 'nan'].index # I create an index of the price column missing values
        precios_nuevos.drop(precios_sin_precio, inplace=True) # I use the index to drop some entries
        precios_nuevos.sucursal_id = precios_nuevos.sucursal_id.astype(float) # I convert the "sucursal_id" to float
        precios_nuevos.producto_id = precios_nuevos.producto_id.astype(float) # I convert the "producto_id" to float
        precios_nuevos.sucursal_id = precios_nuevos.sucursal_id.astype(np.int64) # I convert the "sucursal_id" to integer
        precios_nuevos.producto_id = precios_nuevos.producto_id.astype(np.int64) # I convert the "producto_id" to integer
        precios_nuevos.precio = precios_nuevos.precio.astype(float) # I convert the price column to float
        precios_nuevos.drop_duplicates(inplace=True) # I drop the duplicated entries
        return precios_nuevos


# I extract the data from the datasets as dataframes to work them

Producto = pd.read_parquet(r'C:\Users\Auli\Documents\Henry\Daft-04\Proyecto Individual 1\PI01_DATA_ENGINEERING\Datasets\Datasets relevamiento precios\Producto.parquet', engine= 'pyarrow') # levanto el archivo producto con parquet


Sucursal = pd.read_csv(r'C:\Users\Auli\Documents\Henry\Daft-04\Proyecto Individual 1\PI01_DATA_ENGINEERING\Datasets\Datasets relevamiento precios\sucursal.csv', encoding='utf-8') # levanto el archivo sucursal


precios_semana_20200413 = pd.read_csv(r'C:\Users\Auli\Documents\Henry\Daft-04\Proyecto Individual 1\PI01_DATA_ENGINEERING\Datasets\Datasets relevamiento precios\precios_semana_20200413.csv', encoding='utf-16 LE') # levanto el archivo de precios
precios_semana_20200413 = nomalizacion_precios('C:/Users/Auli/Documents/Henry/Daft-04/Proyecto Individual 1/PI01_DATA_ENGINEERING/Datasets csv/precios_semana_20200413.csv', 'utf-8', ',') # levanto el archivo csv para normalizarlo


precios_semana_20200419 = pd.read_excel(r'C:\Users\Auli\Documents\Henry\Daft-04\Proyecto Individual 1\PI01_DATA_ENGINEERING\Datasets\Datasets relevamiento precios\precios_semanas_20200419_20200426.xlsx', sheet_name='precios_20200419_20200419') # Semana 20200419 (segunda hoja de calculo del excel)
precios_semana_20200419 = nomalizacion_precios('C:/Users/Auli/Documents/Henry/Daft-04/Proyecto Individual 1/PI01_DATA_ENGINEERING/Datasets csv/precios_semana_20200419.csv', 'utf-8', ',') # levanto el archivo csv para normalizarlo


precios_semana_20200426 = pd.read_excel(r'C:\Users\Auli\Documents\Henry\Daft-04\Proyecto Individual 1\PI01_DATA_ENGINEERING\Datasets\Datasets relevamiento precios\precios_semanas_20200419_20200426.xlsx', sheet_name='precios_20200426_20200426') # Semana 20200426 (primero hoja de calculo del excel)
precios_semana_20200426 = nomalizacion_precios('C:/Users/Auli/Documents/Henry/Daft-04/Proyecto Individual 1/PI01_DATA_ENGINEERING/Datasets csv/precios_semana_20200426.csv', 'utf-8', ',') # levanto el archivo csv para normalizarlo


precios_semana_20200503 = pd.read_json(r'C:\Users\Auli\Documents\Henry\Daft-04\Proyecto Individual 1\PI01_DATA_ENGINEERING\Datasets\Datasets relevamiento precios\precios_semana_20200503.json', encoding='utf-8') # levanto el archivo precios
precios_semana_20200503 = nomalizacion_precios('C:/Users/Auli/Documents/Henry/Daft-04/Proyecto Individual 1/PI01_DATA_ENGINEERING/Datasets csv/precios_semana_20200503.csv', 'utf-8', ',') # levanto el archivo csv para normalizarlo

# The file "20200518" I'll only transform it to dataframe to do the incremental load later

precios_semana_20200518 = pd.read_csv(r'C:\Users\Auli\Documents\Henry\Daft-04\Proyecto Individual 1\PI01_DATA_ENGINEERING\Datasets\Datasets relevamiento precios\precios_semana_20200518.txt',sep='|', encoding='utf-8')



# I work the product dataframe

Producto['id'] = Producto.id.str.replace('-','') # I erase the middle dash
Producto.id = Producto.id.astype(np.int64) # I convert the ids to integer
Producto.categoria1.fillna('Sin categoria',inplace=True) # I replace the none values for "No category"
Producto.categoria2.fillna('Sin categoria',inplace=True) # I replace the none values for "No category"
Producto.categoria3.fillna('Sin categoria',inplace=True) # I replace the none values for "No category"
Producto.rename(columns={'id':'producto_id'}, inplace=True) # I rename the id column
Producto.rename(columns={'nombre':'producto'}, inplace=True) # I rename the name column
Producto.producto = Producto.producto.str.replace('1 Un','') # I erase the "1 Un" values 
Producto.producto = Producto.producto.str.replace('1 Kg','') # I erase the "1 Kg" values
Producto.marca = Producto.marca.str.title() # I capitalize the column
Producto.set_index('producto_id', inplace=True) # I set the product_id as index

# I work the "sucursal" dataframe

Sucursal.localidad = Sucursal.localidad.str.title()
Sucursal.banderaDescripcion = Sucursal.banderaDescripcion.str.title()
Sucursal.sucursalNombre = Sucursal.sucursalNombre.str.title() # I capitalize this column
                                                              # just in case
Sucursal.rename(columns={'id':'sucursal_id', 
                 'comercioId':'comercio_id',
                 'banderaId':'bandera_id',
                 'banderaDescripcion':'bandera_desc',
                 'comercioRazonSocial':'comercio_razon_social',
                 'sucursalNombre':'sucursal_nombre',
                 'sucursalTipo':'sucursal_tipo'}, inplace=True)
                 # I rename some columns

Sucursal.sucursal_id = Sucursal.sucursal_id.str.replace('-', '') # I erase the middle dash from "sucursal_id"
Sucursal.sucursal_id = Sucursal.sucursal_id.astype(int) # I convert the "sucursal_id" to integer
Sucursal.set_index('sucursal_id', inplace=True)  # I set the "sucursal_id" to index
ids_duplicadas = Sucursal[Sucursal.index.duplicated()].index # I check for duplicated ids
Sucursal.drop(ids_duplicadas, inplace=True) # I drop the duplicated ids


# I export the dataframes as .csv files to the same folder

Producto.to_csv(r'C:\Users\Auli\Documents\Henry\Daft-04\Proyecto Individual 1\PI01_DATA_ENGINEERING\Datasets csv\producto.csv') # paso el df a csv
Sucursal.to_csv(r'C:\Users\Auli\Documents\Henry\Daft-04\Proyecto Individual 1\PI01_DATA_ENGINEERING\Datasets csv\sucursal.csv') # paso el df a csv
precios_semana_20200413.to_csv(r'C:\Users\Auli\Documents\Henry\Daft-04\Proyecto Individual 1\PI01_DATA_ENGINEERING\Datasets csv\precios_semana_20200413.csv') # paso el df a csv
precios_semana_20200419.to_csv(r'C:\Users\Auli\Documents\Henry\Daft-04\Proyecto Individual 1\PI01_DATA_ENGINEERING\Datasets csv\precios_semana_20200419.csv') # paso el df a csv
precios_semana_20200426.to_csv(r'C:\Users\Auli\Documents\Henry\Daft-04\Proyecto Individual 1\PI01_DATA_ENGINEERING\Datasets csv\precios_semana_20200426.csv') # paso el df a csv
precios_semana_20200503.to_csv(r'C:\Users\Auli\Documents\Henry\Daft-04\Proyecto Individual 1\PI01_DATA_ENGINEERING\Datasets csv\precios_semana_20200503.csv') # paso el df a csv
precios_semana_20200518.to_csv(r'C:\Users\Auli\Documents\Henry\Daft-04\Proyecto Individual 1\PI01_DATA_ENGINEERING\Datasets csv\precios_semana_20200518.csv') # paso el df a csv



class producto (base): # I create the products class refering to the product table
    
    __tablename__ = 'producto'

    producto_id = Column(BigInteger, primary_key=True)
    marca = Column(String)
    producto = Column(String)
    presentacion = Column(String)
    categoria1 = Column(String)
    categoria2 = Column(String)
    categoria3 = Column(String)
    precios = relationship('precio', backref='precio')
    
    def __init__(self, producto_id, marca, producto, presentacion, categoria1, categoria2, categoria3):
        self.producto_id = producto_id
        self.marca = marca
        self.prodcuto = producto
        self.presentacion = presentacion
        self.categoria1 = categoria1
        self.categoria2 = categoria2
        self.categoria3 = categoria3



class sucursal (base): # I create the "sucursal" class refering to the "sucursal" table
    
    __tablename__ = 'sucursal'

    sucursal_id = Column(BigInteger, primary_key=True)
    comercio_id = Column(Integer)
    bandera_id = Column(Integer)
    bandera_desc = Column(String)
    comercio_razon_social = Column(String)
    provincia = Column(String)
    localidad = Column(String)
    direccion = Column(String)
    lat = Column(Float)
    lng = Column(Float)
    sucursal_nombre = Column(String)
    sucursal_tipo = Column(String)
    precios = relationship('precio', backref='precio')
    
    def __init__(self, sucursal_id, comercio_id, bandera_id, bandera_desc, comercio_razon_social, provincia, localidad, direccion, lat, lng, sucursal_nombre, sucursal_tipo):
        self.sucursal_id = sucursal_id
        self.comercio_id = comercio_id
        self.bandera_id = bandera_id
        self.bandera_desc = bandera_desc
        self.comercio_razon_social = comercio_razon_social
        self.provincia = provincia
        self.localidad = localidad
        self.direccion = direccion
        self.lat = lat
        self.lng = lng
        self.sucursal_nombre = sucursal_nombre
        self.sucursal_tipo = sucursal_tipo



class precios (base): # I create the price class refering to the price table
    
    __tablename__ = 'precio'

    precio = Column(Float)
    sucursal_id = Column(BigInteger, ForeignKey('sucursal.sucursal_id'))
    producto_id = Column(BigInteger, ForeignKey('producto.producto_id'))
    fecha = Column(Integer)
    indice_id = Column(BigInteger, primary_key=True)
    
    
    def __init__(self, precio, sucursal_id, producto_id, fecha, indice_id):
        self.precio = precio
        self.sucursal_id = sucursal_id
        self.producto_id = producto_id
        self.fecha = fecha
        self.indice_id = indice_id



# I load the dataframes to the database


Producto.to_sql(con=engine, name='producto', if_exists='replace') # I load to the class(table) the product dataframe

Sucursal.to_sql(con=engine, name='sucursal', if_exists='replace') # I load to the class(table) the "sucursal" dataframe

precios_semana_20200413.to_sql(con=engine, name='precio', if_exists='append') # I load to the class(table) the price dataframe

precios_semana_20200419.to_sql(con=engine, name='precio', if_exists='append') # I load to the class(table) the price dataframe

precios_semana_20200426.to_sql(con=engine, name='precio', if_exists='append') # I load to the class(table) the price dataframe

precios_semana_20200503.to_sql(con=engine, name='precio', if_exists='append') # I load to the class(table) the price dataframe


engine.execute('USE ' + Database) # I use the database

engine.execute('ALTER TABLE producto ADD PRIMARY KEY(producto_id)') # I set "producto_id" as primary key for the products table

engine.execute('ALTER TABLE sucursal ADD PRIMARY KEY(sucursal_id)') # I set "sucursal_id" as primary key for the "sucursal" table

engine.execute('ALTER TABLE precio ADD PRIMARY KEY(indice_id)') # I set "indice_id" as primary key for the price table


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

base.metadata.create_all(engine) # I save the .db file