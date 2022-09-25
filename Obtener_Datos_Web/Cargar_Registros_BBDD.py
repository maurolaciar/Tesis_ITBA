#%%
from matplotlib.font_manager import json_load
import pandas as pd
import json
import os

import psycopg2 as ps
from datetime import date, datetime, timedelta
# %%
#ps.connect(host=hostname,dbname=database,user=username,password=pwd,port=portid)
def connect_to_db(host_name,db_name,port,username,password):
    try:
        conn = ps.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = portid
        )
    except ps.OperationalError as e:
        raise e
    else:
        print("Connected!")
    return conn
# %%
#Parametros para conectarnos a la base de datos
hostname = "localhost"
database = 'precios_clarosdb'
username = 'postgres'
pwd = 'postgres'
portid = 5432
#conn = None
#cur = None

conn = connect_to_db(hostname,database,portid,username,pwd)
# %%
#Create table

def create_table(curr):
    create_table_command = ('''CREATE TABLE IF NOT EXISTS sucursales_y_productos (
        Key_ text,
        banderaId text,
        lat text,
        lng text,
        sucursalNombre text,
        id_ text,
        sucursalTipo text,
        provincia text,
        preciosProducto_promo1_descripcion text,
        preciosProducto_promo1_precio text,
        preciosProducto_precioLista text,
        preciosProducto_promo2_descripcion text,
        preciosProducto_promo2_precio text,
        actualizadoHoy text,
        direccion text,
        banderaDescripcion text,
        localidad text,
        comercioRazonSocial text,
        comercioId text,
        marca text,
        nombre text,
        presentacion text,
        fecha text
        )
        ''')

    curr.execute(create_table_command)
# %%
def insert_rows(curr,key_, banderaId,lat,lng,sucursalNombre,id_,sucursalTipo,provincia,preciosProducto_promo1_descripcion,preciosProducto_promo1_precio,preciosProducto_precioLista,preciosProducto_promo2_descripcion,preciosProducto_promo2_precio,actualizadoHoy,direccion,banderaDescripcion,localidad,comercioRazonSocial,comercioId,marca,nombre,presentacion,fecha):
    insert_into_test2 = ("""INSERT INTO sucursales_y_productos 
    (key_, banderaId,lat,lng,sucursalNombre,id_,sucursalTipo,provincia,preciosProducto_promo1_descripcion,preciosProducto_promo1_precio,preciosProducto_precioLista,preciosProducto_promo2_descripcion,preciosProducto_promo2_precio,actualizadoHoy,direccion,banderaDescripcion,localidad,comercioRazonSocial,comercioId,marca,nombre,presentacion,fecha) 
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""")
    
    row_to_insert = (key_,
                     banderaId,
                     lat,
                     lng,
                     sucursalNombre,
                     id_,
                     sucursalTipo,
                     provincia,
                     preciosProducto_promo1_descripcion,
                     preciosProducto_promo1_precio,
                     preciosProducto_precioLista,
                     preciosProducto_promo2_descripcion,
                     preciosProducto_promo2_precio,
                     actualizadoHoy,
                     direccion,
                     banderaDescripcion,
                     localidad,
                     comercioRazonSocial,
                     comercioId,
                     marca,
                     nombre,
                     presentacion,
                    fecha)
    
    curr.execute(insert_into_test2,row_to_insert)
# %%
curr = conn.cursor()
# %%
create_table(curr)
# %%
conn.commit()
# %%
def insertar_registros(archivo):
    with open(archivo) as f:
        data = json.load(f)
    lista_data = []
    for i in data.items(): #lo convierte a tuplas
        try: 
            lista_data.append([i[0],i[1]]) #hacemos que este todo en una linea
        except: pass
    try:
        Tabla_Data = pd.DataFrame(lista_data, columns=['Key','datos'])
        Solo_sucursales = Tabla_Data.loc[Tabla_Data["Key"] == "sucursales"]
        Solo_sucursales = Solo_sucursales.explode('datos')
        Solo_sucursales =pd.json_normalize(json.loads(Solo_sucursales.to_json(orient="records")))
        #Solo_sucursales.to_csv("Test_sucursales.csv")
        Solo_productos = Tabla_Data.loc[Tabla_Data["Key"] == "producto"]
        Solo_productos =pd.json_normalize(json.loads(Solo_productos.to_json(orient="records")))
        Solo_productos= Solo_productos[["datos.marca","datos.id","datos.nombre","datos.presentacion"]]
        #Solo_productos.to_csv("Test_productos.csv")
        Sucursales_y_Productos = Solo_sucursales
        Sucursales_y_Productos["datos.marca"] = Solo_productos["datos.marca"].iloc[0]
        Sucursales_y_Productos["datos.id"] = Solo_productos["datos.id"].iloc[0]
        Sucursales_y_Productos["datos.nombre"] = Solo_productos["datos.nombre"].iloc[0]
        Sucursales_y_Productos["datos.presentacion"] = Solo_productos["datos.presentacion"].iloc[0]
        Sucursales_y_Productos["Fecha"] = str(date.today())
    except: pass
  
    
    try:
        Resumen_Sucursales_y_Productos = Sucursales_y_Productos[["Key","datos.banderaId","datos.lat","datos.lng","datos.sucursalNombre","datos.id","datos.sucursalTipo","datos.provincia","datos.preciosProducto.promo1.descripcion","datos.preciosProducto.promo1.precio","datos.preciosProducto.precioLista","datos.preciosProducto.promo2.descripcion","datos.preciosProducto.promo2.precio","datos.actualizadoHoy","datos.direccion","datos.banderaDescripcion","datos.localidad","datos.comercioRazonSocial","datos.comercioId","datos.marca","datos.nombre","datos.presentacion","Fecha"]]
    #Sucursales_y_Productos.to_csv("Test_sucyProductos.csv")
        for i, row in Resumen_Sucursales_y_Productos.iterrows():
            try:
                insert_rows(curr,row['Key'],row['datos.banderaId'],row['datos.lat'],row['datos.lng'],row['datos.sucursalNombre'],row['datos.id'],row['datos.sucursalTipo'],row['datos.provincia'],row['datos.preciosProducto.promo1.descripcion'],row['datos.preciosProducto.promo1.precio'],row['datos.preciosProducto.precioLista'],row['datos.preciosProducto.promo2.descripcion'],row['datos.preciosProducto.promo2.precio'],row['datos.actualizadoHoy'],row['datos.direccion'],row['datos.banderaDescripcion'],row['datos.localidad'],row['datos.comercioRazonSocial'],row['datos.comercioId'],row['datos.marca'],row['datos.nombre'],row['datos.presentacion'],row['Fecha'])
            except: pass
    except: pass
    conn.commit()
# %%
hoy = str(date.today())
#hoy = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
carpeta = r"C:/Test"+"/"+hoy
print(carpeta)
# %%

#carpeta = r"C:\Test"
for file in os.listdir(carpeta):
    archivo = os.path.join(carpeta,file)
    insertar_registros(archivo)
# %%
conn.commit()
# %%
ayer = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
print(ayer)


# %%
