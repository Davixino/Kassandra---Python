# Actividad 2
# Estudiante: Ronald Jimenez Paute
# Importación de librerias necesarias para conexión con Cassandra y gestión de fechas
from cassandra.cluster import Cluster
from datetime import date


###################################CLASES POR ENTIDAD O RELACION########################################################
########################################################################################################################
# Parte 1: Definición de clases de las entidades y relaciones


class Provincia:

    def __init__(self,Provincia_Nombres,Provincia_ProCod,):
        self.Provincia_Nombres = Provincia_Nombres
        self.Provincia_ProCod = Provincia_ProCod
    def __init__(self,Provincia_Nombres,Provincia_ProCod,Provincia_Jefes):  # Constructor de coleccion
        self.Provincia_Nombres = Provincia_Nombres
        self.Provincia_ProCod = Provincia_ProCod
        self.Provincia_Jefes = Provincia_Jefes

class Zona:
    def __init__(self, Zona_ZonCod, Zona_Nombre):  # Constructor sin relacion 1:n sin coleccion
        self.Zona_ZonCod = Zona_ZonCod
        self.Zona_Nombre = Zona_Nombre


    def __init__(self, Zona_ZonCod, Zona_Nombre, Zona_Municipios):  # Constructor con coleccion
        self.Zona_ZonCod = Zona_ZonCod
        self.Zona_Nombre = Zona_Nombre
        self.Zona_Municipios = Zona_Municipios

    def __init__(self, Zona_ZonCod, Zona_Nombre, Zona_Municipios,Provincia_ProCod):  # Constructor con relacion 1:n entre provincia y zona
        self.Zona_ZonCod = Zona_ZonCod
        self.Zona_Nombre = Zona_Nombre
        self.Zona_Municipios = Zona_Municipios
        self.Provincia_ProCod = Provincia_ProCod

class Zona_Subestacion:
    def __init__(self, Zona_ZonCod, Subestacion_CodSub, Zona_Subestacion_Cantidad, Zona_Subestacion_Fecha):
        # relacion n:m entre Zona y Subestaciom
        self.Zona_ZonCod = Zona_ZonCod
        self.Subestacion_CodSub = Subestacion_CodSub
        self.Zona_Subestacion_Cantidad = Zona_Subestacion_Cantidad
        self.Zona_Subestacion_Fecha = Zona_Subestacion_Fecha


class Subestacion:
    def __init__(self, Subestacion_CodSub, Subestacion_Capacidad):  # Constructor sin relacion
        self.Subestacion_CodSub = Subestacion_CodSub
        self.Subestacion_Capacidad = Subestacion_Capacidad

    def __init__(self, Subestacion_CodSub, Subestacion_Capacidad, Linea_CodLin):  # Constructor relacion
        ## 1:n entre Subestacion y Linea
        self.Subestacion_CodSub = Subestacion_CodSub
        self.Subestacion_Capacidad = Subestacion_Capacidad
        self.Linea_CodLin = Linea_CodLin


class Linea:
    def __init__(self, Linea_CodLin, Linea_Longitud):  # Constructor sin relacion
        self.Linea_CodLin = Linea_CodLin
        self.Linea_Longitud = Linea_Longitud

    def __init__(self, Linea_CodLin, Linea_Longitud, Distribucion_CodDis):  # Constructor con relacion
        ## entre Linea y distribuccion 1:n
        self.Linea_CodLin = Linea_CodLin
        self.Linea_Longitud = Linea_Longitud
        self.Distribucion_CodDis = Distribucion_CodDis


class DistribuccionDeRed:
    def __init__(self, Distribuccion_CodDis, Distribuccion_LongitudMaxima):  # Constructor sin relacion
        self.Distribuccion_CodDis = Distribuccion_CodDis
        self.Distribuccion_LongitudMaxima = Distribuccion_LongitudMaxima

    def __init__(self, Distribuccion_CodDis, Distribuccion_LongitudMaxima, Estacion_CodEst):
        # Constructor con relacion entre distribuccion y Estacion 1:n
        self.Distribuccion_CodDis = Distribuccion_CodDis
        self.Distribuccion_LongitudMaxima = Distribuccion_LongitudMaxima
        self.Estacion_CodEst = Estacion_CodEst


class Estacion:
    def __init__(self, Estacion_CodEst, Estacion_Nombre):  # Constructor sin relacion
        self.Estacion_CodEst = Estacion_CodEst
        self.Estacion_Nombre = Estacion_Nombre


class EstacionProductor:
    def __init__(self, Estacion_CodEst, Productor_CodPro, Estacion_Productor_Fecha, Estacion_Productor_Cantidad):
        self.Estacion_CodEst = Estacion_CodEst
        self.Productor_CodPro = Productor_CodPro
        self.Estacion_Productor_Fecha = Estacion_Productor_Fecha
        self.Estacion_Productor_Cantidad = Estacion_Productor_Cantidad


class Productor:
    def __init__(self, Productor_CodPro, Productor_MediaProduccion, Productor_MaximoProduccion, Productor_Nombre, Productor_Pais, Productor_OrigenEnergia):
        self.Productor_CodPro = Productor_CodPro
        self.Productor_MediaProduccion = Productor_MediaProduccion
        self.Productor_MaximoProduccion = Productor_MaximoProduccion
        self.Productor_Nombre = Productor_Nombre
        self.Productor_Pais = Productor_Pais
        self.Productor_OrigenEnergia = Productor_OrigenEnergia


#############################################INSERCION DE DATOS#########################################################
########################################################################################################################
# Se piden funciones para insertar instancias de las siguientes entidades. Entre paréntesis se indica
# las consultas de las que se crearon las tablas a insertar los datos:
# Provincia en una tabla Consulta 1
# Obtener los jefes provinciales que están asociados a una provincia buscando a través del nombre de la provincia
# Funcion para pedir datos de una provincia e insertarlos en la Base de Datos
def insertProvincia():  #CORREGIDA
    # pedimos al usuario del programa los datos  de la provincia
    provincia_Nombres = input("Dame el nombre de la provincia")
    provincia_ProCod = input("Dame el codigo de la provincia")
    provincia_Jefes = set()  # inciamos la coleccion
    provincia_JefeProvincial = input("Introduzca un jefe provincial, vacio para parar")

    while (provincia_JefeProvincial != ""):
        provincia_Jefes.add(provincia_JefeProvincial)
        provincia_JefeProvincial = input("Introduzca un jefe provincial. vacio para parar")

    p = Provincia (provincia_Nombres,provincia_ProCod, provincia_Jefes)
    insertStatementprovincia = session.prepare('INSERT INTO ronaldjimenez."Tabla01" ("Provincia_Nombres", "Provincia_ProCod", "Provincia_Jefes") VALUES (?,?,?)')
    session.execute(insertStatementprovincia,[p.Provincia_Nombres, p.Provincia_ProCod, p.Provincia_Jefes])
    insertStatementJefes = session.prepare('INSERT INTO ronaldjimenez."Tabla03" ("Provincia_JefeProvincial", "Provincia_Nombres", "Provincia_ProCod", "Provincia_Jefes") VALUES (?,?,?,?)')

    for jefes in provincia_Jefes:
        session.execute(insertStatementJefes,[jefes, p.Provincia_Nombres, p.Provincia_ProCod, p.Provincia_Jefes])


# Productores en una tabla Consulta 8
# Buscar productores según el origen de la energía que producen (eólica, nuclear, carbón, solar o gas). Nota: En la
# actualidad el 50% de productores usan carbón, un 30% nuclear, un 15% solar y un 5% eólica.
def insertProductores():  #CORREGIDA
    # pedimos al usuario del programa los datos del productor
    productor_CodPro = input("Dame el codigo del productor")
    productor_MediaProduccion = float(input("Dame media produccion"))
    productor_MaximoProduccion = float(input("Dame maxima Produccion"))
    productor_Nombre = input("Dame nombre del productor")
    productor_Pais = input("Dame el pais")
    productor_OrigenEnergia = input("Dame Origen de Energia")
    p2 = Productor(productor_CodPro, productor_MediaProduccion, productor_MaximoProduccion, productor_Nombre,
                   productor_Pais, productor_OrigenEnergia)
    insertStatementProductor = session.prepare('INSERT INTO ronaldjimenez."Tabla08" ("Productor_CodPro", "Productor_MediaProduccion", "Productor_MaximoProduccion", "Productor_Nombre", "Productor_Pais", "Productor_OrigenEnergia") VALUES (?,?,?,?,?,?)')
    session.execute(insertStatementProductor,[p2.Productor_CodPro, p2.Productor_MediaProduccion, p2.Productor_MaximoProduccion,p2.Productor_Nombre, p2.Productor_Pais, p2.Productor_OrigenEnergia])


# Se piden funciones para insertar las siguientes relaciones entre instancias:
# Relacion Provee

def insertEstacionProductor(): #CORREGIDA
    estacion_CodEst = input("Dame el codigo de estacion")
    estacion_Nombre = input("Dame el nombre de la estacion")
    productor_CodPro=input("dame el codigo del productor") #clave primaria tabla 08
    hoy = date.today()
    estacion_Productor_Cantidad = float(input("Dame la cantidad"))
    prod = extraerDatosProductores (productor_CodPro)
    if (prod != None):
        insertStatement = session.prepare('INSERT INTO ronaldjimenez."Tabla04" ("Estacion_Productor_Fecha", "Estacion_Productor_Cantidad", "Estacion_CodEst", "Productor_CodProd","Estacion_Nombre", "Productor_OrigenEnergia") VALUES (?,?,?,?,?,?)')
        session.execute(insertStatement,[hoy, estacion_Productor_Cantidad, estacion_CodEst, prod.Productor_CodPro, estacion_Nombre, prod.Productor_OrigenEnergia])


# Relacion conjunta Consiste-Suple
def insertDistribucionLineaSubestacion():  #CORREGIDA
    distribuccion_CodDis = input("Dame el codigo de distribucion")
    linea_CodLin = input("Dame la linea")
    subestacion_CodSub = input("Dame el codigo de la Subestacion")
    distribuccion_LongitudMaxima = float(input("Dame la longitud maxima "))
    linea_Longitud = float(input("Dame la longitud de la linea"))
    subestacion_Capacidad = float(input("Dame la capacidad de subestacion"))
    cod= input("Dame el codigo del productor")
    infoprod= extraerDatosProductores(cod)
    if (infoprod != None):
        insertStatementDistribucionLineaSubestacion = session.prepare('INSERT INTO ronaldjimenez."Tabla05" ("Distribucion_CodDis","Subestacion_CodSub","Linea_Longitud","Distribucion_LongitudMaxima", "Linea_CodLin","Subestacion_Capacidad") VALUES (?,?,?,?,?,?)')
        insertStatementLineaSubestacion = session.prepare('INSERT INTO ronaldjimenez."Tabla02" ("Subestacion_Capacidad","Subestacion_CodSub","Linea_CodLin","Linea_Longitud") VALUES (?,?,?,?)')
        insertStatementLineaProdcutor= session.prepare('INSERT INTO ronaldjimenez."Tabla07" ("Linea_Longitud", "Productor_CodPro","Distribucion_CodDis", "Linea_CodLin", "Productor_Nombre") VALUES (?,?,?,?,?)')
        session.execute(insertStatementDistribucionLineaSubestacion,[distribuccion_CodDis,subestacion_CodSub,linea_Longitud,distribuccion_LongitudMaxima, linea_CodLin,subestacion_Capacidad])
        session.execute(insertStatementLineaSubestacion,[subestacion_Capacidad, subestacion_CodSub, linea_CodLin, linea_Longitud])
        session.execute(insertStatementLineaProdcutor,[linea_Longitud, infoprod.Productor_CodPro,distribuccion_CodDis, linea_CodLin, infoprod.Productor_Nombre])


#############################################TABLAS SOPORTE ###########################################################
#######################################################################################################################
# Creacion de metodos destinados a consultar la informacion de tablas soporte

# ENTIDAD PROVINCIA #CORREGIDA
def extraerDatosProvincia(Provincia_Nombres):

    select = session.prepare('SELECT * FROM ronaldjimenez."Tabla01" WHERE "Provincia_Nombres" = ? ALLOW FILTERING;')
    filas = session.execute(select,[Provincia_Nombres,])
    # = []
    for fila in filas:
        pt = Provincia(Provincia_Nombres, fila.Provincia_ProCod, fila.Provincia_Jefes)
       # provincias.append(pt)
        return pt


# ENTIDAD PRODUCTORES
def extraerDatosProductores(Productor_CodPro):
   # "Productor_Pais", "Productor_Nombre", "Productor_OrigenEnergia"


    select = session.prepare('SELECT * FROM ronaldjimenez."Tabla08" WHERE "Productor_CodPro" = ? ALLOW FILTERING;')
    filas = session.execute(select,[Productor_CodPro,])
   # productores = []
    for fila in filas:
        pr = Productor(Productor_CodPro, fila.Productor_MediaProduccion,fila.Productor_MaximoProduccion,fila.Productor_Nombre, fila.Productor_Pais,fila.Productor_OrigenEnergia)
        #productores.append(pr)
        return pr


#############################################ACTUALIZACION DE DATOS#####################################################
#######################################################################################################################
# Actualizar el nombre de una provincia en la tabla que satisfaga la consulta 1 de la actividad 1 (grupo 1).

def actualizarprovincia(): #CORREGIDA
    nombre = input("Dame el nombre de la provincia")
    cod = input("Dame el codigo de la provincia")
    nombre2= input("Dame el nombre de la provincia a actualizar")
    infoprovincia = extraerDatosProvincia(nombre)
    if (infoprovincia != None):
       # select = session.prepare('SELECT "Provincia_Nombres" FROM ronaldjimenez."Tabla01" WHERE "Provincia_ProCod" = ? ALLOW FILTERING;')
       # filas = session.execute(select, [ProCod,])
        seleccionarprovincia= session.prepare ('SELECT "Provincia_ProCod" FROM ronaldjimenez."Tabla01" WHERE "Provincia_Nombres" = ? ALLOW FILTERING;')
        session.execute(seleccionarprovincia,[nombre,])
        borrarprovincia = session.prepare('DELETE FROM ronaldjimenez."Tabla01" WHERE "Provincia_ProCod" = ? AND "Provincia_Nombres" = ? ')
        session.execute(borrarprovincia, [cod, infoprovincia.Provincia_Nombres])
        insertStatementInsertar = session.prepare('INSERT INTO ronaldjimenez."Tabla01" ("Provincia_Nombres", "Provincia_ProCod", "Provincia_Jefes") VALUES (?,?,?)')
        session.execute(insertStatementInsertar, [nombre2, infoprovincia.Provincia_ProCod, infoprovincia.Provincia_Jefes])
     #   insertUpdate = session.prepare ('UPDATE ronaldjimenez."Tabla01" SET "Provincia_Nombres" = ? WHERE "Provincia_ProCod" = ?')
      #  session.execute(insertUpdate,[nombre,cod])



# Actualizar el origen de la energía que produce un productor en la tabla que satisfaga la consulta 8 de la actividad 1 (grupo 1).
def actualizarOrigen():
    origen = input("Dame el origen de energia a actualizar")
    cod = input("Dame el codigo del productor")
    infoproductor = extraerDatosProductores(cod)
    if (infoproductor != None):
        # select = session.prepare('SELECT "Provincia_Nombres" FROM ronaldjimenez."Tabla01" WHERE "Provincia_ProCod" = ? ALLOW FILTERING;')
        # filas = session.execute(select, [ProCod,])
        seleccionarproductor = session.prepare('SELECT "Productor_OrigenEnergia" FROM ronaldjimenez."Tabla08" WHERE "Productor_CodPro"= ? ALLOW FILTERING;')
        session.execute(seleccionarproductor, [cod,])
        borrarproductor = session.prepare('DELETE FROM ronaldjimenez."Tabla08" WHERE "Productor_OrigenEnergia" = ? AND "Productor_CodPro" = ?')
        session.execute(borrarproductor, [infoproductor.Productor_OrigenEnergia, cod])
        insertStatementInsertar = session.prepare('INSERT INTO ronaldjimenez."Tabla08" ("Productor_CodPro", "Productor_MediaProduccion", "Productor_MaximoProduccion", "Productor_Nombre", "Productor_Pais", "Productor_OrigenEnergia") VALUES (?,?,?,?,?,?)')
        session.execute(insertStatementInsertar,[infoproductor.Productor_CodPro,infoproductor.Productor_MediaProduccion, infoproductor.Productor_MaximoProduccion,infoproductor.Productor_Nombre, infoproductor.Productor_Pais, origen])


# Recuerde que en ocasiones no se puede usar la operación UPDATE cuando haya que actualizar
# el valor de una clave primaria de una tabla y que se requiere de la combinación REMOVE + INSERT.
#############################################CONSULTA DATOS#############################################################
########################################################################################################################
def consultaprovincia():
    nombres = input("Dame el nombre de la provincia a consultar")
    infopro = extraerDatosProvincia(nombres)
    if (infopro != None):
        print("Nombre de la provincia", nombres)
        print("Codigo",infopro.Provincia_ProCod)
        print("Jefes Provinnciales", infopro.Provincia_Jefes)
def consultatabla02():

    cod= input("Dame el codigo de la subestacion")

    select = session.prepare('SELECT*FROM ronaldjimenez."Tabla02" WHERE "Subestacion_CodSub"= ? ALLOW FILTERING;')
    filas= session.execute(select, [cod,])
    for i in filas:
        print("\n", i)

def consultatabla03():

    cod= input("Dame el codigo de la provincia")

    select = session.prepare('SELECT*FROM ronaldjimenez."Tabla03" WHERE "Provincia_ProCod"= ? ALLOW FILTERING;')
    filas= session.execute(select, [cod,])
    for i in filas:
        print("\n", i)

def consultatabla04():

    cod= input("Dame el codigo de la estacion")

    select = session.prepare('SELECT*FROM ronaldjimenez."Tabla04" WHERE "Estacion_CodEst"= ? ALLOW FILTERING;')
    filas= session.execute(select, [cod,])
    for i in filas:
        print("\n", i)

def consultatabla05():

    cod= input("Dame el codigo de la subestacion")

    select = session.prepare('SELECT*FROM ronaldjimenez."Tabla05" WHERE "Subestacion_CodSub"= ? ALLOW FILTERING;')
    filas= session.execute(select, [cod,])
    for i in filas:
        print("\n", i)

def consultatabla06():

    cod= input("Dame el codigo de la zona")

    select = session.prepare('SELECT*FROM ronaldjimenez."Tabla06" WHERE "Zona_ZonCod"= ? ALLOW FILTERING;')
    filas= session.execute(select, [cod,])
    for i in filas:
        print("\n", i)

def consultatabla07():
    cod = input("Dame el codigo del productor ")

    select = session.prepare('SELECT*FROM ronaldjimenez."Tabla07" WHERE "Productor_CodPro"= ? ALLOW FILTERING;')
    filas = session.execute(select, [cod, ])
    for i in filas:
        print("\n", i)

def consultaProductores():
    cod = input("Dame el codigo del productor a consultar")
    productor = extraerDatosProductores(cod)
    if (productor != None):
        print("Codigo del productor", cod)
        print("Media produccion del productor", productor.Productor_MediaProduccion)
        print("Maxima produccion del productor", productor.Productor_MaximoProduccion)
        print("Nombre del producctor", productor.Productor_Nombre)
        print("Nombre del pais", productor.Productor_Pais)
        print("OrigenEnergia", productor.Productor_OrigenEnergia)


#############################################CONEXION CASSANDRA#########################################################
########################################################################################################################

#cluster = Cluster()
cluster = Cluster(['127.0.0.1'], port=9042,ssl_context=False)
session = cluster.connect('ronaldjimenez')

#####################################INTERFAZ DE INTERACION EL USUARIO##################################################
########################################################################################################################
numero = -1
numero2= -1
numero3= -1
numero4= -1

while (numero2 != 0):
    # Pedimos numero al usuario
    print("##########################################################################")
    print("Introduzca un número para ejecutar una de las siguientes operaciones:")
    print("1. INSERCION DE DATOS")
    print("2. CONSULTA DE DATOS")
    print("3. ACTUALIZACION DE DATOS")
    print("0. SALIR")
    print("##########################################################################")
    numero2 = int(input("TECLADO-->"))

    if (numero2 == 1): #INSERCION
        print("Introduzca un número para ejecutar una de las siguientes operaciones:")
        print("1. Insertar provincia ")
        print("2. Insertar productores")
        print("3. Insertar relación Estacion y Productor (algunos datos)")
        print("4. Insertar relación Distribucion, Linea y Subestacion (algunos datos)")
        numero = int(input("-->"))
        if(numero== 1):
            insertProvincia()
        if(numero==2):
            insertProductores()
        if(numero==3):
            insertEstacionProductor()
        if(numero==4):
            insertDistribucionLineaSubestacion()
    elif (numero2 == 2): #CONSULTA
        print("Introduzca un número para ejecutar una de las siguientes operaciones:")
        print("1. Consultar datos provincia según su nombre de provincia")
        print("2. Consultar datos de la Subestacion_linea segun su cod de subestacion")
        print("3. Consultar datos de la provincia segun su cod de provincia ")
        print("4. Consultar datos de Estacion_productor segun su cod de estacion")
        print("5. Consultar datos de Distribucion_Subestacion_Linea segun su cod de subestacion")
        print("6. Consultar datos de capacidad sumanda segun su cod de zona")
        print("7. Consultar datos de Linea_Distribucion_Productor segun su cod de productor")
        print("8. Consultar datos productores segun su cod de productor")
        numero3 = int(input("-->"))
        if (numero3== 1):
            consultaprovincia()
        if (numero3== 2):
            consultatabla02()
        if (numero3==3):
            consultatabla03()
        if (numero3==4):
            consultatabla04()
        if (numero3==5):
            consultatabla05()
        if (numero3==6):
            consultatabla06()
        if (numero3==7):
            consultatabla07()
        if (numero3==8):
            consultaProductores()
    elif (numero2 == 3): #ACTUALIZACION
        print("1. Actualizar el nombre de la provincia")
        print("2. Actualizar el origen de la energia")
        numero4 = int(input("-->"))  # Pedimos numero al usuario
        if (numero4 == 1):
            actualizarprovincia()
        if (numero4 == 2):
            actualizarOrigen()

    elif (numero2==0):
        print("Gracias")
        print("BY: RONALD JIMENEZ")

    else:
        print("numero incorrecto")

cluster.shutdown()