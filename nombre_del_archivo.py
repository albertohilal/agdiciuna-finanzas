from sqlalchemy import create_engine

def conectar_mysql():
    usuario = "agdiciun_b3toh"  # Usuario MySQL en el servidor remoto
    contraseña = "elgeneral2018"  # Reemplaza con la contraseña real
    host = "sv71.ifastnet.com"  # Dirección del servidor remoto
    base_de_datos = "agdiciun_financiera"  # Base de datos en el servidor

    try:
        engine = create_engine(f"mysql+mysqlconnector://{usuario}:{contraseña}@{host}/{base_de_datos}")
        connection = engine.connect()
        print("✅ Conexión exitosa a la base de datos remota")
        connection.close()
    except Exception as e:
        print(f"❌ Error al conectar: {e}")

# Ejecutar la prueba de conexión
conectar_mysql()
