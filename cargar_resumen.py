import os
import PyPDF2
import pandas as pd
import re
from sqlalchemy import create_engine

# 📌 1. Definir la carpeta de trabajo
directorio_actual = os.path.dirname(os.path.abspath(__file__))

# 📌 2. Función para extraer texto de un PDF
def extraer_texto_pdf(archivo_pdf):
    with open(archivo_pdf, "rb") as file:
        reader = PyPDF2.PdfReader(file)
        texto = ""
        for page in reader.pages:
            texto += page.extract_text() + "\n"
    
    print(f"📄 Texto extraído de {os.path.basename(archivo_pdf)}:\n{texto[:500]}")
    return texto

# 📌 3. Función para procesar movimientos bancarios
def procesar_movimientos(texto):
    lineas = texto.split("\n")
    print(f"🔍 Se encontraron {len(lineas)} líneas en el PDF")

    datos = []
    for linea in lineas:
        print(f"📌 Analizando línea: {linea}")  # Depuración de cada línea

        match = re.match(r"(\d{2}/\d{2}/\d{2})\s+(.+?)\s+(\d+)\s+([\d.,]*)\s+([\d.,]*)\s+([\d.,]*)", linea)
        if match:
            fecha, descripcion, comprobante, debito, credito, saldo = match.groups()

            # Función para limpiar números
            def limpiar_numero(valor):
                if valor:
                    valor = valor.replace(".", "").replace(",", ".")
                    try:
                        return float(valor)
                    except ValueError:
                        print(f"❌ Error al convertir número: '{valor}'")
                        return 0.0
                return 0.0

            datos.append({
                "fecha": pd.to_datetime(fecha, format="%d/%m/%y"),
                "descripcion": descripcion.strip(),
                "comprobante": comprobante.strip(),
                "debito": limpiar_numero(debito),
                "credito": limpiar_numero(credito),
                "saldo": limpiar_numero(saldo)
            })

    print(f"✅ {len(datos)} movimientos procesados.")
    return pd.DataFrame(datos)

# 📌 4. Conectar a MySQL
def conectar_mysql():
    usuario = "agdiciun_b3toh"  
    contraseña = "elgeneral2018"  
    host = "sv71.ifastnet.com"  
    base_de_datos = "agdiciun_financiera"

    try:
        engine = create_engine(f"mysql+mysqlconnector://{usuario}:{contraseña}@{host}/{base_de_datos}")
        return engine
    except Exception as e:
        print(f"❌ Error de conexión a MySQL: {e}")
        exit(1)

# 📌 5. Guardar datos en MySQL
def guardar_en_mysql(df):
    if df.empty:
        print("⚠️ No se encontraron datos para cargar en la base de datos.")
        return

    engine = conectar_mysql()
    df.to_sql("movimientos_bancarios", con=engine, if_exists="append", index=False)
    print("✅ Datos cargados en la base de datos correctamente.")

# 📌 6. Procesar todos los PDFs de la carpeta
def procesar_todos_los_pdfs():
    archivos_pdf = [f for f in os.listdir(directorio_actual) if f.endswith("Extracto.pdf")]

    if not archivos_pdf:
        print("⚠️ No se encontraron archivos PDF para procesar.")
        return

    print(f"🔍 Encontrados {len(archivos_pdf)} archivos PDF para procesar.\n")

    for archivo in archivos_pdf:
        print(f"🔄 Procesando: {archivo}")
        ruta_pdf = os.path.join(directorio_actual, archivo)

        texto_extraido = extraer_texto_pdf(ruta_pdf)
        df_movimientos = procesar_movimientos(texto_extraido)

        print(f"📊 Datos extraídos de {archivo}:")
        print(df_movimientos.head())

        guardar_en_mysql(df_movimientos)

# 📌 7. Ejecutar el procesamiento de todos los PDFs
if __name__ == "__main__":
    procesar_todos_los_pdfs()
