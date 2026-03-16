import os
import s3_connection as s3_conn
from dotenv import load_dotenv
from pathlib import Path


def main():

    # Carga las variables del archivo .env
    load_dotenv()

    DATA_PROJ = os.getenv("JSON_PATH").strip("/")
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_PATH = BASE_DIR / DATA_PROJ

    lugares = ["Patagonia", "Riohacha"]
    bucket_name = os.getenv("BUCKET_RAW")

    s3 = s3_conn.connect_to_s3()

    historico_encontrado = {}

    for lugar in lugares:

        found = False
        for archivo in DATA_PATH.iterdir():
            # Verificamos si es un archivo y si el string está en el nombre

            if archivo.is_file() and lugar in archivo.name:
                # Asignamos el nombre completo (o la ruta completa) a una variable
                nombre_completo = archivo.name
                ruta_completa = str(archivo)
                historico_encontrado[lugar] = ruta_completa

                print(f"Encontrado para {lugar}: {nombre_completo}")
                s3_path = f"bronze/historicos/{nombre_completo}"
                s3.upload_file(historico_encontrado[lugar], bucket_name, s3_path)
                print(f"✅Archivo subido a s3://{bucket_name}/{s3_path}")
                found = True
                break # Si encontrás el primero, pasás al siguiente lugar

        if not found:
            print(f"No se encontró ningún archivo para: {lugar}")

# Permite ejecutar el archivo directamente
if __name__ == "__main__":
    main()



