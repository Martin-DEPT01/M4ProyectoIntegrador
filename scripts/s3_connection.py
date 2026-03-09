import boto3
from botocore.exceptions import NoCredentialsError, ClientError

def connect_to_s3():
    try:
        s3 = boto3.client("s3")

        # Llamada mínima para validar conexión y credenciales
        s3.list_buckets()

        print("Conexión a S3 exitosa ✅")
        return s3

    except NoCredentialsError:
        print("No se encontraron credenciales AWS. Ejecutá `aws configure`.")
        raise

    except ClientError as e:
        print(f"Error de AWS: {e}")
        raise


if __name__ == "__main__":
    s3_client = connect_to_s3()
