#!/usr/bin/env python3
"""
Script para descargar todo el contenido del bucket S3 localmente
Requiere: pip install boto3
"""

import boto3
import os
from datetime import datetime
from pathlib import Path
from botocore.exceptions import ClientError, NoCredentialsError

# =========================
# CONFIGURACIÓN
# =========================
S3_BUCKET = "flujoprtimagenes"
S3_PREFIX = "capturas/"
LOCAL_DIR = "./capturas_descargadas"

# Si usas perfil AWS específico, descomenta y configura:
# AWS_PROFILE = "mi-perfil"
# session = boto3.Session(profile_name=AWS_PROFILE)
# s3 = session.client('s3')

# Para credenciales por defecto:
s3 = boto3.client('s3')


def descargar_bucket():
    """Descarga todo el contenido del bucket S3"""
    
    print("="*60)
    print(f"DESCARGANDO BUCKET: s3://{S3_BUCKET}/{S3_PREFIX}")
    print(f"DESTINO LOCAL: {LOCAL_DIR}")
    print("="*60)
    
    # Crear directorio local si no existe
    Path(LOCAL_DIR).mkdir(parents=True, exist_ok=True)
    
    try:
        # Verificar credenciales
        s3.list_buckets()
        print("✓ Credenciales AWS verificadas")
    except NoCredentialsError:
        print("✗ ERROR: No se encontraron credenciales AWS")
        print("\nConfigura credenciales usando uno de estos métodos:")
        print("  1. aws configure")
        print("  2. Variables de entorno: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
        print("  3. Archivo ~/.aws/credentials")
        return
    except Exception as e:
        print(f"✗ ERROR verificando credenciales: {e}")
        return
    
    # Listar todos los objetos del bucket
    print("\nListando objetos en S3...")
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
    
    total_archivos = 0
    total_bytes = 0
    descargados = 0
    omitidos = 0
    errores = 0
    
    for page in pages:
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            key = obj['Key']
            size = obj['Size']
            total_archivos += 1
            total_bytes += size
            
            # Crear ruta local preservando estructura S3
            local_path = os.path.join(LOCAL_DIR, key)
            local_dir = os.path.dirname(local_path)
            
            # Crear directorios si no existen
            Path(local_dir).mkdir(parents=True, exist_ok=True)
            
            # Verificar si el archivo ya existe y tiene el mismo tamaño
            if os.path.exists(local_path):
                local_size = os.path.getsize(local_path)
                if local_size == size:
                    omitidos += 1
                    if omitidos % 100 == 0:
                        print(f"  Omitidos: {omitidos} (ya existen)")
                    continue
            
            # Descargar archivo
            try:
                s3.download_file(S3_BUCKET, key, local_path)
                descargados += 1
                
                if descargados % 10 == 0:
                    print(f"  Descargados: {descargados}/{total_archivos} - Último: {os.path.basename(key)}")
                    
            except ClientError as e:
                errores += 1
                print(f"  ✗ Error descargando {key}: {e}")
            except Exception as e:
                errores += 1
                print(f"  ✗ Error inesperado con {key}: {e}")
    
    # Resumen
    print("\n" + "="*60)
    print("RESUMEN DE DESCARGA")
    print("="*60)
    print(f"Total archivos en S3: {total_archivos}")
    print(f"Descargados: {descargados}")
    print(f"Omitidos (ya existían): {omitidos}")
    print(f"Errores: {errores}")
    print(f"Tamaño total: {total_bytes / (1024**3):.2f} GB")
    print(f"Directorio local: {os.path.abspath(LOCAL_DIR)}")
    print("="*60)


def listar_estructura():
    """Muestra la estructura del bucket sin descargar"""
    
    print("="*60)
    print(f"ESTRUCTURA DEL BUCKET: s3://{S3_BUCKET}/{S3_PREFIX}")
    print("="*60)
    
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX, Delimiter='/')
    
    # Obtener "carpetas" (prefijos comunes)
    for page in pages:
        if 'CommonPrefixes' in page:
            print("\nCarpetas encontradas:")
            for prefix in page['CommonPrefixes']:
                print(f"  📁 {prefix['Prefix']}")
        
        if 'Contents' in page:
            count = len(page['Contents'])
            total_size = sum(obj['Size'] for obj in page['Contents'])
            print(f"\nArchivos: {count}")
            print(f"Tamaño total: {total_size / (1024**3):.2f} GB")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "list":
        listar_estructura()
    else:
        print("\nOpciones:")
        print("  python download_s3_bucket.py        → Descargar todo el bucket")
        print("  python download_s3_bucket.py list   → Solo listar estructura")
        print()
        
        respuesta = input("¿Descargar todo el bucket? (s/N): ")
        if respuesta.lower() in ['s', 'si', 'sí', 'yes', 'y']:
            descargar_bucket()
        else:
            print("Operación cancelada")
