"""
INGESTA DE METADATA ESTRUCTURADA - Pipeline de Captura CCTV
===========================================================

Módulo que:
1. Lee un CSV con datos de plantas de revisión técnica
2. Enriquece con información de cámaras activas
3. Genera y sube catálogos de plantas a S3
4. Genera metadata por captura de imagen

Cumple criterios académicos:
- Fuente estructurada: CSV local
- Automatización: integrado en pipeline
- Logging: trazabilidad de inicio/éxito/error/cantidad
- Ejecutable: puede correr standalone o integrado
"""

import csv
import json
import asyncio
import logging
import os
from datetime import datetime

import aiohttp
import aioboto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

# ===========================
# Configuración
# ===========================

METADATA_PREFIX = os.getenv("METADATA_PREFIX", "metadata")
PLANTAS_CSV_PATH = os.getenv("PLANTAS_CSV_PATH", "OrganizacionPlantas/plantas_revision_tecnica.csv")
METADATA_SNAPSHOT = os.getenv("METADATA_SNAPSHOT", "false").lower() == "true"
S3_BUCKET = os.getenv("S3_BUCKET", "flujo-prt-imagenes")

# ===========================
# Logging
# ===========================

logger = logging.getLogger("flujo-prt.metadata")

# Cache de metadata EC2 (se llena una vez al inicio)
_ec2_metadata_cache: dict | None = None

IMDS_BASE = "http://169.254.169.254/latest"
IMDS_TOKEN_URL = f"{IMDS_BASE}/api/token"
IMDS_TIMEOUT = aiohttp.ClientTimeout(total=2)


async def obtener_metadata_ec2() -> dict | None:
    """
    Consulta el Instance Metadata Service (IMDS v2) de EC2.

    Retorna dict con info de la instancia, o None si no está en EC2.
    Best-effort: timeout de 2s, no propaga excepciones.
    """
    try:
        async with aiohttp.ClientSession(timeout=IMDS_TIMEOUT) as session:
            # IMDSv2: obtener token
            async with session.put(
                IMDS_TOKEN_URL,
                headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"}
            ) as token_resp:
                if token_resp.status != 200:
                    logger.debug("[EC2] IMDS token request falló, no estamos en EC2")
                    return None
                token = await token_resp.text()

            headers = {"X-aws-ec2-metadata-token": token}

            # Consultar campos de metadata
            campos = {
                "instance_id": f"{IMDS_BASE}/meta-data/instance-id",
                "instance_type": f"{IMDS_BASE}/meta-data/instance-type",
                "availability_zone": f"{IMDS_BASE}/meta-data/placement/availability-zone",
                "ami_id": f"{IMDS_BASE}/meta-data/ami-id",
            }

            resultado = {}
            for key, url in campos.items():
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        resultado[key] = (await resp.text()).strip()

            # Derivar región de availability_zone (quitar última letra)
            az = resultado.get("availability_zone", "")
            if az:
                resultado["region"] = az[:-1]

            logger.info(f"[EC2] Metadata obtenida: {resultado.get('instance_type', '?')} en {resultado.get('region', '?')}")
            return resultado if resultado else None

    except Exception:
        logger.debug("[EC2] IMDS no disponible (probablemente no estamos en EC2)")
        return None


# ===========================
# FUNCIONES PURAS (Sin I/O)
# ===========================

def leer_csv_plantas(csv_path: str) -> list[dict]:
    """
    Lee el CSV de plantas y retorna lista de diccionarios crudos.

    Args:
        csv_path: Ruta al archivo CSV

    Returns:
        Lista de dicts con keys: Plataforma, Region, Comuna, Direccion, URL_Reserva

    Raises:
        FileNotFoundError: Si el CSV no existe
    """
    try:
        registros = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            registros = list(reader)

        plataformas = set(r.get("Plataforma", "").strip() for r in registros if r.get("Plataforma"))
        logger.info(f"CSV leído: {len(registros)} plantas de {len(plataformas)} plataformas")
        return registros

    except FileNotFoundError as e:
        logger.error(f"No se encontró CSV: {csv_path}")
        raise


def _normalizar_nombre_planta(nombre: str) -> str:
    """
    Normaliza nombres de plantas removiendo tildes y espacios extras.
    Permite cruzar "San Joaquín" (CSV) con "San Joaquin" (dict).
    """
    return nombre.strip().casefold().replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")


def construir_catalogo_plantas(
    plantas_csv: list[dict],
    camaras: dict,
    denominadores: dict,
    horarios: dict
) -> list[dict]:
    """
    Construye catálogo solo con plantas que tienen cámara CCTV activa.

    Cruza datos del CSV con el diccionario de cámaras usando normalización
    de tildes (ej: "San Joaquín" en CSV → "San Joaquin" en dict).

    Args:
        plantas_csv: Registros del CSV
        camaras: dict {planta: cam_id} del sistema
        denominadores: dict {planta: cod_3letras}
        horarios: dict {planta: {tipo: (inicio, fin)}}

    Returns:
        Lista de plantas con cámara activa enriquecidas con metadata
    """
    # Construir mapa de búsqueda normalizado
    camaras_norm = {_normalizar_nombre_planta(k): k for k in camaras.keys()}

    catalogo = []
    for row in plantas_csv:
        nombre = row.get("Comuna", "").strip()
        nombre_norm = _normalizar_nombre_planta(nombre)

        if nombre_norm not in camaras_norm:
            continue

        nombre_real = camaras_norm[nombre_norm]
        cam_id = camaras[nombre_real]
        denom = denominadores.get(nombre_real, nombre_real.replace(" ", "_"))
        horario = horarios.get(nombre_real, {})

        record = {
            "planta_id": denom,
            "nombre": nombre,
            "plataforma": row.get("Plataforma", "").strip(),
            "region": row.get("Region", "").strip(),
            "comuna": nombre,
            "direccion": row.get("Direccion", "").strip(),
            "url_reserva": row.get("URL_Reserva", "").strip(),
            "cam_id": cam_id,
            "horarios": {
                "semana": {
                    "apertura": horario.get("semana", ("", ""))[0],
                    "cierre": horario.get("semana", ("", ""))[1]
                },
                "sabado": {
                    "apertura": horario.get("sabado", ("", ""))[0],
                    "cierre": horario.get("sabado", ("", ""))[1]
                }
            }
        }
        catalogo.append(record)

    logger.info(f"Catálogo construido: {len(catalogo)} plantas con cámara activa (de {len(plantas_csv)} en CSV)")

    return catalogo


def generar_s3_key_metadata(planta: str, fecha_str: str, prefix: str = METADATA_PREFIX) -> str:
    """
    Genera la key S3 para un archivo de metadata, espejando la estructura de imagen.

    Transforma:
        capturas/YYYY/MM/DD/Planta/DEN_YYYYMMDD_HHMMSS.jpg
    A:
        metadata/capturas/YYYY/MM/DD/Planta/DEN_YYYYMMDD_HHMMSS.json
    """
    try:
        dt = datetime.strptime(fecha_str, "%Y%m%d_%H%M%S")
        # Importar DENOMINADORES solo cuando se necesita para evitar circular imports
        from imageRecopilator.Cloud.ImageRecompilerCloud import DENOMINADORES
        denom = DENOMINADORES.get(planta, planta.replace(" ", "_"))

        filename = f"{denom}_{fecha_str}.json"
        return (
            f"{prefix}/"
            f"capturas/"
            f"{dt.year}/"
            f"{dt.month:02d}/"
            f"{dt.day:02d}/"
            f"{planta}/"
            f"{filename}"
        )
    except Exception as e:
        logger.error(f"Error generando key metadata para {planta}: {e}")
        raise


def generar_metadata_captura(
    planta: str,
    fecha_str: str,
    s3_key_imagen: str,
    bytes_originales: int,
    bytes_comprimidos: int,
    bucket: str
) -> dict:
    """
    Genera el diccionario de metadata para una captura.

    Args:
        planta: Nombre de la planta (ej: "Huechuraba")
        fecha_str: Timestamp formato "YYYYMMDD_HHMMSS"
        s3_key_imagen: Key S3 completa de la imagen capturada
        bytes_originales: Tamaño original de la imagen
        bytes_comprimidos: Tamaño comprimido
        bucket: Nombre del bucket S3

    Returns:
        dict con estructura de metadata
    """
    try:
        from imageRecopilator.Cloud.ImageRecompilerCloud import DENOMINADORES
        timestamp_captura = datetime.strptime(fecha_str, "%Y%m%d_%H%M%S").isoformat(timespec='seconds')
        ratio = round(bytes_comprimidos / bytes_originales, 4) if bytes_originales > 0 else None

        # Subset ligero de EC2 metadata para cada captura
        ec2_info = None
        if _ec2_metadata_cache:
            ec2_info = {
                "instance_id": _ec2_metadata_cache.get("instance_id"),
                "instance_type": _ec2_metadata_cache.get("instance_type")
            }

        return {
            "version": "1",
            "planta_id": DENOMINADORES.get(planta, planta.replace(" ", "_")),
            "planta_nombre": planta,
            "plataforma": "TÜV Rheinland",
            "timestamp_captura": timestamp_captura,
            "fecha_str": fecha_str,
            "s3_imagen_key": s3_key_imagen,
            "s3_bucket": bucket,
            "bytes_originales": bytes_originales,
            "bytes_comprimidos": bytes_comprimidos,
            "ratio_compresion": ratio,
            "instancia_ec2": ec2_info,
            "generado_en": datetime.now().isoformat(timespec='seconds')
        }
    except Exception as e:
        logger.error(f"Error generando metadata para {planta}: {e}")
        raise


# ===========================
# FUNCIONES ASYNC (I/O en S3)
# ===========================

async def subir_json_s3(
    s3_client,
    bucket: str,
    key: str,
    payload: dict
) -> bool:
    """
    Sube un diccionario como JSON a S3.

    Args:
        s3_client: Cliente S3 aioboto3 abierto
        bucket: Nombre del bucket
        key: Key S3 destino
        payload: Diccionario a serializar

    Returns:
        True si éxito, False si error
    """
    try:
        body = json.dumps(payload, ensure_ascii=False, indent=None).encode('utf-8')
        await s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json"
        )
        return True
    except (BotoCoreError, ClientError) as e:
        logger.warning(f"[META] S3 error subiendo {key}: {e}")
        return False


async def ingestar_catalogo_plantas(
    bucket: str,
    prefix: str = METADATA_PREFIX,
    csv_path: str = PLANTAS_CSV_PATH
) -> int:
    """
    Orquesta la lectura del CSV y subida del catálogo a S3.

    Genera un JSON estructurado con:
    - Solo las plantas con cámara CCTV activa (14)
    - Metadata de la instancia EC2 donde se ejecuta el pipeline
    - Versión y timestamp de generación

    Args:
        bucket: Nombre del bucket S3
        prefix: Prefijo para las keys (default: METADATA_PREFIX)
        csv_path: Ruta al CSV

    Returns:
        Cantidad de registros subidos
    """
    global _ec2_metadata_cache

    logger.info("=" * 60)
    logger.info("=== INICIO INGESTA CATÁLOGO PLANTAS ===")
    logger.info("=" * 60)

    try:
        # Obtener metadata EC2 (best-effort, None si no estamos en EC2)
        _ec2_metadata_cache = await obtener_metadata_ec2()

        # Importar config del módulo de captura
        from imageRecopilator.Cloud.ImageRecompilerCloud import (
            camaras, DENOMINADORES, HORARIOS
        )

        # Leer y procesar CSV
        plantas_csv = leer_csv_plantas(csv_path)
        catalogo = construir_catalogo_plantas(plantas_csv, camaras, DENOMINADORES, HORARIOS)

        # Estructura envolvente con metadata del sistema
        payload = {
            "version": "1",
            "generado_en": datetime.now().isoformat(timespec='seconds'),
            "total_plantas": len(catalogo),
            "infraestructura": _ec2_metadata_cache,
            "plantas": catalogo
        }

        # Conectar a S3 y subir
        session = aioboto3.Session()
        async with session.client('s3') as s3:
            key_catalogo = f"{prefix}/plantas/catalogo_plantas.json"
            exito = await subir_json_s3(s3, bucket, key_catalogo, payload)

            if exito:
                logger.info(f"Catálogo subido: {len(catalogo)} plantas → s3://{bucket}/{key_catalogo}")
            else:
                logger.error(f"Fallo subiendo catálogo a {key_catalogo}")
                return 0

            # Snapshot fechado (opcional)
            if METADATA_SNAPSHOT:
                fecha_hoy = datetime.now().strftime("%Y%m%d")
                key_snapshot = f"{prefix}/plantas/catalogo_plantas_{fecha_hoy}.json"
                snapshot_ok = await subir_json_s3(s3, bucket, key_snapshot, payload)
                if snapshot_ok:
                    logger.info(f"Snapshot guardado: {key_snapshot}")

        logger.info("=" * 60)
        return len(catalogo)

    except Exception as e:
        logger.error(f"Error en ingesta catálogo: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0


async def subir_metadata_captura(
    s3_client,
    planta: str,
    fecha_str: str,
    s3_key_imagen: str,
    bytes_originales: int,
    bytes_comprimidos: int,
    bucket: str,
    prefix: str = METADATA_PREFIX
) -> None:
    """
    Genera y sube metadata JSON para una captura exitosa.

    Best-effort: fallo en metadata no interrumpe el pipeline de captura.

    Args:
        s3_client: Cliente S3 aioboto3 abierto (reutilizado)
        planta: Nombre de la planta
        fecha_str: Timestamp "YYYYMMDD_HHMMSS"
        s3_key_imagen: Key S3 de la imagen capturada
        bytes_originales: Tamaño original
        bytes_comprimidos: Tamaño comprimido
        bucket: Nombre del bucket
        prefix: Prefijo S3 para metadata
    """
    try:
        # Generar estrutura
        metadata = generar_metadata_captura(
            planta, fecha_str, s3_key_imagen, bytes_originales, bytes_comprimidos, bucket
        )

        # Generar key espejo
        key_metadata = generar_s3_key_metadata(planta, fecha_str, prefix)

        # Subir
        exito = await subir_json_s3(s3_client, bucket, key_metadata, metadata)

        if exito:
            logger.debug(f"[META] {planta} → s3://{bucket}/{key_metadata}")

    except Exception as e:
        # Best-effort: log pero no propagar excepción
        logger.warning(f"[META] No se pudo generar metadata para {planta}: {e}")


# ===========================
# Standalone Execution
# ===========================

async def verificar_credenciales_aws():
    """Verifica que las credenciales AWS estén disponibles."""
    logger.info("Verificando credenciales AWS...")

    session = aioboto3.Session()

    try:
        async with session.client('sts') as sts:
            identity = await sts.get_caller_identity()
            logger.info(f"✓ Credenciales AWS válidas")
            logger.info(f"  Account: {identity['Account']}")
            logger.info(f"  ARN: {identity['Arn']}")
            return True
    except NoCredentialsError:
        logger.critical("=" * 60)
        logger.critical("ERROR: NO SE ENCONTRARON CREDENCIALES AWS")
        logger.critical("=" * 60)
        return False
    except Exception as e:
        logger.critical(f"ERROR AL VERIFICAR CREDENCIALES: {e}")
        return False


async def main():
    """Punto de entrada: verifica credenciales y ejecuta ingesta de catálogo."""
    if not await verificar_credenciales_aws():
        logger.critical("ABORTANDO: Configura credenciales AWS primero")
        return

    await ingestar_catalogo_plantas(S3_BUCKET, METADATA_PREFIX, PLANTAS_CSV_PATH)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
    except Exception as e:
        logger.critical(f"Error fatal: {e}")
        import traceback
        logger.critical(traceback.format_exc())
