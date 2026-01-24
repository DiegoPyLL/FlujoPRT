import asyncio
import aiohttp
import time
from datetime import datetime, timedelta
import os
import ssl
import hashlib
import subprocess
import tempfile
import shutil
from pathlib import Path
from collections import defaultdict
from PIL import Image

"""
SISTEMA COMPLETO LOCAL: CAPTURA + PROCESAMIENTO DOMINICAL
==========================================================

FUNCIONAMIENTO:
---------------
1. Loop principal infinito
2. Lunes-Sábado: Captura imágenes cada 60s y guarda localmente
3. Domingos: Ejecuta job de procesamiento (deduplicación + timelapses)
4. Vuelve al paso 1

PROCESAMIENTO DOMINICAL:
------------------------
- Deduplicación por hash MD5
- Descompresión a quality=100
- Validación de resolución
- Generación de timelapses con ffmpeg
- Eliminación de imágenes procesadas
"""

# URL base del servidor de cámaras
BASE_URL = "https://pti-cameras.cl.tuv.com/camaras"

# Directorio donde se guardarán las imágenes capturadas
BASE_DIR = r"C:/Users/Laptop/Desktop/Trabajos/ProyectosPersonales/FlujoPRT_Main/RecompilacionFotos"

# Directorio donde se guardarán los timelapses
TIMELAPSES_DIR = r"C:/Users/Laptop/Desktop/Trabajos/ProyectosPersonales/FlujoPRT_Main/Timelapses"

# Intervalo entre capturas en segundos (60 = 1 minuto)
INTERVALO = 60
# Minutos antes de la apertura para reactivar (20 minutos)
MARGEN_PREVIO = 20 * 60  # 20 minutos en segundos

# Diccionario con los nombres de plantas y sus IDs de cámara correspondientes
camaras = {
    "Huechuraba": "10.57.6.222_Cam08",
    "La Florida": "10.57.0.222_Cam03",
    "La Pintana": "10.57.5.222_Cam09",
    "Pudahuel": "10.57.4.222_Cam07",
    "Quilicura": "10.57.2.222_Cam06",
    "Recoleta": "10.57.7.222_Cam09",
    "San Joaquin": "10.57.3.222_Cam07",
    "Temuco": "10.57.32.222_Cam01",
    "Villarica": "10.57.33.222_Cam04",
    "Chillan": "10.57.12.70",
    "Yungay": "10.57.20.70",
    "Concepcion": "10.57.19.70",
    "San Pedro de la Paz": "10.57.16.70",
    "Yumbel": "10.57.17.70"
}

# Horarios de operación para cada planta
HORARIOS = {
    "Huechuraba": {"semana": ("07:10", "16:50"), "sabado": ("07:10", "16:50")},
    "La Florida": {"semana": ("07:40", "17:20"), "sabado": ("07:10", "16:50")},
    "La Pintana": {"semana": ("07:40", "17:20"), "sabado": ("07:10", "16:50")},
    "Pudahuel": {"semana": ("07:40", "17:20"), "sabado": ("07:10", "16:50")},
    "Quilicura": {"semana": ("07:10", "16:50"), "sabado": ("07:10", "16:50")},
    "Recoleta": {"semana": ("07:40", "17:20"), "sabado": ("07:10", "16:50")},
    "San Joaquin": {"semana": ("07:40", "17:20"), "sabado": ("07:10", "16:50")},
    "Temuco": {"semana": ("08:10", "18:20"), "sabado": ("08:10", "13:50")},
    "Villarica": {"semana": ("07:10", "17:50"), "sabado": ("07:40", "13:50")},
    "Chillan": {"semana": ("06:40", "17:20"), "sabado": ("07:10", "13:50")},
    "Yungay": {"semana": ("07:40", "17:20"), "sabado": ("08:10", "13:50")},
    "Concepcion": {"semana": ("07:40", "20:20"), "sabado": ("08:10", "16:50")},
    "San Pedro de la Paz": {"semana": ("07:40", "17:20"), "sabado": ("08:10", "13:50")},
    "Yumbel": {"semana": ("07:40", "17:20"), "sabado": ("08:10", "13:50")}
}

DENOMINADORES = {
    "Huechuraba": "HCH",
    "La Florida": "LFL",
    "La Pintana": "LPT",
    "Pudahuel": "PUD",
    "Quilicura": "QLC",
    "Recoleta": "RCL",
    "San Joaquin": "SJQ",
    "Temuco": "TMU",
    "Villarica": "VLL",
    "Chillan": "CHL",
    "Yungay": "YGY",
    "Concepcion": "CCP",
    "San Pedro de la Paz": "SPP",
    "Yumbel": "YMB"
}

# Configuración SSL
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


# =========================
# Utilidades de horarios
# =========================

def es_domingo():
    return datetime.now().weekday() == 6


def dentro_horario(planta):
    ahora = datetime.now()
    dia = ahora.weekday()

    if dia == 6:
        return False

    tipo = "sabado" if dia == 5 else "semana"
    inicio, fin = HORARIOS[planta][tipo]

    hora_inicio = datetime.strptime(inicio, "%H:%M").time()
    hora_fin = datetime.strptime(fin, "%H:%M").time()

    return hora_inicio <= ahora.time() <= hora_fin


def segundos_hasta_apertura(planta):
    ahora = datetime.now()
    dia = ahora.weekday()

    if dia == 6:
        return None

    tipo = "sabado" if dia == 5 else "semana"
    inicio, _ = HORARIOS[planta][tipo]

    hora_inicio = datetime.strptime(inicio, "%H:%M").time()
    apertura = datetime.combine(ahora.date(), hora_inicio)

    if ahora.time() < hora_inicio:
        return int((apertura - ahora).total_seconds())
    else:
        dia_siguiente = (dia + 1) % 7
        
        if dia_siguiente == 6:
            return None
            
        tipo_siguiente = "sabado" if dia_siguiente == 5 else "semana"
        inicio_siguiente, _ = HORARIOS[planta][tipo_siguiente]
        
        hora_inicio_siguiente = datetime.strptime(inicio_siguiente, "%H:%M").time()
        apertura_siguiente = datetime.combine(ahora.date(), hora_inicio_siguiente) + timedelta(days=1)
        
        segundos = int((apertura_siguiente - ahora).total_seconds())
        return segundos


def todas_fuera_de_horario():
    for planta in camaras.keys():
        if dentro_horario(planta):
            return False
    return True


def obtener_tiempos_restantes():
    tiempos = {}
    for planta in camaras.keys():
        if es_domingo():
            tiempos[planta] = None
        elif dentro_horario(planta):
            tiempos[planta] = 0
        else:
            tiempos[planta] = segundos_hasta_apertura(planta)
    return tiempos


def obtener_menor_tiempo_espera():
    tiempos = obtener_tiempos_restantes()
    tiempos_validos = [t for t in tiempos.values() if t is not None and t > 0]
    
    if not tiempos_validos:
        return None
    
    return min(tiempos_validos)


async def esperar_hasta_apertura():
    while True:
        if es_domingo():
            ahora = datetime.now()
            lunes = ahora + timedelta(days=1)
            while lunes.weekday() != 0:
                lunes += timedelta(days=1)
            
            primer_hora = min(HORARIOS[p]["semana"][0] for p in camaras.keys())
            hora_apertura = datetime.strptime(primer_hora, "%H:%M").time()
            apertura_lunes = datetime.combine(lunes.date(), hora_apertura)
            
            despertar = apertura_lunes - timedelta(seconds=MARGEN_PREVIO)
            segundos = int((despertar - ahora).total_seconds())
            
            horas = segundos // 3600
            minutos = (segundos % 3600) // 60
            print(f"Domingo. Suspendiendo {horas}h {minutos}min (hasta 20 min antes de apertura del lunes)...")
            await asyncio.sleep(segundos)
            continue
        
        if todas_fuera_de_horario():
            menor = obtener_menor_tiempo_espera()
            if menor:
                espera_real = max(0, menor - MARGEN_PREVIO)
                minutos = int(espera_real / 60)
                print(f"Todas fuera de horario. Suspendiendo {minutos} min (hasta 20 min antes de apertura)...")
                await asyncio.sleep(espera_real)
            else:
                print("Sin aperturas inmediatas. Verificando en 1 hora...")
                await asyncio.sleep(3600)
        else:
            break


# =========================
# Captura
# =========================

async def capturar_camara(session, planta, cam_id):
    denominador = DENOMINADORES.get(planta, planta.replace(" ", "_"))

    while True:
        now = datetime.now()
        
        carpeta = os.path.join(
            BASE_DIR,            
            f"{now.year}",
            f"{now.month:02d}",
            f"{now.day:02d}",
            planta.replace(" ", "_")
        )
        os.makedirs(carpeta, exist_ok=True)

        if not dentro_horario(planta):
            await asyncio.sleep(INTERVALO)
            continue

        pitime = int(time.time())
        fecha = now.strftime("%Y%m%d_%H%M%S")
        url = f"{BASE_URL}/{cam_id}/imagen.jpg"

        exito = False

        for intento in range(5):
            try:
                async with session.get(url, params={"pitime": pitime}) as resp:
                    if resp.status == 200:
                        data = await resp.read()
                        nombre_archivo = f"{denominador}_{fecha}.jpg"

                        with open(os.path.join(carpeta, nombre_archivo), "wb") as f:
                            f.write(data)

                        print(f"\033[1m{planta}\033[0m - Imagen guardada: {nombre_archivo}")
                        exito = True
                        break
                    else:
                        print(f"\033[1m{planta}\033[0m - Intento {intento + 1}/5 HTTP {resp.status}")

            except Exception as e:
                print(f"\033[1m{planta}\033[0m - Intento {intento + 1}/5 error: {e}")

            await asyncio.sleep(2.5)

        if not exito:
            print(f"\033[1m{planta}\033[0m no respondió.")

        await asyncio.sleep(INTERVALO)


# ====================================
# SUNDAY WORKER - Procesamiento Dominical LOCAL
# ====================================

class SundayWorkerLocal:
    def __init__(self):
        self.procesado_semana = None
    
    def obtener_semana_anterior(self):
        """Retorna (año, semana) del lunes-sábado pasado"""
        hoy = datetime.now()
        inicio_semana_anterior = hoy - timedelta(days=hoy.weekday() + 1)
        return inicio_semana_anterior.isocalendar()[:2]
    
    def hash_archivo(self, filepath):
        """Calcula MD5 de un archivo"""
        md5 = hashlib.md5()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                md5.update(chunk)
        return md5.hexdigest()
    
    def identificar_conjuntos(self, semana):
        """Identifica todas las imágenes de la semana anterior"""
        año, num_semana = semana
        inicio = datetime.strptime(f"{año}-W{num_semana:02d}-1", "%Y-W%W-%w")
        
        conjuntos = defaultdict(list)
        
        for dia_offset in range(6):  # lunes a sábado
            fecha = inicio + timedelta(days=dia_offset)
            
            # Buscar carpetas de ese día
            carpeta_dia = Path(BASE_DIR) / str(fecha.year) / f"{fecha.month:02d}" / f"{fecha.day:02d}"
            
            if not carpeta_dia.exists():
                continue
            
            # Recorrer cada planta
            for planta_dir in carpeta_dia.iterdir():
                if not planta_dir.is_dir():
                    continue
                
                planta = planta_dir.name.replace("_", " ")
                dia_key = f"{fecha.date()}"
                
                # Listar todas las imágenes .jpg
                for img_file in planta_dir.glob("*.jpg"):
                    conjuntos[(planta, dia_key)].append({
                        'path': img_file,
                        'size': img_file.stat().st_size
                    })
        
        print(f"Identificados {len(conjuntos)} conjuntos planta/día")
        return conjuntos
    
    def deduplicar_imagenes(self, conjuntos):
        """Elimina duplicados por hash MD5"""
        total_duplicados = 0
        
        for (planta, dia), imagenes in conjuntos.items():
            if len(imagenes) < 2:
                continue
            
            print(f"Deduplicando {planta}/{dia} ({len(imagenes)} imágenes)")
            
            hashes_vistos = {}
            duplicados = []
            
            for img in imagenes:
                file_hash = self.hash_archivo(img['path'])
                
                if file_hash in hashes_vistos:
                    duplicados.append(img['path'])
                    img['duplicado'] = True
                else:
                    hashes_vistos[file_hash] = img['path']
                    img['duplicado'] = False
            
            # Eliminar duplicados
            for dup_path in duplicados:
                dup_path.unlink()
                total_duplicados += 1
            
            print(f"  → {len(duplicados)} duplicados eliminados")
        
        print(f"\nTotal duplicados eliminados: {total_duplicados}")
    
    def generar_timelapses(self, conjuntos):
        """Genera timelapses por planta"""
        por_planta = defaultdict(list)
        
        for (planta, dia), imagenes in conjuntos.items():
            # Filtrar duplicados
            imagenes_unicas = [img for img in imagenes if not img.get('duplicado', False)]
            por_planta[planta].extend(imagenes_unicas)
        
        for planta, imagenes in por_planta.items():
            if not imagenes:
                continue
            
            # Ordenar por path (contiene timestamp en el nombre)
            imagenes_sorted = sorted(imagenes, key=lambda x: str(x['path']))
            
            print(f"[GENERANDO] {planta} - {len(imagenes_sorted)} frames")
            
            self.crear_timelapse(planta, imagenes_sorted)
    
    def crear_timelapse(self, planta, imagenes):
        """Crea timelapse con descompresión a quality=100"""
        año, semana = self.obtener_semana_anterior()
        
        # Calcular rango de fechas
        inicio = datetime.strptime(f"{año}-W{semana:02d}-1", "%Y-W%W-%w")
        fin = inicio + timedelta(days=5)  # sábado
        nombre_rango = f"{inicio.day:02d}_{inicio.month:02d}-{fin.day:02d}_{fin.month:02d}"
        
        # Crear directorio temporal
        with tempfile.TemporaryDirectory() as tmpdir:
            resolucion_ref = None
            frames_validos = []
            archivos_procesados = []
            
            # Procesar cada imagen
            for img_info in imagenes:
                archivos_procesados.append(img_info['path'])
                
                try:
                    img = Image.open(img_info['path'])
                    resolucion = img.size
                    
                    # Validar resolución
                    if resolucion_ref is None:
                        resolucion_ref = resolucion
                    elif resolucion != resolucion_ref:
                        print(f"  [WARN] Resolución inconsistente: {img_info['path'].name}")
                        continue
                    
                    # DESCOMPRIMIR a quality=100
                    img = img.convert("RGB")
                    frame_path = Path(tmpdir) / f"{len(frames_validos):06d}.jpg"
                    img.save(
                        frame_path,
                        format="JPEG",
                        quality=100,
                        optimize=False,
                        subsampling=0
                    )
                    
                    frames_validos.append(frame_path)
                    
                except Exception as e:
                    print(f"  [ERROR] Procesando {img_info['path'].name}: {e}")
                    continue
                
                if len(frames_validos) % 100 == 0 and len(frames_validos) > 0:
                    print(f"  Procesados {len(frames_validos)} frames...")
            
            if len(frames_validos) < 10:
                print(f"  [ERROR] Solo {len(frames_validos)} frames válidos, abortando")
                return
            
            print(f"  Total frames válidos: {len(frames_validos)}")
            print(f"  Generando video con ffmpeg...")
            
            # Crear directorio de salida
            output_dir = Path(TIMELAPSES_DIR) / str(año) / f"semana_{semana:02d}"
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Nombre con rango de fechas
            video_path = output_dir / f"{planta.replace(' ', '_')}_{nombre_rango}.mp4"
            
            # Generar video con ffmpeg
            result = subprocess.run([
                'ffmpeg', '-y',
                '-framerate', '30',
                '-pattern_type', 'glob',
                '-i', str(Path(tmpdir) / '*.jpg'),
                '-c:v', 'libx264',
                '-preset', 'fast',
                '-crf', '28',
                '-pix_fmt', 'yuv420p',
                str(video_path)
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"  [ERROR] ffmpeg falló: {result.stderr}")
                return
            
            print(f"  → Video guardado: {video_path}")
            
            # ELIMINAR IMÁGENES PROCESADAS
            print(f"  Eliminando {len(archivos_procesados)} imágenes...")
            for img_path in archivos_procesados:
                try:
                    img_path.unlink()
                except Exception as e:
                    print(f"  [ERROR] No se pudo eliminar {img_path}: {e}")
            
            # Eliminar carpetas vacías
            for img_path in archivos_procesados:
                try:
                    parent = img_path.parent
                    if parent.exists() and not any(parent.iterdir()):
                        parent.rmdir()
                except:
                    pass
    
    def ejecutar(self):
        """Ejecuta el procesamiento dominical"""
        semana_actual = self.obtener_semana_anterior()
        
        # Evitar reprocesar la misma semana
        if self.procesado_semana == semana_actual:
            print("Semana ya procesada, esperando próximo domingo...")
            return
        
        año, num_semana = semana_actual
        
        print("="*60)
        print("INICIO PROCESAMIENTO DOMINICAL")
        print(f"Procesando semana {año}-W{num_semana:02d}")
        print("="*60)
        
        conjuntos = self.identificar_conjuntos(semana_actual)
        self.deduplicar_imagenes(conjuntos)
        self.generar_timelapses(conjuntos)
        
        self.procesado_semana = semana_actual
        
        print("="*60)
        print("FIN PROCESAMIENTO DOMINICAL")
        print("="*60)


# =========================
# Main
# =========================

async def main():
    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=50)
    timeout = aiohttp.ClientTimeout(total=20)
    
    sunday_worker = SundayWorkerLocal()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        while True:
            # Si es domingo, ejecutar procesamiento
            if es_domingo():
                print("DOMINGO DETECTADO - Iniciando procesamiento de semana anterior...")
                sunday_worker.ejecutar()
                
                # Suspender hasta el lunes
                await esperar_hasta_apertura()
                continue
            
            # Lunes-Sábado: modo captura normal
            await esperar_hasta_apertura()
            
            await asyncio.gather(
                *[capturar_camara(session, planta, cam_id) for planta, cam_id in camaras.items()]
            )


if __name__ == "__main__":
    asyncio.run(main())