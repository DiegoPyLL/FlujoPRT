import asyncio
import aiohttp
import time
from datetime import datetime
import os
import ssl


BASE_URL = "https://pti-cameras.cl.tuv.com/camaras"

# Directorio base para guardar las imágenes
BASE_DIR = r"C:/Users/Laptop/Desktop/Trabajos/ProyectosPersonales/FlujoPRT_Main/RecompilacionFotos"

# intervalo de captura en segundos
INTERVALO = 60

# cada 10 minutos intenta capturar fuera de horario
REINTENTO_FUERA_HORARIO = 600

MAX_ERRORES = 60


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

# Se inicia 20 minutos antes y despues para capturar el flujo completo vehicular
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
    "Chillan": {"semana": ("07:00", "17:20"), "sabado": ("07:10", "13:50")},
    "Yungay": {"semana": ("07:40", "17:20"), "sabado": ("08:10", "13:50")},
    "Concepcion": {"semana": ("07:40", "20:20"), "sabado": ("08:10", "16:50")},
    "San Pedro de la Paz": {"semana": ("07:40", "17:20"), "sabado": ("08:10", "13:50")},
    "Yumbel": {"semana": ("07:40", "17:20"), "sabado": ("08:10", "13:50")}
}




# Configuración SSL para ignorar la verificación del certificado. 
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE



# 0 = Lunes, 6 = Domingo
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

    if ahora.time() <= hora_inicio:
        delta = (apertura - ahora).total_seconds()
        return max(60, min(delta, REINTENTO_FUERA_HORARIO))

    return REINTENTO_FUERA_HORARIO




async def capturar_camara(session, planta, cam_id):
    # Crea la carpeta destino para la planta, reemplazando espacios por guiones bajos
    carpeta = os.path.join(BASE_DIR, planta.replace(" ", "_"))
    os.makedirs(carpeta, exist_ok=True)

    # Contador de errores consecutivos de red o HTTP
    contador_errores = 0

    # Loop principal de captura para la cámara
    while True:

        # Si es domingo, se deshabilita completamente la captura y se termina la tarea
        if es_domingo():
            print(f"{planta} domingo. Captura deshabilitada.")
            break

        # Si la planta está fuera de horario operativo
        if not dentro_horario(planta):
            # Calcula cuántos segundos esperar antes de reintentar
            espera = segundos_hasta_apertura(planta)

            # Si no hay próxima apertura válida, se corta la tarea
            if espera is None:
                break

            # Espera antes de volver a evaluar el horario
            print(f"{planta} está fuera de horario. Reintentando captura en {int(espera/60)} min.")
            await asyncio.sleep(espera)
            continue

        # Timestamp usado como parámetro para evitar cache del servidor
        pitime = int(time.time())

        # Nombre de archivo con fecha, hora y microsegundos para evitar colisiones
        fecha = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

        # URL de la imagen de la cámara
        url = f"{BASE_URL}/{cam_id}/imagen.jpg"

        try:
            # Solicitud HTTP asíncrona para obtener la imagen
            async with session.get(url, params={"pitime": pitime}) as resp:
                if resp.status == 200:
                    # Lee el binario de la imagen
                    data = await resp.read()

                    # Guarda la imagen en disco
                    with open(os.path.join(carpeta, f"{fecha}.jpg"), "wb") as f:
                        f.write(data)

                    print(f"{planta} - Imagen guardada: {fecha}.jpg")

                    # Reset del contador de errores al tener éxito
                    contador_errores = 0
                else:
                    # Error HTTP distinto de 200
                    contador_errores += 1
                    print(f"{planta} HTTP {resp.status}")

        except Exception as e:
            # Error de red, timeout u otro problema de conexión
            contador_errores += 1
            print(f"{planta} error de conexión: {e}")

        # Si se alcanzan demasiados errores consecutivos, se detiene la captura
        if contador_errores >= MAX_ERRORES:
            print(f"{planta} detenido por {MAX_ERRORES} errores consecutivos.")
            break

        # Espera el intervalo definido antes de la siguiente captura
        await asyncio.sleep(INTERVALO)



# Creación de la sesión y ejecución de tareas
async def main():
    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=50)
    timeout = aiohttp.ClientTimeout(total=20)

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:
        await asyncio.gather(
            *[capturar_camara(session, planta, cam_id) for planta, cam_id in camaras.items()]
        )



# Ejecución del script
asyncio.run(main())
print("Recompilación finalizada.")
