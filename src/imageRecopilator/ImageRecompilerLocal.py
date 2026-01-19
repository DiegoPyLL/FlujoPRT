import asyncio
import aiohttp
import time
from datetime import datetime
import os
import ssl

# URL base del servidor de cámaras
BASE_URL = "https://pti-cameras.cl.tuv.com/camaras"

# Directorio donde se guardarán las imágenes capturadas
BASE_DIR = r"C:/Users/Laptop/Desktop/Trabajos/ProyectosPersonales/FlujoPRT_Main/RecompilacionFotos"

# Intervalo entre capturas en segundos (60 = 1 minuto)
INTERVALO = 60
# Tiempo de espera cuando está fuera de horario en segundos (600 = 10 minutos)
REINTENTO_FUERA_HORARIO = 600




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
# Define horas de inicio y fin para días de semana y sábados
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


# Configuración SSL para permitir conexiones sin verificar certificados
# Útil para servidores con certificados autofirmados
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE




# Verifica si hoy es domingo
def es_domingo():
    return datetime.now().weekday() == 6




# Verifica si la hora actual está dentro del horario de operación de una planta
def dentro_horario(planta):
    ahora = datetime.now()
    dia = ahora.weekday()

    # Si es domingo, retorna False
    if dia == 6:
        return False

    # Determina si es sábado o día de semana
    tipo = "sabado" if dia == 5 else "semana"
    inicio, fin = HORARIOS[planta][tipo]

    hora_inicio = datetime.strptime(inicio, "%H:%M").time()
    hora_fin = datetime.strptime(fin, "%H:%M").time()

    return hora_inicio <= ahora.time() <= hora_fin




# Calcula cuántos segundos faltan para que abra la planta
def segundos_hasta_apertura(planta):
    ahora = datetime.now()
    dia = ahora.weekday()

    # Si es domingo, no hay apertura
    if dia == 6:
        return None

    tipo = "sabado" if dia == 5 else "semana"
    inicio, _ = HORARIOS[planta][tipo]
    hora_inicio = datetime.strptime(inicio, "%H:%M").time()
    apertura = datetime.combine(ahora.date(), hora_inicio)

    # Si aún no llega la hora de apertura, calcula el tiempo restante
    if ahora.time() <= hora_inicio:
        delta = (apertura - ahora).total_seconds()
        # Retorna mínimo 60 segundos, máximo el tiempo configurado de reintento
        return max(60, min(delta, REINTENTO_FUERA_HORARIO))

    return REINTENTO_FUERA_HORARIO




async def capturar_camara(session, planta, cam_id):
    denominador = DENOMINADORES.get(planta, planta.replace(" ", "_"))

    while True:
        now = datetime.now()

        # Carpeta SIEMPRE basada en el día actual
        carpeta = os.path.join(
            BASE_DIR,            
            f"{now.year}",
            f"{now.month:02d}",
            f"{now.day:02d}",
            planta.replace(" ", "_")
        )
        os.makedirs(carpeta, exist_ok=True)

        if es_domingo():
            print(f"\033[1m{planta}\033[0m domingo.- Captura deshabilitada.")
            break

        if not dentro_horario(planta):
            espera = segundos_hasta_apertura(planta)
            if espera is None:
                break
            print(f"\033[1m{planta}\033[0m fuera de horario.- Reintentando en {int(espera/60)} min.")
            await asyncio.sleep(espera)
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
                        print(f"\033[1m{planta}\033[0m - Intento {intento + 1}/5 HTTP {resp.status or Exception}")

            except Exception as e:
                print(f"\033[1m{planta}\033[0m - Intento {intento + 1}/5 error: {e}")

            await asyncio.sleep(2.5)

        if not exito:
            print(f"\033[1m{planta}\033[0m no respondió. Se reintentará en el próximo ciclo.")

        await asyncio.sleep(INTERVALO)





# Función principal que coordina todas las capturas
async def main():
    # Configura el conector HTTP con SSL personalizado
    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=50)
    timeout = aiohttp.ClientTimeout(total=20)

    # Crea una sesión HTTP compartida
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:
        # Ejecuta todas las tareas de captura en paralelo
        await asyncio.gather(
            *[capturar_camara(session, planta, cam_id) for planta, cam_id in camaras.items()]
        )




# Inicia la ejecución del programa
if __name__ == "__main__":
    asyncio.run(main())