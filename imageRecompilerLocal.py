import asyncio
import aiohttp
import time
from datetime import datetime
import os
import ssl


#Base URL y configuración
BASE_URL = "https://pti-cameras.cl.tuv.com/camaras"
BASE_DIR = r"C:/Users/Laptop/Desktop/Trabajos/ProyectosPersonales/FlujoPRT_Main/RecompilacionFotos"


# El servidor actualiza la imagen cada 60 con un pequeño desfase
INTERVALO = 60      


#Direccion de servidor de cada planta
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


# Rangos de horarios por planta
HORARIOS = {
    "Huechuraba": {"semana": ("07:30", "16:30"), "sabado": ("07:30", "16:30")},
    "La Florida": {"semana": ("08:00", "17:00"), "sabado": ("07:30", "16:30")},
    "La Pintana": {"semana": ("08:00", "17:00"), "sabado": ("07:30", "16:30")},
    "Pudahuel": {"semana": ("08:00", "17:00"), "sabado": ("07:30", "16:30")},
    "Quilicura": {"semana": ("07:30", "16:30"), "sabado": ("07:30", "16:30")},
    "Recoleta": {"semana": ("08:00", "17:00"), "sabado": ("07:30", "16:30")},
    "San Joaquin": {"semana": ("08:00", "17:00"), "sabado": ("07:30", "16:30")},
    "Temuco": {"semana": ("08:30", "18:00"), "sabado": ("08:30", "13:30")},
    "Villarica": {"semana": ("07:30", "17:30"), "sabado": ("08:00", "13:30")},
    "Chillan": {"semana": ("07:00", "17:00"), "sabado": ("07:30", "13:30")},
    "Yungay": {"semana": ("08:00", "17:00"), "sabado": ("08:30", "13:30")},
    "Concepcion": {"semana": ("08:00", "20:00"), "sabado": ("08:30", "16:30")},
    "San Pedro de la Paz": {"semana": ("08:00", "17:00"), "sabado": ("08:30", "13:30")},
    "Yumbel": {"semana": ("08:00", "17:00"), "sabado": ("08:30", "13:30")}
}


# Desactiva la validación del certificado SSL para permitir conexiones 
# HTTPS a servidores con certificados internos o no confiables.
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


# Verifica si la planta se encuentra dentro del horario de atención
def dentro_horario(planta):
    ahora = datetime.now()
    dia = ahora.weekday()  # 0=lunes ... 5=sábado ... 6=domingo

    if dia == 6:
        return False

    tipo = "sabado" if dia == 5 else "semana"
    inicio, fin = HORARIOS[planta][tipo]

    hora_inicio = datetime.strptime(inicio, "%H:%M").time()
    hora_fin = datetime.strptime(fin, "%H:%M").time()

    return hora_inicio <= ahora.time() <= hora_fin


# Función para capturar imágenes de una cámara específica
async def capturar_camara(session, planta, cam_id):
    carpeta = os.path.join(BASE_DIR, planta.replace(" ", "_"))
    os.makedirs(carpeta, exist_ok=True)


    while True:

        if not dentro_horario(planta):
            print(f"{planta} fuera de horario. Captura finalizada.")
            break

        pitime = int(time.time())
        fecha = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        url = f"{BASE_URL}/{cam_id}/imagen.jpg"
        
        # Hace un GET request para obtener la imagen y la guarda en la 
        # carpeta correspondiente
        try:
            async with session.get(url, params={"pitime": pitime}) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    with open(os.path.join(carpeta, f"{fecha}.jpg"), "wb") as f:
                        f.write(data)
                        print(f"{planta} - Imagen guardada: {fecha}.jpg")
                    contador_errores = 0
                else:
                    print(planta, resp.status)
                    contador_errores += 1
        except Exception as e:
            print(f"Sucedió un error al tratar de obtener la imagen de {planta}. Error: {e}")
            
            #En caso de 5 errores consecutivos, se detiene la captura para esa cámara
            if contador_errores == 5:
                print(f"Se han producido {contador_errores} errores consecutivos al intentar obtener la imagen de {planta}.")
                break
            
        print("\n")
        await asyncio.sleep(INTERVALO)


#Crea una sesión HTTP asíncrona compartida con SSL deshabilitado y un pool de conexiones limitado, 
# y ejecuta en paralelo las tareas de captura de imágenes para todas las cámaras.
async def main():
    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=50)
    timeout = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:
        tareas = [
            capturar_camara(session, planta, cam_id)
            for planta, cam_id in camaras.items()
        ]
        await asyncio.gather(*tareas)


#Ejecución
asyncio.run(main())
print("Recompilación finalizada.")
