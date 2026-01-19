import asyncio
import aiohttp
import time
from datetime import datetime, timedelta
import os
import ssl


"""

FUNCIONAMIENTO GENERAL:
-----------------------
1. Loop principal infinito que nunca termina
2. Antes de cada ciclo verifica si hay plantas operando:
   - Si es domingo → suspende hasta el lunes (20 min antes de apertura)
   - Si todas están fuera de horario → suspende hasta próxima apertura
   - Si al menos una opera → lanza las tareas de captura
3. Ejecuta 14 tareas asíncronas en paralelo (una por planta)
4. Cada tarea captura imágenes cada 60 segundos cuando está en horario

TEMPORIZADORES:
---------------
- COMPARTIDO: La suspensión inicial aplica a TODAS las plantas juntas
- INDEPENDIENTES: Cada planta tiene su ciclo propio de 60 segundos una vez activa

ITERACIÓN:
----------
Las 14 plantas se procesan EN PARALELO usando asyncio.gather().
No es secuencial - todas operan simultáneamente.
"""





# URL base del servidor de cámaras
BASE_URL = "https://pti-cameras.cl.tuv.com/camaras"

# Directorio donde se guardarán las imágenes capturadas
BASE_DIR = r"C:/Users/Laptop/Desktop/Trabajos/ProyectosPersonales/FlujoPRT_Main/RecompilacionFotos"

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

    # Domingo: no abre
    if dia == 6:
        return None

    tipo = "sabado" if dia == 5 else "semana"
    inicio, _ = HORARIOS[planta][tipo]

    hora_inicio = datetime.strptime(inicio, "%H:%M").time()
    apertura = datetime.combine(ahora.date(), hora_inicio)

    if ahora.time() < hora_inicio:
        return int((apertura - ahora).total_seconds())
    else:
        # Ya pasó la hora de hoy, calcula para mañana
        dia_siguiente = (dia + 1) % 7
        
        if dia_siguiente == 6:  # Mañana es domingo
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
            # Calcula tiempo hasta el lunes menos 20 minutos
            ahora = datetime.now()
            lunes = ahora + timedelta(days=1)
            while lunes.weekday() != 0:
                lunes += timedelta(days=1)
            
            # Primera apertura del lunes
            primer_hora = min(HORARIOS[p]["semana"][0] for p in camaras.keys())
            hora_apertura = datetime.strptime(primer_hora, "%H:%M").time()
            apertura_lunes = datetime.combine(lunes.date(), hora_apertura)
            
            # Restar 20 minutos
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
                # Restar 20 minutos al tiempo de espera
                espera_real = max(0, menor - MARGEN_PREVIO)
                minutos = int(espera_real / 60)
                print(f"Todas fuera de horario. Suspendiendo {minutos} min (hasta 20 min antes de apertura)...")
                await asyncio.sleep(espera_real)
            else:
                # Caso extremo: espera 1 hora y vuelve a verificar
                print("Sin aperturas inmediatas. Verificando en 1 hora...")
                await asyncio.sleep(3600)
        else:
            break


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





# Función principal que coordina todas las capturas
async def main():
    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=50)
    timeout = aiohttp.ClientTimeout(total=20)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        while True:
            await esperar_hasta_apertura()
            
            await asyncio.gather(
                *[capturar_camara(session, planta, cam_id) for planta, cam_id in camaras.items()]
            )




# Inicia la ejecución del programa
if __name__ == "__main__":
    asyncio.run(main())