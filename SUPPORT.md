# FlujoPRT — Despliegue desde cero en AWS

Guia paso a paso para levantar el sistema completo (captura + S3 + dashboard) en una instancia EC2 nueva, incluyendo la configuracion de usuarios IAM y descarga de capturas desde una maquina local.

---

## Requisitos previos

- Cuenta AWS activa con permisos de administrador
- Cliente AWS CLI instalado en tu maquina local (`aws --version`)
- Git instalado localmente
- pip install -r deploy/requirements.cloud.txt

---

## 1. Configuracion IAM

### 1.1 Crear rol para la instancia EC2 (recomendado)

Este es el metodo correcto en produccion: la EC2 asume un rol, sin claves hardcodeadas.

1. AWS Console → **IAM** → **Roles** → **Create role**
2. Tipo de entidad: **AWS service** → **EC2**
3. Adjuntar la siguiente politica inline (o crear una politica gestionada con este JSON):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Full",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::flujo-prt-imagenes",
        "arn:aws:s3:::flujo-prt-imagenes/*"
      ]
    },
    {
      "Sid": "STSVerify",
      "Effect": "Allow",
      "Action": "sts:GetCallerIdentity",
      "Resource": "*"
    }
  ]
}
```

4. Nombre del rol: `FlujoPRT-EC2-Role`
5. **Create role**

### 1.2 Usuario `s3-downloader` — descarga desde maquina local

Este usuario permite descargar capturas desde una PC local sin necesidad de conectarse a la EC2.

**Crear el usuario:**

1. IAM → **Users** → **Create user** → nombre: `s3-downloader`

**Crear la politica:**

2. IAM → **Policies** → **Create policy** → tab **JSON** → pegar el siguiente JSON:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:ListAccessPointsForObjectLambda",
                "s3:GetAccessPoint",
                "s3:PutAccountPublicAccessBlock",
                "s3:ListAccessPoints",
                "s3:CreateStorageLensGroup",
                "s3:ListJobs",
                "s3:PutStorageLensConfiguration",
                "s3:ListMultiRegionAccessPoints",
                "s3:ListStorageLensGroups",
                "s3:ListStorageLensConfigurations",
                "s3:GetAccountPublicAccessBlock",
                "s3:ListAllMyBuckets",
                "s3:ListAccessGrantsInstances",
                "s3:PutAccessPointPublicAccessBlock",
                "s3:CreateJob"
            ],
            "Resource": "*"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": [
                "s3:*",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::flujo-prt-imagenes"
        },
        {
            "Sid": "VisualEditor2",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion"
            ],
            "Resource": "arn:aws:s3:::flujo-prt-imagenes/*"
        }
    ]
}
```

3. Nombre de la politica: `S3-FlujoPRT-ReadOnly` → **Create policy**

**Asignar la politica al usuario:**

4. IAM → **Users** → `s3-downloader` → **Add permissions** → **Attach policies directly** → buscar `S3-FlujoPRT-ReadOnly` → **Add permissions**

**Generar claves de acceso: (Paso necesario si quieres descargar las imagenes a tu computadora)**

5. IAM → **Users** → `s3-downloader` → **Security credentials** → **Create access key** → use case: **Command Line Interface (CLI)**
6. Guardar `Access key ID` y `Secret access key`

**Configurar en la maquina local:**

```bash
aws configure --profile s3-downloader
# AWS Access Key ID:     <tu-access-key>
# AWS Secret Access Key: <tu-secret-key>
# Default region name:   us-east-1
# Default output format: json
```

Verificar acceso:

```bash
aws s3 ls s3://flujo-prt-imagenes --profile s3-downloader
```

Si las credenciales se comprometen: IAM → Users → s3-downloader → Security credentials → desactivar o borrar el Access Key → crear uno nuevo → `aws configure --profile s3-downloader`.

### 1.3 (Alternativa) Usuario IAM con claves para la EC2

Si prefieres credenciales estaticas en lugar de un rol para la instancia:

1. IAM → **Users** → **Create user** → nombre: FlujoPRT_user
2. Adjuntar la misma politica JSON del paso 1.1
3. **Security credentials** → **Create access key** → use case: **Application running on AWS compute service**
4. Guardar `Access key ID` y `Secret access key` — se usaran en el paso 5

---

## 2. Crear el bucket S3

```bash
# Crear el bucket en us-east-1
aws s3api create-bucket \
  --bucket flujo-prt-imagenes \
  --region us-east-1

# Habilitar INTELLIGENT_TIERING (opcional, reduce costos a largo plazo)
aws s3api put-bucket-intelligent-tiering-configuration \
  --bucket flujo-prt-imagenes \
  --id EntiresBucket \
  --intelligent-tiering-configuration '{
    "Id": "EntiresBucket",
    "Status": "Enabled",
    "Tierings": [{"Days": 90, "AccessTier": "ARCHIVE_ACCESS"}]
  }'

# Verificar que el bucket existe
aws s3 ls | grep flujo-prt-imagenes
```

> El bucket y la EC2 deben estar en la **misma region** (`us-east-1`).

---

## 3. Crear la instancia EC2

1. EC2 → **Launch instance**
2. **Name:** `FlujoPRT`
3. **AMI:** Ubuntu Server 22.04 LTS (64-bit x86)
4. **Instance type:** `t2.large` (minimo recomendado; `t3.medium` si hay disponibilidad)
5. **Key pair:** crear o seleccionar un par de claves `.pem`
6. **Security group:** crear nuevo con las siguientes reglas de entrada

| Tipo              | Puerto | Origen             | Descripción                   |
| :---------------- | :----- | :----------------- | :----------------------------- |
| SSH               | 22     | Tu IP (x.x.x.x/32) | Acceso administrativo          |
| TCP personalizado | 8501   | Tu IP (x.x.x.x/32) | Dashboard Streamlit (opcional) |

1. **Advanced details → IAM instance profile:** seleccionar `FlujoPRT-EC2-Role`
2. **Storage:** 20 GB gp3 es suficiente (las imagenes van a S3, no al disco)
3. **Launch instance**



Verificar que la EC2 puede acceder a S3:

```bash
# Conectarse a la EC2
ssh -i ~/.ssh/tu-clave.pem ubuntu@<IP_EC2>

# Dentro de la EC2
aws s3 ls s3://flujo-prt-imagenes
# Debe responder sin error (bucket vacio al inicio)
```

---

## 4. Instalar dependencias en la EC2

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3-pip awscli git tmux

# Verificar versiones
python3 --version   # debe ser 3.9+
pip3 --version
```

> **Nota:** Para el subsistema de reconocimiento vehicular se requiere adicionalmente: `pip install ultralytics tqdm`. El modelo `yolov8m.pt` (~50 MB) se descarga automaticamente la primera vez.

---

## 5. Clonar el repositorio

```bash
git clone https://github.com/DiegoPyLL/FlujoPRT
cd FlujoPRT

# Dependencias necesarias para el proyecto
pip install --user -r deploy/requirements.cloud.txt

# Dependencias del subsistema vehicular (opcional, solo si se usara VehicleRecognition)
pip install --user ultralytics tqdm
```

Si usaste **usuario IAM con claves** (paso 1.3) en lugar de rol, configurar credenciales ahora:

```bash
aws configure
# AWS Access Key ID:     <tu-access-key>
# AWS Secret Access Key: <tu-secret-key>
# Default region name:   us-east-1
# Default output format: json
```

Si usaste **rol de EC2**, este paso no es necesario — boto3 lo detecta automaticamente.

---

## 6. Verificar acceso antes de ejecutar

```bash
# Verificar credenciales AWS
aws sts get-caller-identity

# Verificar acceso al bucket
aws s3 ls s3://flujo-prt-imagenes

# Verificar que el CSV de plantas esta presente
ls data/plantas_revision_tecnica.csv
```

---

## 7. Ejecutar el sistema

El sistema corre en tres sesiones `tmux` independientes.

### Sesion 1 — Captura (proceso principal)

```bash
tmux new -s FlujoPRT_CCTV
cd ~/FlujoPRT
python3 src/imageRecopilator/Cloud/ImageRecompilerCloud.py
# Desconectarse sin matar el proceso: Ctrl+B, luego D
```

Al arrancar, los logs deben mostrar:

- `Credenciales AWS verificadas`
- `=== INICIO INGESTA CATALOGO PLANTAS ===`
- `Catalogo subido: 116 registros`
- `[HCH] Iniciando captura...` (y demas plantas)

### Sesion 2 — Dashboard Streamlit (opcional)

```bash
tmux new -s FlujoPRT_Dashboard
cd ~/FlujoPRT
streamlit run scripts/realtime_dashboard.py --server.port 8501 --server.address 0.0.0.0
# Desconectarse: Ctrl+B, luego D
```

Acceso directo (si el puerto 8501 esta abierto en el Security Group):

```
http://<IP_EC2>:8501
```

Acceso via SSH tunnel (recomendado, no requiere exponer el puerto):

```bash
ssh -L 8501:localhost:8501 -i ~/.ssh/tu-clave.pem ubuntu@<IP_EC2>
# Luego abrir: http://localhost:8501
```

### Sesion 3 — Reconocimiento vehicular (opcional)

Procesa el historico de imagenes S3 detectando vehiculos con YOLOv8. Solo es necesario correrlo una vez para poner al dia el backlog; luego se puede ejecutar periodicamente para las capturas nuevas.

```bash
tmux new -s FlujoPRT_VehicleDetection
cd ~/FlujoPRT

# Primera ejecucion: analisis incremental (omite lo ya procesado)
python3 scripts/VehicleRecognition/analisis_historico.py

# Rango especifico de fechas
python3 scripts/VehicleRecognition/analisis_historico.py \
    --fecha-inicio 2026-04-01 --fecha-fin 2026-04-30

# Validador con bbox (genera imagenes anotadas y JSONL)
python3 scripts/VehicleRecognition/validate_vehiculos.py \
    --prefijo capturas/2026/04/

# Desconectarse: Ctrl+B, luego D
tmux attach -t FlujoPRT_VehicleDetection
```

---

## 8. Verificar que el sistema esta capturando

```bash
# Fotos subidas hoy
aws s3 ls s3://flujo-prt-imagenes/capturas/$(date +%Y/%m/%d)/

# Metadata generada hoy
aws s3 ls s3://flujo-prt-imagenes/metadata/capturas/$(date +%Y/%m/%d)/

# Detecciones vehiculares generadas hoy
aws s3 ls s3://flujo-prt-imagenes/metadata/detecciones/$(date +%Y/%m/%d)/

# Imagenes con bounding boxes
aws s3 ls s3://flujo-prt-imagenes/capturas_anotadas/$(date +%Y/%m/%d)/

# Tamanio total del bucket
aws s3 ls s3://flujo-prt-imagenes/capturas/ --recursive --human-readable --summarize
```

---

## 9. Logs y diagnostico

### Ver logs en tiempo real

```bash
# Si el proceso corre con run.sh (background)
tail -f ~/captura.log

# Si corre dentro de tmux, los logs aparecen directamente en la sesion
tmux attach -t FlujoPRT_CCTV
```

### Buscar errores en el historial

```bash
# Errores criticos
grep "CRITICAL" ~/captura.log | tail -n 20

# Errores de una camara especifica (ejemplo: Temuco)
grep "TMU" ~/captura.log | grep -i "error\|warning" | tail -n 20

# Ver las ultimas 100 lineas del log
tail -n 100 ~/captura.log
```

### Verificar conectividad con una camara

```bash
# Probar que la URL de una camara responde (sin descargar la imagen)
curl -I https://pti-cameras.cl.tuv.com/camaras/10.57.32.222_Cam01/imagen.jpg
# Esperado: HTTP/1.1 200 OK
```

### Estado del servidor

```bash
# Espacio en disco de la EC2
df -h

# Memoria y CPU en uso
free -h
top -bn1 | head -20
```

### Limpiar cache de Python

```bash
# Eliminar carpetas __pycache__ acumuladas
find . -type d -name "__pycache__" -exec rm -rf {} +
```

---

## 10. Descargar capturas desde maquina local

Usar el perfil `s3-downloader` configurado en el paso 1.2.

```bash
# Verificar credenciales del perfil
aws configure list --profile s3-downloader

# Sincronizar todas las capturas
aws s3 sync s3://flujo-prt-imagenes/capturas/ ./capturas/ \
  --profile s3-downloader

# Solo archivos nuevos (mas rapido en sincronizaciones frecuentes)
aws s3 sync s3://flujo-prt-imagenes/capturas/ ./capturas/ \
  --profile s3-downloader --size-only

# Un dia especifico (ejemplo: 22-ene-2026)
aws s3 sync s3://flujo-prt-imagenes/capturas/2026/01/22/ ./capturas/2026/01/22/ \
  --profile s3-downloader

# Una planta especifica
aws s3 sync s3://flujo-prt-imagenes/capturas/2026/01/22/Temuco/ ./capturas/temuco/ \
  --profile s3-downloader

# Solo metadata
aws s3 sync s3://flujo-prt-imagenes/metadata/ ./metadata/ \
  --profile s3-downloader

# Con timestamps exactos y sin barra de progreso (util para scripts)
aws s3 sync s3://flujo-prt-imagenes/capturas/ ./capturas/ \
  --profile s3-downloader --exact-timestamps --no-progress
```

---

## 11. Administracion de sesiones tmux

```bash
# Ver sesiones activas
tmux ls

# Volver a conectarse
tmux attach -t FlujoPRT_CCTV
tmux attach -t FlujoPRT_Dashboard
tmux attach -t FlujoPRT_VehicleDetection

# Matar sesiones (para reiniciar procesos)
tmux kill-session -t FlujoPRT_CCTV
tmux kill-session -t FlujoPRT_Dashboard
tmux kill-session -t FlujoPRT_VehicleDetection

# Reiniciar captura
tmux new -s FlujoPRT_CCTV
python3 src/imageRecopilator/Cloud/ImageRecompilerCloud.py
```

Alternativa con el script de servicio:

```bash
chmod +x deploy/run.sh
./deploy/run.sh start    # inicia en background
./deploy/run.sh status   # ver si corre
./deploy/run.sh logs     # ver log en tiempo real
./deploy/run.sh stop     # detener
./deploy/run.sh restart  # reiniciar
```

---

## 12. Variables de entorno (todas opcionales)

Exportar antes de ejecutar o agregar al inicio de la sesion tmux:

```bash
export S3_BUCKET=flujo-prt-imagenes   # bucket destino
export INTERVALO=60                    # segundos entre capturas
export JPEG_QUALITY=80                 # calidad de compresion (0-100)
export MAX_DESCARGAS=10                # descargas HTTP simultaneas
export NUM_UPLOADERS=2                 # workers paralelos de subida S3
export TZ=America/Santiago             # zona horaria del sistema
export METRICAS_INTERVALO=300          # segundos entre reportes de metricas
```

---

## Resolucion de problemas

**`NoCredentialError` al iniciar**

- Con rol EC2: verificar que `FlujoPRT-EC2-Role` esta asociado en EC2 → Actions → Security → Modify IAM role.
- Con claves: ejecutar `aws configure` y verificar con `aws sts get-caller-identity`.

**`NoSuchBucket`**

- El bucket se llama exactamente `flujo-prt-imagenes` (con guiones, no `flujoprtimagenes`).
- Bucket y EC2 deben estar en la misma region (`us-east-1`).

**`AccessDenied` al subir a S3**

- Verificar que la politica IAM incluye `s3:PutObject` sobre `arn:aws:s3:::flujo-prt-imagenes/*`.

**`InvalidAccessKeyId` al descargar**

- Ejecutar `aws configure list --profile s3-downloader` para verificar las claves.
- Si estan mal: `aws configure --profile s3-downloader` y reingresar las credenciales.
- Esperar 1-2 minutos tras crear o rotar claves para que se propaguen.

**`FileNotFoundError: data/plantas_revision_tecnica.csv`**

- Ejecutar siempre desde la raiz del proyecto: `cd ~/FlujoPRT` antes de `python3 src/...`.

**Camaras sin respuesta (timeout)**

- Las camaras (`10.57.x.x`) son red privada TUV Rheinland. Solo accesibles desde la red interna o VPN TUV. Fuera de esa red los timeouts son esperados.
