# POLÍTICA IAM PARA DESCARGAR BUCKET S3

## ✅ CONFIGURACIÓN COMPLETADA

**Usuario IAM creado:** `s3-downloader`

---

## 📋 Política IAM Necesaria

Esta es la política que debes asignar al usuario `s3-downloader`:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ListBucket",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::flujo-prt-imagenes"
        },
        {
            "Sid": "DownloadObjects",
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

---

## 🔧 Cómo Asignar la Política

### Método 1: Crear y Asignar Política (Recomendado)

1. **Crear la política:**

   - AWS Console → IAM → Policies
   - Click **"Create policy"**
   - Tab **"JSON"**
   - Pega el JSON de arriba
   - Click **"Next"**
   - Nombre: `S3-FlujoPRT-ReadOnly`
   - Click **"Create policy"**
2. **Asignar al usuario:**

   - IAM → Users → `s3-downloader`
   - Tab **"Permissions"**
   - Click **"Add permissions"** → **"Attach policies directly"**
   - Busca `S3-FlujoPRT-ReadOnly`
   - Selecciona y click **"Add permissions"**

### Método 2: Inline Policy (Rápido)

1. IAM → Users → `s3-downloader`
2. Tab **"Permissions"**
3. Click **"Add permissions"** → **"Create inline policy"**
4. Tab **"JSON"**
5. Pega el JSON de arriba
6. Nombre: `S3FlujoPRTAccess`
7. Click **"Create policy"**

---

## 🔐 Credenciales Configuradas en tu PC

```cmd
aws configure list
```

---

## 🚀 Comandos para Descargar

### Verificar Acceso

```cmd
aws s3 ls s3://flujo-prt-imagenes/capturas/
```

**Resultado esperado:**

```
                           PRE 2026/
```

### Descargar TODO el Bucket

**Opción 1: AWS CLI (Recomendado - Más rápido)**

```cmd
aws s3 sync s3://flujo-prt-imagenes/capturas/ "D:\Trabajos\Proyectos Personales\FlujoPRT_main\capturas_descargadas\"
```

**Opciones útiles:**

```cmd
# Solo archivos nuevos/modificados
aws s3 sync s3://flujo-prt-imagenes/capturas/ "D:\Trabajos\Proyectos Personales\FlujoPRT_main\capturas_descargadas\" --size-only

# Con progreso detallado
aws s3 sync s3://flujo-prt-imagenes/capturas/ "D:\Trabajos\Proyectos Personales\FlujoPRT_main\capturas_descargadas\" --progress

# Solo una fecha específica
aws s3 sync s3://flujo-prt-imagenes/capturas/2026/02/07/ "D:\Trabajos\Proyectos Personales\FlujoPRT_main\capturas_descargadas\2026\02\07\"
```

**Opción 2: Script Python**

```cmd
cd "D:\Trabajos\Proyectos Personales\FlujoPRT_main"
python download_s3_bucket.py
```

---

## ⚠️ Troubleshooting

### Error: "InvalidAccessKeyId"

```cmd
# Verifica las credenciales
aws configure list

# Reconfigura si es necesario
aws configure
```

### Error: "AccessDenied"

- ✅ Verifica que asignaste la política al usuario
- ✅ Espera 1-2 minutos para que los permisos se propaguen
- ✅ Verifica el nombre del bucket: `flujo-prt-imagenes` (CON guiones)

### Error: "NoSuchBucket"

- ❌ Nombre incorrecto: `flujoprtimagenes` (sin guiones)
- ✅ Nombre correcto: `flujo-prt-imagenes` (con guiones)

### Ver permisos actuales del usuario

```cmd
aws iam list-attached-user-policies --user-name s3-downloader
```

---

## 📊 Monitorear Descarga

```cmd
# Ver tamaño del bucket
aws s3 ls s3://flujo-prt-imagenes/capturas/ --recursive --human-readable --summarize

# Contar archivos (PowerShell)
(aws s3 ls s3://flujo-prt-imagenes/capturas/ --recursive).Count
```

---

## 🔒 Seguridad

**✅ Buenas prácticas:**

- Usuario `s3-downloader` solo tiene permisos de lectura (GetObject, ListBucket)
- No puede escribir, borrar ni modificar archivos
- Credenciales guardadas localmente en `~/.aws/credentials` (protegido por Windows)

Si las credenciales se comprometen:

1. IAM → Users → s3-downloader → Security credentials
2. Desactiva o borra el Access Key comprometido
3. Crea uno nuevo
4. Ejecuta `aws configure` con las nuevas credenciales

---

## 📍 Ubicación de Descarga

```
D:\Trabajos\Proyectos Personales\FlujoPRT_main\capturas_descargadas\
```

Estructura esperada:

```
capturas_descargadas/
├── capturas/
│   └── 2026/
│       ├── 02/
│       │   ├── 01/
│       │   │   ├── Chillan/
│       │   │   │   ├── CHL_20260201_080000.jpg
│       │   │   │   └── ...
│       │   │   └── Concepcion/
│       │   │       ├── CCP_20260201_080000.jpg
│       │   │       └── ...
│       │   ├── 02/
│       │   ├── 03/
│       │   └── ...
```
