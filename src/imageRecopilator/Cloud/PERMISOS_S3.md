# POLÍTICA IAM PARA DESCARGAR BUCKET S3

## Opción 1: Política IAM para Usuario/Rol (Recomendado)

Crea esta política en AWS IAM Console:

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
            "Resource": "arn:aws:s3:::flujoprtimagenes"
        },
        {
            "Sid": "DownloadObjects",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion"
            ],
            "Resource": "arn:aws:s3:::flujoprtimagenes/*"
        }
    ]
}
```

**Pasos para aplicar:**

1. Ve a AWS Console → IAM
2. Crea una nueva política:
   - Policies → Create policy
   - Pega el JSON de arriba
   - Nombre: `S3-FlujoPRT-ReadOnly`

3. Asigna la política a tu usuario/rol:
   - Users → Tu usuario → Add permissions → Attach policies
   - Busca y selecciona `S3-FlujoPRT-ReadOnly`

## Opción 2: AWS CLI para configurar credenciales locales

Si descargarás desde tu máquina local:

```bash
# Instalar AWS CLI
pip install awscli

# Configurar credenciales
aws configure

# Te pedirá:
# AWS Access Key ID: [tu-access-key]
# AWS Secret Access Key: [tu-secret-key]
# Default region name: us-east-1
# Default output format: json
```

## Opción 3: Crear usuario IAM nuevo solo para descarga

1. IAM → Users → Add user
   - Nombre: `s3-downloader`
   - Access type: Programmatic access

2. Attach la política creada arriba

3. Guarda las credenciales:
   - Access Key ID
   - Secret Access Key

4. Úsalas en tu máquina local:
   ```bash
   export AWS_ACCESS_KEY_ID=tu-access-key
   export AWS_SECRET_ACCESS_KEY=tu-secret-access-key
   ```

## Verificar permisos

Prueba que tienes acceso:

```bash
# Listar bucket
aws s3 ls s3://flujoprtimagenes/capturas/

# Descargar un archivo de prueba
aws s3 cp s3://flujoprtimagenes/capturas/2026/02/01/test.jpg ./test.jpg
```

## Descargar todo el bucket

### Método 1: AWS CLI (Más rápido)

```bash
# Descargar todo el bucket
aws s3 sync s3://flujoprtimagenes/capturas/ ./capturas_descargadas/

# Solo descargar nuevos/modificados
aws s3 sync s3://flujoprtimagenes/capturas/ ./capturas_descargadas/ --size-only

# Descargar solo de una fecha específica
aws s3 sync s3://flujoprtimagenes/capturas/2026/02/ ./capturas_descargadas/2026_02/
```

### Método 2: Script Python (Incluido)

```bash
# Usar el script download_s3_bucket.py
python download_s3_bucket.py

# O solo listar sin descargar
python download_s3_bucket.py list
```

## Troubleshooting

### Error: "Unable to locate credentials"
```bash
# Verifica que las credenciales están configuradas
aws configure list

# O usa variables de entorno
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

### Error: "Access Denied"
- Verifica que la política IAM está correctamente asignada
- Verifica que el bucket name es correcto
- Verifica que tu usuario tiene la política attachada

### Bucket policy (Alternativa)
Si prefieres hacer el bucket público (NO RECOMENDADO para datos sensibles):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PublicReadGetObject",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::flujoprtimagenes/*"
        }
    ]
}
```

Aplica en: S3 Console → Bucket → Permissions → Bucket Policy
