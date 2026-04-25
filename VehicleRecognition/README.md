# Reconocimiento de Tipo de Vehiculo

Sistema de reconocimiento de tipo de vehiculo basado en aprendizaje profundo, que usa la red ResNet34 para clasificar con precision mas de 1777 modelos de vehiculos.

[![PyTorch](https://img.shields.io/badge/PyTorch-1.x-red)](https://pytorch.org/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.7+-green)](https://www.python.org/)

## Demostracion

![demo1](https://github.com/erquren/vehicle_recognition/blob/main/car_img/demo1.png?raw=true)
![demo2](https://github.com/erquren/vehicle_recognition/blob/main/car_img/demo2.png?raw=true)
![demo3](https://github.com/erquren/vehicle_recognition/blob/main/car_img/demo3.png?raw=true)

## Descripcion del Proyecto

Este proyecto usa el framework PyTorch para construir un modelo de clasificacion de tipos de vehiculos basado en la red neuronal profunda ResNet34, con soporte para reconocer mas de 1777 categorias de vehiculos. El modelo aplica aprendizaje por transferencia, ajustando finamente los pesos preentrenados de ImageNet para lograr un reconocimiento de alta precision.

### Caracteristicas Principales

- 🚗 **Soporta 1777+ modelos**: Cubre las principales marcas y modelos de automoviles
- 🎯 **Alta precision**: Basado en la arquitectura de red ResNet34
- ⚡ **Prediccion rapida**: Tiempo de prediccion por imagen < 0.1 segundos (GPU)
- 📦 **Listo para usar**: Incluye modelo preentrenado, puede usarse directamente
- 🔧 **Facil de extender**: Estructura de codigo clara, soporta entrenamiento personalizado

## Dataset

Este proyecto usa el dataset **HyperVID** para el entrenamiento, que contiene una gran cantidad de imagenes de vehiculos de multiples tipos.

### Descarga del Dataset

Enlace de descarga Baidu Netdisk:
- Enlace: https://pan.baidu.com/s/1vvV2H5Jpewgba_VFsWvDcA
- Contrasena: vuo4

## Inicio Rapido

### Requisitos del Entorno

- Python 3.7+
- PyTorch 1.x
- CUDA (opcional, para aceleracion GPU)

### Instalacion de Dependencias

```bash
pip install -r requirements.txt
```

### Prediccion Rapida

Usar el modelo preentrenado para predecir:

```bash
python predict.py
```

Coloca las imagenes a predecir en el directorio `car_img/` y modifica `image_list` en `predict.py` para especificar las rutas de tus imagenes.

## Entrenamiento del Modelo

### Preparacion de Datos

1. Descarga y descomprime el dataset
2. Ejecuta el script de division del dataset:

```bash
python dataset_split.py
```

### Iniciar Entrenamiento

```bash
python train.py
```

Los parametros de entrenamiento se pueden ajustar en `train.py`, como:
- `epochs`: Numero de epocas (por defecto 500)
- `batch_size`: Tamanio del lote (por defecto 32)
- `lr`: Tasa de aprendizaje (por defecto 0.0001)

Al finalizar el entrenamiento, los pesos del modelo se guardan como `resnet34.pth`.

## Estructura del Proyecto

```
vehicle_recognition/
├── car_img/                 # Directorio de imagenes a predecir
├── model.py                 # Definicion del modelo ResNet
├── train.py                 # Script de entrenamiento
├── predict.py               # Script de prediccion
├── dataset_split.py         # Script de division del dataset
├── resnet_car.pth          # Pesos del modelo preentrenado
├── class_car.json          # Mapeo de etiquetas de clases
└── requirements.txt         # Dependencias del proyecto
```

## Arquitectura del Modelo

Este proyecto usa ResNet34 como red base, con los siguientes componentes principales:

- **BasicBlock**: Bloque residual basico de ResNet
- **Bottleneck**: Bloque cuello de botella (para redes mas profundas)
- **ResNet**: Estructura completa de la red, soporta multiples variantes (ResNet34/50/101, etc.)

### Pesos Preentrenados

Descarga de pesos preentrenados de ResNet34:
https://download.pytorch.org/models/resnet34-333f7ec4.pth

## Ejemplos de Uso

### Prediccion de Una Imagen

```python
from model import resnet34
from predict import load_model, predict_image, load_class_mapping

device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
class_indict = load_class_mapping('class_car.json')
model = load_model(device, num_classes=1778)

# Predecir imagen
time_used, imgshow, result, top5 = predict_image(
    model, 'path/to/image.jpg', class_indict, device
)
print(f"Resultado: {result}")
print(f"Top-5: {top5}")
```

### Entrenamiento Personalizado

```python
from train import main, get_data_transform, build_model

# Modificar ruta de datos y parametros
image_path = '/path/to/dataset'
data_transform = get_data_transform()
# ... mas configuracion personalizada
```

## Indicadores de Rendimiento

| Indicador | Valor |
|-----------|-------|
| Modelos soportados | 1777+ |
| Tamanio del modelo | ~85 MB |
| Velocidad de prediccion (CPU) | ~0.3s/imagen |
| Velocidad de prediccion (GPU) | <0.1s/imagen |
| Precision de entrenamiento | >95% |

## Preguntas Frecuentes

**P: Como agregar nuevas categorias de vehiculos?**

R: Agrega las imagenes del nuevo tipo de vehiculo al directorio de entrenamiento correspondiente y vuelve a entrenar el modelo.

**P: Como usar mi propio dataset?**

R: Organiza los datos en formato ImageFolder (una carpeta por clase) y modifica `image_path` en `train.py`.

**P: Que hacer si la VRAM es insuficiente durante el entrenamiento?**

R: Reduce `batch_size` o usa una red mas pequena (como ResNet18).

## Pendientes

- [ ] Ampliar la cantidad de datos de entrenamiento
- [ ] Soportar mas categorias de vehiculos
- [ ] Optimizar la velocidad de inferencia del modelo
- [ ] Proveer interfaz Web API
- [ ] Soportar prediccion en lote

## Licencia

Este proyecto usa la licencia MIT - ver el archivo [LICENSE](LICENSE) para mas detalles

## Agradecimientos

- Proveedor del dataset HyperVID
- Framework PyTorch y documentacion oficial

## Contacto

Para preguntas o sugerencias, abrir un Issue o Pull Request.
