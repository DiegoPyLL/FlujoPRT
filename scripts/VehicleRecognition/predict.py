"""
Script de prediccion de tipo de vehiculo

Usa el modelo ResNet entrenado para clasificar vehiculos en imagenes
"""

import os
import json
import time
import numpy as np
import torch
from torchvision import transforms
import cv2 as cv

os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'
from scripts.VehicleRecognition.model import resnet34
from PIL import Image, ImageDraw, ImageFont

data_transform = transforms.Compose([
    transforms.ToTensor(),
    transforms.Resize(256),
    transforms.CenterCrop(224),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
])


def cvImgAddText(img, text, left, top, textColor=(0, 255, 0), textSize=20):
    """
    Agrega texto a una imagen OpenCV

    Args:
        img: Objeto de imagen (formato OpenCV o PIL)
        text (str): Contenido del texto a agregar
        left (int): Distancia del borde izquierdo del texto
        top (int): Distancia del borde superior del texto
        textColor (tuple): Color del texto (B, G, R), por defecto (0, 255, 0)
        textSize (int): Tamanio de la fuente, por defecto 20

    Returns:
        np.ndarray: Imagen con texto agregado (formato OpenCV)
    """
    if isinstance(img, np.ndarray):
        img = Image.fromarray(cv.cvtColor(img, cv.COLOR_BGR2RGB))
    draw = ImageDraw.Draw(img)
    fontText = ImageFont.truetype("NotoSansCJK-Black.ttc",
                                  textSize,
                                  encoding="utf-8")
    draw.text((left, top), text, textColor, font=fontText)
    return cv.cvtColor(np.asarray(img), cv.COLOR_RGB2BGR)


def convert_to_input(image_path):
    """
    Convierte la ruta de una imagen a un tensor de entrada para el modelo

    Args:
        image_path (str): Ruta del archivo de imagen

    Returns:
        Tensor: Tensor de entrada para el modelo [1, C, H, W]
    """
    assert os.path.exists(image_path), "El archivo '{}' no existe.".format(
        image_path)
    img = cv.imdecode(np.fromfile(image_path, dtype=np.uint8), 1)
    img = data_transform(img)
    img = torch.unsqueeze(img, dim=0)
    return img


def load_class_mapping(json_path):
    """
    Carga el mapeo de indices de clases

    Args:
        json_path (str): Ruta del archivo JSON

    Returns:
        dict: Diccionario de mapeo de indices de clases
    """
    assert os.path.exists(json_path), "El archivo '{}' no existe.".format(
        json_path)
    with open(json_path, "r", encoding='utf-8') as json_file:
        class_indict = json.load(json_file)
    return class_indict


def load_model(device, num_classes=1778, weights_path="resnet_car.pth"):
    """
    Carga el modelo y sus pesos

    Args:
        device: Objeto de dispositivo PyTorch
        num_classes (int): Numero de clases de clasificacion, por defecto 1778
        weights_path (str): Ruta de los pesos del modelo, por defecto "resnet_car.pth"

    Returns:
        ResNet: Modelo con pesos cargados
    """
    assert os.path.exists(weights_path), "El archivo '{}' no existe.".format(
        weights_path)
    model = resnet34(num_classes=num_classes).to(device)
    model.load_state_dict(torch.load(weights_path, map_location=device))
    return model


def predict_image(model, image_path, class_indict, device):
    """
    Realiza la prediccion de una imagen individual y retorna los resultados

    Args:
        model: Objeto del modelo
        image_path (str): Ruta de la imagen
        class_indict (dict): Mapeo de indices de clases
        device: Objeto de dispositivo PyTorch

    Returns:
        tuple: (tiempo_inferencia, imagen_display, resultado_prediccion, lista_top5)
    """
    img = convert_to_input(image_path)
    imgshow = cv.imdecode(np.fromfile(image_path, dtype=np.uint8), 1)

    model.eval()
    start_time = time.time()

    with torch.no_grad():
        output = torch.squeeze(model(img.to(device))).cpu()
        predict = torch.softmax(output, dim=0)
        predict_cla = torch.argmax(predict).numpy()
        predict_max5 = torch.topk(predict, 5)

    inference_time = time.time() - start_time
    print_res = "clase: {}   prob: {:.3}".format(
        class_indict[str(predict_cla)], predict[predict_cla].numpy())

    top5_results = []
    for idx, ii in enumerate(predict_max5[1]):
        class_name = class_indict[str(ii.numpy())]
        probability = predict[ii].numpy()
        top5_results.append((class_name, probability))
        print(f"{class_name}: {probability:.4f}")

        imgshow = cvImgAddText(imgshow, f"{class_name}  Puntaje:{probability:.4f}",
                               10, (idx + 1) * 25, (255, 255, 0), 20)

    return inference_time, imgshow, print_res, top5_results


def main():
    """
    Funcion principal de prediccion

    Ejecuta la prediccion de tipo de vehiculo, con soporte para prediccion
    en lote y visualizacion de resultados.
    """
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

    class_indict = load_class_mapping('class_car.json')
    model = load_model(device, num_classes=1778, weights_path="resnet_car.pth")

    image_list = [
        './car_img/car_1.png', './car_img/car_2.png', './car_img/car_3.png'
    ]
    inference_times = []

    for image_path in image_list:
        print(f"\nPredecir imagen: {image_path}")
        inference_time, imgshow, print_res, _ = predict_image(
            model, image_path, class_indict, device)

        print(print_res)
        print(f'Tiempo de prediccion: {inference_time:.4f} segundos')

        cv.imshow('window', imgshow)
        cv.waitKey(5000)
        inference_times.append(inference_time)

    print(f'\nTiempo promedio de prediccion: {np.mean(inference_times):.4f} segundos')


if __name__ == '__main__':
    main()
