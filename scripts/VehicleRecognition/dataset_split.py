import os
from shutil import copy, rmtree
import random

VERSION = "1.0.0"

img_dir = 'data'


def mk_file(file_path: str):
    if os.path.exists(file_path):
        # Si la carpeta existe, se elimina y se vuelve a crear
        rmtree(file_path)
    os.makedirs(file_path)


def main():
    print("dataset-split v{}".format(VERSION))
    # Fijar semilla para reproducibilidad
    random.seed(0)

    # Porcentaje del dataset asignado a validacion
    split_rate = 0.1

    # Ruta al directorio raiz del dataset descomprimido
    data_root = '/media/新加卷/HyperVID-Dataset'  # ruta del dataset HyperVID
    origin_car_path = os.path.join(data_root, img_dir)
    assert os.path.exists(origin_car_path), "La ruta '{}' no existe.".format(origin_car_path)

    flower_class = [cla for cla in os.listdir(origin_car_path)
                    if os.path.isdir(os.path.join(origin_car_path, cla))]

    # Crear carpeta para el conjunto de entrenamiento
    train_root = os.path.join(data_root, "train")
    mk_file(train_root)
    for cla in flower_class:
        # Crear subcarpeta por cada clase
        mk_file(os.path.join(train_root, cla))

    # Crear carpeta para el conjunto de validacion
    val_root = os.path.join(data_root, "val")
    mk_file(val_root)
    for cla in flower_class:
        # Crear subcarpeta por cada clase
        mk_file(os.path.join(val_root, cla))

    for cla in flower_class:
        cla_path = os.path.join(origin_car_path, cla)
        images = os.listdir(cla_path)
        num = len(images)
        # Muestreo aleatorio de indices para el conjunto de validacion
        eval_index = random.sample(images, k=int(num * split_rate))
        for index, image in enumerate(images):
            if image in eval_index:
                # Copiar al directorio de validacion
                image_path = os.path.join(cla_path, image)
                new_path = os.path.join(val_root, cla)
                copy(image_path, new_path)
            else:
                # Copiar al directorio de entrenamiento
                image_path = os.path.join(cla_path, image)
                new_path = os.path.join(train_root, cla)
                copy(image_path, new_path)
            print("\r[{}] procesando [{}/{}]".format(cla, index + 1, num), end="")  # barra de progreso
        print()

    print("Procesamiento completado!")


if __name__ == '__main__':
    main()
