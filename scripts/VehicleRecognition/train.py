"""
Script de entrenamiento del modelo de reconocimiento de tipo de vehiculo

Usa la red ResNet34 para clasificacion de tipos de vehiculos, con soporte para
carga de pesos preentrenados y guardado del modelo.
"""

import os
import json

import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import transforms, datasets
from tqdm import tqdm

from scripts.VehicleRecognition.model import resnet34


def get_data_transform():
    """
    Obtiene las transformaciones de preprocesamiento para entrenamiento y validacion

    Returns:
        dict: Diccionario con las transformaciones para entrenamiento y validacion
    """
    return {
        "train":
        transforms.Compose([
            transforms.RandomResizedCrop(224),
            transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
        ]),
        "val":
        transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
        ])
    }


def save_class_mapping(dataset, save_path='class_index.json'):
    """
    Guarda el mapeo de indices de clases en un archivo JSON

    Args:
        dataset: Objeto de dataset ImageFolder
        save_path (str): Ruta de guardado del JSON, por defecto 'class_index.json'
    """
    car_list = dataset.class_to_idx
    cla_dict = dict((val, key) for key, val in car_list.items())
    json_str = json.dumps(cla_dict, indent=4)
    with open(save_path, 'w') as json_file:
        json_file.write(json_str)


def create_dataloaders(image_path,
                       data_transform,
                       batch_size=32,
                       num_workers=8):
    """
    Crea los cargadores de datos para entrenamiento y validacion

    Args:
        image_path (str): Ruta raiz del dataset
        data_transform (dict): Diccionario de transformaciones de preprocesamiento
        batch_size (int): Tamanio del lote, por defecto 32
        num_workers (int): Numero de hilos de carga de datos, por defecto 8

    Returns:
        tuple: (train_loader, validate_loader, train_num, val_num)
    """
    train_dataset = datasets.ImageFolder(root=os.path.join(
        image_path, "train"),
                                         transform=data_transform["train"])
    train_num = len(train_dataset)
    save_class_mapping(train_dataset)

    validate_dataset = datasets.ImageFolder(root=os.path.join(
        image_path, "val"),
                                            transform=data_transform["val"])
    val_num = len(validate_dataset)

    nw = min(
        [os.cpu_count(), batch_size if batch_size > 1 else 0, num_workers])
    print('Usando {} workers por proceso'.format(nw))

    train_loader = torch.utils.data.DataLoader(train_dataset,
                                               batch_size=batch_size,
                                               shuffle=True,
                                               num_workers=nw)

    validate_loader = torch.utils.data.DataLoader(validate_dataset,
                                                  batch_size=batch_size,
                                                  shuffle=False,
                                                  num_workers=nw)

    print("Usando {} imagenes para entrenamiento, {} imagenes para validacion.".format(
        train_num, val_num))

    return train_loader, validate_loader, train_num, val_num


def build_model(device,
                num_classes=1777,
                pretrained_path="./resnet34-333f7ec4.pth"):
    """
    Construye e inicializa el modelo

    Args:
        device: Objeto de dispositivo PyTorch
        num_classes (int): Numero de clases de clasificacion, por defecto 1777
        pretrained_path (str): Ruta de los pesos preentrenados, por defecto "./resnet34-333f7ec4.pth"

    Returns:
        ResNet: Modelo inicializado
    """
    net = resnet34()
    assert os.path.exists(pretrained_path), "El archivo {} no existe.".format(
        pretrained_path)
    net.load_state_dict(torch.load(pretrained_path, map_location=device))

    in_channel = net.fc.in_features
    net.fc = nn.Linear(in_channel, num_classes)
    net.to(device)

    return net


def train_one_epoch(net, train_loader, loss_function, optimizer, device, epoch,
                    epochs):
    """
    Entrena una epoca

    Args:
        net: Objeto del modelo
        train_loader: Cargador de datos de entrenamiento
        loss_function: Funcion de perdida
        optimizer: Optimizador
        device: Objeto de dispositivo PyTorch
        epoch (int): Numero de epoca actual
        epochs (int): Total de epocas

    Returns:
        float: Perdida promedio de entrenamiento
    """
    net.train()
    running_loss = 0.0
    train_bar = tqdm(train_loader)

    for step, data in enumerate(train_bar):
        images, labels = data
        optimizer.zero_grad()
        logits = net(images.to(device))
        loss = loss_function(logits, labels.to(device))
        loss.backward()
        optimizer.step()

        running_loss += loss.item()
        train_bar.desc = "entrenamiento epoch[{}/{}] perdida:{:.3f}".format(
            epoch + 1, epochs, loss)

    return running_loss / len(train_loader)


def validate(net, validate_loader, device, epoch, epochs):
    """
    Valida el rendimiento del modelo

    Args:
        net: Objeto del modelo
        validate_loader: Cargador de datos de validacion
        device: Objeto de dispositivo PyTorch
        epoch (int): Numero de epoca actual
        epochs (int): Total de epocas

    Returns:
        float: Precision de validacion
    """
    net.eval()
    acc = 0.0
    val_num = 0

    with torch.no_grad():
        val_bar = tqdm(validate_loader)
        for val_images, val_labels in val_bar:
            outputs = net(val_images.to(device))
            predict_y = torch.max(outputs, dim=1)[1]
            acc += torch.eq(predict_y, val_labels.to(device)).sum().item()
            val_num += val_labels.size(0)

            val_bar.desc = "validacion epoch[{}/{}]".format(epoch + 1, epochs)

    return acc / val_num


def main():
    """
    Funcion principal de entrenamiento

    Ejecuta el flujo completo de entrenamiento: carga de datos, construccion del
    modelo, ciclo de entrenamiento y guardado del modelo.
    """
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    print("Usando dispositivo: {}".format(device))

    data_transform = get_data_transform()

    image_path = '/media/HyperVID-Dataset'
    assert os.path.exists(image_path), "La ruta {} no existe.".format(
        image_path)

    batch_size = 32
    train_loader, validate_loader, train_num, val_num = create_dataloaders(
        image_path, data_transform, batch_size=batch_size)

    net = build_model(device, num_classes=1777)

    loss_function = nn.CrossEntropyLoss()
    params = [p for p in net.parameters() if p.requires_grad]
    optimizer = optim.Adam(params, lr=0.0001)

    epochs = 500
    best_acc = 0.0
    save_path = './resnet34.pth'
    train_steps = len(train_loader)

    for epoch in range(epochs):
        train_loss = train_one_epoch(net, train_loader, loss_function,
                                     optimizer, device, epoch, epochs)
        val_accurate = validate(net, validate_loader, device, epoch, epochs)

        print('[epoch %d] perdida_entrenamiento: %.3f  precision_validacion: %.3f' %
              (epoch + 1, train_loss, val_accurate))

        if val_accurate > best_acc:
            best_acc = val_accurate
            torch.save(net.state_dict(), save_path)

    print('Entrenamiento finalizado')


if __name__ == '__main__':
    main()
