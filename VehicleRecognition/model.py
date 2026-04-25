"""
Implementacion del modelo ResNet

Contiene los bloques BasicBlock, Bottleneck y la estructura completa de la red ResNet
"""

import torch.nn as nn
import torch


class BasicBlock(nn.Module):
    """
    Bloque basico de ResNet (para ResNet-18 y ResNet-34)

    Attributes:
        expansion (int): Factor de expansion de canales de salida, fijo en 1
    """
    expansion = 1

    def __init__(self,
                 in_channel,
                 out_channel,
                 stride=1,
                 downsample=None,
                 **kwargs):
        """
        Inicializa BasicBlock

        Args:
            in_channel (int): Numero de canales de entrada
            out_channel (int): Numero de canales de salida
            stride (int): Paso de convolucion, por defecto 1
            downsample (nn.Module): Capa de submuestreo para ajustar dimensiones de entrada, por defecto None
            **kwargs: Otros parametros
        """
        super(BasicBlock, self).__init__()
        self.conv1 = nn.Conv2d(in_channels=in_channel,
                               out_channels=out_channel,
                               kernel_size=3,
                               stride=stride,
                               padding=1,
                               bias=False)
        self.bn1 = nn.BatchNorm2d(out_channel)
        self.relu = nn.ReLU()
        self.conv2 = nn.Conv2d(in_channels=out_channel,
                               out_channels=out_channel,
                               kernel_size=3,
                               stride=1,
                               padding=1,
                               bias=False)
        self.bn2 = nn.BatchNorm2d(out_channel)
        self.downsample = downsample

    def forward(self, x):
        """
        Propagacion hacia adelante

        Args:
            x (Tensor): Tensor de entrada

        Returns:
            Tensor: Tensor de salida
        """
        identity = x
        if self.downsample is not None:
            identity = self.downsample(x)

        out = self.conv1(x)
        out = self.bn1(out)
        out = self.relu(out)

        out = self.conv2(out)
        out = self.bn2(out)

        out += identity
        out = self.relu(out)

        return out


class Bottleneck(nn.Module):
    """
    Bloque cuello de botella de ResNet (para ResNet-50, ResNet-101, etc.)

    Attributes:
        expansion (int): Factor de expansion de canales de salida, fijo en 4
    """
    expansion = 4

    def __init__(self,
                 in_channel,
                 out_channel,
                 stride=1,
                 downsample=None,
                 groups=1,
                 width_per_group=64):
        """
        Inicializa Bottleneck

        Args:
            in_channel (int): Numero de canales de entrada
            out_channel (int): Numero de canales de salida
            stride (int): Paso de convolucion, por defecto 1
            downsample (nn.Module): Capa de submuestreo para ajustar dimensiones de entrada, por defecto None
            groups (int): Numero de grupos para convolucion agrupada, por defecto 1
            width_per_group (int): Ancho por grupo, por defecto 64
        """
        super(Bottleneck, self).__init__()

        width = int(out_channel * (width_per_group / 64.)) * groups

        self.conv1 = nn.Conv2d(in_channels=in_channel,
                               out_channels=width,
                               kernel_size=1,
                               stride=1,
                               bias=False)  # reducir canales
        self.bn1 = nn.BatchNorm2d(width)
        # -----------------------------------------
        self.conv2 = nn.Conv2d(in_channels=width,
                               out_channels=width,
                               groups=groups,
                               kernel_size=3,
                               stride=stride,
                               bias=False,
                               padding=1)
        self.bn2 = nn.BatchNorm2d(width)
        # -----------------------------------------
        self.conv3 = nn.Conv2d(in_channels=width,
                               out_channels=out_channel * self.expansion,
                               kernel_size=1,
                               stride=1,
                               bias=False)  # expandir canales
        self.bn3 = nn.BatchNorm2d(out_channel * self.expansion)
        self.relu = nn.ReLU(inplace=True)
        self.downsample = downsample

    def forward(self, x):
        """
        Propagacion hacia adelante

        Args:
            x (Tensor): Tensor de entrada

        Returns:
            Tensor: Tensor de salida
        """
        identity = x
        if self.downsample is not None:
            identity = self.downsample(x)

        out = self.conv1(x)
        out = self.bn1(out)
        out = self.relu(out)

        out = self.conv2(out)
        out = self.bn2(out)
        out = self.relu(out)

        out = self.conv3(out)
        out = self.bn3(out)

        out += identity
        out = self.relu(out)

        return out


class ResNet(nn.Module):
    """
    Red ResNet

    Clase principal de ResNet (Red Residual), soporta configuraciones de diferentes
    profundidades y variantes.

    Attributes:
        include_top (bool): Si incluir la capa fully-connected superior
        in_channel (int): Numero actual de canales de entrada
    """

    def __init__(self,
                 block,
                 blocks_num,
                 num_classes=1000,
                 include_top=True,
                 groups=1,
                 width_per_group=64):
        """
        Inicializa ResNet

        Args:
            block: Tipo de bloque base (BasicBlock o Bottleneck)
            blocks_num (list): Lista con el numero de bloques por capa, ej. [3, 4, 6, 3]
            num_classes (int): Numero de clases de clasificacion, por defecto 1000
            include_top (bool): Si incluir la capa fully-connected superior, por defecto True
            groups (int): Numero de grupos para convolucion agrupada, por defecto 1
            width_per_group (int): Ancho por grupo, por defecto 64
        """
        super(ResNet, self).__init__()
        self.include_top = include_top
        self.in_channel = 64

        self.groups = groups
        self.width_per_group = width_per_group

        self.conv1 = nn.Conv2d(3,
                               self.in_channel,
                               kernel_size=7,
                               stride=2,
                               padding=3,
                               bias=False)
        self.bn1 = nn.BatchNorm2d(self.in_channel)
        self.relu = nn.ReLU(inplace=True)
        self.maxpool = nn.MaxPool2d(kernel_size=3, stride=2, padding=1)
        self.layer1 = self._make_layer(block, 64, blocks_num[0])
        self.layer2 = self._make_layer(block, 128, blocks_num[1], stride=2)
        self.layer3 = self._make_layer(block, 256, blocks_num[2], stride=2)
        self.layer4 = self._make_layer(block, 512, blocks_num[3], stride=2)
        if self.include_top:
            self.avgpool = nn.AdaptiveAvgPool2d((1, 1))  # tamanio de salida = (1, 1)
            self.fc = nn.Linear(512 * block.expansion, num_classes)

        for m in self.modules():
            if isinstance(m, nn.Conv2d):
                nn.init.kaiming_normal_(m.weight,
                                        mode='fan_out',
                                        nonlinearity='relu')

    def _make_layer(self, block, channel, block_num, stride=1):
        """
        Construye una capa de la red

        Args:
            block: Tipo de bloque base
            channel (int): Numero de canales de salida
            block_num (int): Numero de bloques en esta capa
            stride (int): Paso del primer bloque, por defecto 1

        Returns:
            nn.Sequential: Capa de red construida
        """
        downsample = None
        if stride != 1 or self.in_channel != channel * block.expansion:
            downsample = nn.Sequential(
                nn.Conv2d(self.in_channel,
                          channel * block.expansion,
                          kernel_size=1,
                          stride=stride,
                          bias=False),
                nn.BatchNorm2d(channel * block.expansion))

        layers = []
        layers.append(
            block(self.in_channel,
                  channel,
                  downsample=downsample,
                  stride=stride,
                  groups=self.groups,
                  width_per_group=self.width_per_group))
        self.in_channel = channel * block.expansion

        for _ in range(1, block_num):
            layers.append(
                block(self.in_channel,
                      channel,
                      groups=self.groups,
                      width_per_group=self.width_per_group))

        return nn.Sequential(*layers)

    def forward(self, x):
        """
        Propagacion hacia adelante

        Args:
            x (Tensor): Tensor de entrada

        Returns:
            Tensor: Tensor de salida
        """
        x = self.conv1(x)
        x = self.bn1(x)
        x = self.relu(x)
        x = self.maxpool(x)

        x = self.layer1(x)
        x = self.layer2(x)
        x = self.layer3(x)
        x = self.layer4(x)

        if self.include_top:
            x = self.avgpool(x)
            x = torch.flatten(x, 1)
            x = self.fc(x)

        return x


def resnet34(num_classes=1000, include_top=True):
    """
    Construye el modelo ResNet-34

    Args:
        num_classes (int): Numero de clases de clasificacion, por defecto 1000
        include_top (bool): Si incluir la capa fully-connected superior, por defecto True

    Returns:
        ResNet: Modelo ResNet-34

    Note:
        Pesos preentrenados: https://download.pytorch.org/models/resnet34-333f7ec4.pth
    """
    return ResNet(BasicBlock, [3, 4, 6, 3],
                  num_classes=num_classes,
                  include_top=include_top)


def resnet50(num_classes=1000, include_top=True):
    """
    Construye el modelo ResNet-50

    Args:
        num_classes (int): Numero de clases de clasificacion, por defecto 1000
        include_top (bool): Si incluir la capa fully-connected superior, por defecto True

    Returns:
        ResNet: Modelo ResNet-50

    Note:
        Pesos preentrenados: https://download.pytorch.org/models/resnet50-19c8e357.pth
    """
    return ResNet(Bottleneck, [3, 4, 6, 3],
                  num_classes=num_classes,
                  include_top=include_top)


def resnet101(num_classes=1000, include_top=True):
    """
    Construye el modelo ResNet-101

    Args:
        num_classes (int): Numero de clases de clasificacion, por defecto 1000
        include_top (bool): Si incluir la capa fully-connected superior, por defecto True

    Returns:
        ResNet: Modelo ResNet-101

    Note:
        Pesos preentrenados: https://download.pytorch.org/models/resnet101-5d3b4d8f.pth
    """
    return ResNet(Bottleneck, [3, 4, 23, 3],
                  num_classes=num_classes,
                  include_top=include_top)


def resnext50_32x4d(num_classes=1000, include_top=True):
    """
    Construye el modelo ResNeXt-50 32x4d

    Args:
        num_classes (int): Numero de clases de clasificacion, por defecto 1000
        include_top (bool): Si incluir la capa fully-connected superior, por defecto True

    Returns:
        ResNet: Modelo ResNeXt-50 32x4d

    Note:
        Pesos preentrenados: https://download.pytorch.org/models/resnext50_32x4d-7cdf4587.pth
    """
    groups = 32
    width_per_group = 4
    return ResNet(Bottleneck, [3, 4, 6, 3],
                  num_classes=num_classes,
                  include_top=include_top,
                  groups=groups,
                  width_per_group=width_per_group)


def resnext101_32x8d(num_classes=1000, include_top=True):
    """
    Construye el modelo ResNeXt-101 32x8d

    Args:
        num_classes (int): Numero de clases de clasificacion, por defecto 1000
        include_top (bool): Si incluir la capa fully-connected superior, por defecto True

    Returns:
        ResNet: Modelo ResNeXt-101 32x8d

    Note:
        Pesos preentrenados: https://download.pytorch.org/models/resnext101_32x8d-8ba56ff5.pth
    """
    groups = 32
    width_per_group = 8
    return ResNet(Bottleneck, [3, 4, 23, 3],
                  num_classes=num_classes,
                  include_top=include_top,
                  groups=groups,
                  width_per_group=width_per_group)
