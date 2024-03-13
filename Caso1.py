"""
Programación Paralela

Práctica 1: Búsqueda en paralelo

"""

# caso básico

from multiprocessing import Queue, Process, Event
import math

import os

dir = os.getcwd()   # Obtengo la ruta actual

# OBS: estoy presuponiendo que las pruebas se encuentran en la misma carpeta que este archivo.py


# CONDICIONES USADAS (deberán ser cambiadas a gusto del usuario).

# La estructura SIEMPRE deberá ser la de una función: str -> bool.
# OBS: esto permitiría establecer condiciones más generales, no necesariamente para números.


def es_primo(num_str: str) -> bool:   # f1: primera condición
    
    numero = int(num_str)
    
    if numero < 2:
        
        return False
    
    for i in range(2, int(math.sqrt(numero)) + 1):
    
        if numero % i == 0:
        
            return False
        
    return True

def termina_227(num_str: str)-> bool:   # f2: segunda condición
    
    return num_str.endswith('227')


# En esta lista debemos añadir las condiciones (que vienen dadas por funciones).

funciones = [es_primo, termina_227]


###################################################################################################

# CÓDIGO PARA EL FUNCIONAMIENTO

# Coloca los elementos de una lista en una cola.
def encolar_lista(lista: list, q: Queue) -> Queue:
    for x in lista:
        q.put(x)

    return q


# Generamos una función auxiliar para poder usar centinelas

def colocar_nones(q: Queue, nr_procesos: int) -> Queue:
    for _ in range(nr_procesos):
        q.put(None)

    return q


# La cola de archivos tiene los Nones al final, para haber terminado con todos los archivos antes.
def creacion_cola_arch(nr_proc: int, archs: list) -> Queue:
    q = Queue()
    pre_cola_arch = encolar_lista(archs, q)
    c_arch = colocar_nones(pre_cola_arch, nr_proc)

    return c_arch

def leer_archivo(nombre_arch: str) -> list:

    l_arch = []

    archivo = open(nombre_arch, "r")

    for line in archivo:

        for num_str in line.split():

            l_arch.append(num_str)

    archivo.close()

    return l_arch

# Comprueba si un string dado cumple todas las condiciones, dadas como una lista de funciones.
# En este caso el string representa un int y mis condiciones son si es primo y termina en 227.

def cumple_conds(num_str: str, funciones) -> bool:

    return all(f(num_str) for f in funciones)

# Lee los archivos de la cola, busca un número que cumpla la condición,
# y lo agrega junto con el nombre del archivo a la cola de resultados.
# Usamos centinelas para evitar problemas con la concurrencia.
def lectura_comprobacion_archivo_cola(cola_arch: Queue, cola_res: Queue, parada: Event, conds: list[callable]):

    nombre = cola_arch.get()

    while nombre is not None:

        l_archivo = leer_archivo(nombre)
        n = len(l_archivo)
        i= 0

        while i<n and not parada.is_set():

            num = l_archivo[i]

            if cumple_conds(num, conds):

                cola_res.put((int(num),nombre))

            i += 1

        nombre = cola_arch.get()


# Dada una cola, devuelvo el resultado de la búsqueda. Al tenerlo en una función aparte, podría devolverlo de otras formas.
# OBS: Como no forma parte de la paralelización, no tenemos problemas de concurrencia con empty.

def resultado(c: Queue):

    if c.empty():

        print('En la lista de archivos indicada no hay ningún elemento que cumpla la condición.')

    else:

        x, y = c.get()
        print(f'El primer número que cumple la condición es {x}. Está en el archivo {y}.')

# Función principal.

if __name__ == "__main__":

    # Lista de archivos .txt usando comprensión de listas y la ubicación dir
    archivos = [archivo for archivo in os.listdir(dir) if not archivo.endswith('.py')]
    # Como convención, leo todos los archivos del directorio salvo los .py

    nr_archivos = len(archivos)

    # Introducimos un ajuste dinámico del número de procesos .
    # Tomamos la raíz del número de archivos redondeada hacia abajo (por pruebas que hemos ido haciendo)
    # pero lo acotamos por 7, pues también influye el número de núcleos del sistema.

    nr_procesos = min (math.floor(math.sqrt(nr_archivos)), 7)

    cola_arch = creacion_cola_arch(nr_procesos, archivos)
    cola_res = Queue()
    parada = Event()

    procesos = []

    for _ in range (nr_procesos):

        p = Process(target=lectura_comprobacion_archivo_cola, args=(cola_arch, cola_res, parada, funciones))

        # OBS: estoy dando como argumento la lista de funciones(condiciones) que debe ser establecida por el usuario
        # al comienzo del código.

        procesos.append(p)
        p.start()

    while any(proceso.is_alive() for proceso in procesos) and cola_res.empty(): # estoy aquí hasta que obtenga algo sin hacer nada

        pass

    parada.set()

    for proceso in procesos:

        proceso.join()

    resultado(cola_res)
