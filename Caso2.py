"""
Programación Paralela

Práctica 1: Búsqueda en paralelo
"""
import time
# caso avanzado

from multiprocessing import Queue, Process

from time import perf_counter
import math

import os

dir = os.getcwd()   # Obtengo la ruta actual

#OBS: estoy presuponiendo que las pruebas se encuentran en la misma carpeta que este archivo.py


# CONDICIONES USADAS (deberán ser cambiadas a gusto del usuario).

# La estructura SIEMPRE deberá ser la de una función: str -> bool.
#OBS: esto permitiría establecer condiciones más generales, no necesariamente para números.


def es_primo(num_str: str)-> bool:   # f1: primera condición
    
    numero = int (num_str)
    
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

####### CREACIÓN DE ESTRUCTURAS

# Coloca los elementos de una lista en una cola.
def encolar_lista(lista: list, q: Queue) -> Queue:
    for x in lista:
        q.put(x)

    return q


# Generamos una función auxiliar para poder usar centinelas

def colocar_nones(q: Queue, nr_procesos: int) -> None:
    for _ in range(nr_procesos):
        q.put(None)

    return q

# La cola de archivos tiene los Nones al final, para haber terminado con todos los archivos antes.
def creacion_cola_arch(nr_proc:int, archs: list) -> Queue:
        
    q = Queue()
    pre_cola_arch = encolar_lista(archs, q)
    c_arch = colocar_nones(pre_cola_arch, nr_proc)

    return c_arch
def creacion_cola_coin(nr_req:int) -> Queue:

    q = Queue()
    c_coin = colocar_nones(q,nr_req)

    return c_coin
####### CONTROL DE CONDICIONES

# Comprueba si un string dado cumple todas las condiciones, dadas como una lista de funciones.
# En este caso el string representa un int y mis condiciones son si es primo y termina en 227.

def cumple_conds(num_str: str, funciones) -> bool:
    
    return all(f(num_str) for f in funciones)

####### BÚSQUEDA EN LOS DATOS

# A diferencia del caso básico, sólo lee archivos y busca strs que cumplan las condiciones.
# Usamos centinelas para evitar problemas con la concurrencia.
def buscador(cola_arch: Queue, cola_coin: Queue, conds: list[callable]) -> Queue:


    nombre = cola_arch.get()

    while nombre is not None:

        archivo = open(nombre, "r")

        for line in archivo :

            for num_str in line.split():

                if cumple_conds(num_str, conds):

                    marca = cola_coin.get()  # veo mi marcador de cola.coin

                    if marca is None:

                        cola_coin.put(int(num_str))
                        print(num_str)

                    else:

                        cola_coin.put(marca)      # lo devuelvo a la cola y termino; ya tengo lo que buscaba

                        return cola_coin

        archivo.close()
        nombre = cola_arch.get()

    return cola_coin

####### DEVOLUCIÓN DE RESULTADOS

def limpiar_cola(c: Queue, nr_nones):
    e = c.get()
    while e is None:
        e = c.get()
    c.put(e)  # añado el último de vuelta
    return c

def de_cola_a_lista(c: Queue) -> [int]:  # función auxiliar que trata la cola de resultados
    # Como no influye en la paralelización, podemos usar empty sin problemas.

    l = []    

    while not c.empty() :     

        x = c.get()
        l.append(x)       

    return l
# Dada una lista y un número de datos requeridos, devuelvo los resultados del programa.
#OBS: lo hago mediante print pero podría hacerse de otro modo, al tenerlo en una función independiente.  
     
def resultados(lista: [int], nr_datos: int):
    if len(lista) < nr_datos:
        print(f'No se ha encontrado la cantidad de números que cumplen la condición requerida. Se pedían {nr_datos}. Se han encontrado {len(lista)}.')
    else:
        print(f'Se han encontrado al menos {len(lista)} números que cumplen la condición. Los primeros de ellos son:')
        print(lista)
        

####### FUNCIÓN PRINCIPAL

if __name__ == "__main__":

    # Lista de archivos .txt usando comprensión de listas y la ubicación dir
    archivos = [archivo for archivo in os.listdir(dir) if not archivo.endswith('.py')]
    
    nr_archivos = len(archivos)
    
    # Introducimos un ajuste dinámico del número de procesos .
    # Tomamos la raíz del número de archivos redondeada hacia abajo (por pruebas que hemos ido haciendo)
    # pero lo acotamos por 7, pues también influye el número de núcleos del sistema.
    
    nr_procesos = min (math.floor(math.sqrt(nr_archivos)), 7)

    nr_datos_requeridos = 1_000_000 # condiciónn modificable por el usuario

    cola_arch = creacion_cola_arch(nr_procesos,archivos)

    cola_coin = creacion_cola_coin(nr_datos_requeridos)

    pr_buscadores = []
        
    for _ in range(nr_procesos):
        
        p = Process(target=buscador, args=(cola_arch, cola_coin, funciones))
        # OBS: estoy dando como argumento la lista de funciones(condiciones) que debe ser establecida por el usuario
        # al comienzo del código.
        pr_buscadores.append(p)
        p.start()
    
    for pr in pr_buscadores:
    
        pr.join()

    # Quito los Nones en el caso de que quedara alguno:
    cola_coin_limpia = limpiar_cola(cola_coin,nr_datos_requeridos)
    lfinal = de_cola_a_lista(cola_coin_limpia)
    
    resultados(lfinal, nr_datos_requeridos)  # llamo a mi función dedicada a los resultados






