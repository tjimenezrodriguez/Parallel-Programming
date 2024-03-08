"""
Programación Paralela

Práctica 1: Búsqueda en paralelo

Integrantes: Álex Carrillo Delgado, Alonso Delgado Morales, Teófilo Jiménez Rodríguez.

"""

# caso básico

from multiprocessing import Queue, Process, Event
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

def termina_277(num_str: str)-> bool:   # f2: segunda condición
    
    return num_str.endswith('227')


# En esta lista debemos añadir las condiciones (que vienen dadas por funciones).

funciones = [es_primo, termina_277]


###################################################################################################

# CÓDIGO PARA EL FUNCIONAMIENTO
# Crea una cola y coloca los elementos de la lista en ella.
def colocar(lista: [int]) -> Queue:
    
    q = Queue(len(lista))
    
    for x in lista:
        
        q.put(x)
    
    return q

# Extrae un elemento de la cola.
def sacar(cola: Queue) -> int:
    
    return cola.get()

# Comprueba si un string dado cumple todas las condiciones, dadas como una lista de funciones.
# En este caso el string representa un int y mis condiciones son si es primo y termina en 227.

def cumple_conds(num_str: str, funciones) -> bool:
    
    return all(f(num_str) for f in funciones)

# Lee los archivos de la cola, busca un número que cumpla la condición, 
# y lo agrega junto con el nombre del archivo a la cola de resultados.
def lectura_comprobacion_archivo_cola(cola_arch: Queue, e: Event, cola_res: Queue, conds: [callable]) -> Queue:
    
    while not e.is_set():
        
        if not cola_arch.empty():
            
            nombre = sacar(cola_arch)
            archivo = open(nombre, "r")
            
            for line in archivo:
                
                linea = line.split()
                
                for num_str in linea:
                    
                    if cumple_conds(num_str, conds):
                        
                        cola_res.put((int(num_str), nombre))
                        archivo.close()
                        
                        return cola_res
                    
            archivo.close()
            
        else:
            
            break
        
    return cola_res

# Dada una cola, devuelvo el resultado de la búsqueda. Al tenerlo en una función aparte, podría devolverlo de otras formas.
def resultado(c: Queue):
    if not c.empty():
        
        x, y = sacar(c)
        print(f'El primer número que cumple la condición es {x}. Está en el archivo {y}.')
        
    else:
        
        print('En la lista de archivos indicada no hay ningún elemento que cumpla la condición.')

# Función principal.

if __name__ == "__main__":
    t1 = perf_counter()
    
    # Lista de archivos .txt usando comprensión de listas y la ubicación dir
    archivos = [archivo for archivo in os.listdir(dir) if archivo.endswith('.txt')]
    
    nr_archivos = len(archivos)
    
    # Introducimos un ajuste dinámico del número de procesos .
    # Tomamos la raíz del número de archivos redondeada hacia abajo (por pruebas que hemos ido haciendo)
    # pero lo acotamos por 7, pues también influye el número de núcleos del sistema.
    
    nr_procesos = min (math.floor(math.sqrt(nr_archivos)), 7)
    
    cola_arch = colocar(archivos)
    e = Event()
    cola_res = Queue()
    
    pr_actual = 0
    
    procesos = []
    
    while cola_res.empty() and (any(proceso.is_alive() for proceso in procesos) or procesos==[]) : 
 
        # mientras no hayamos obtenido lo buscado y los procesos sigan trabajando o acabe de empezar.
    
        while pr_actual < nr_procesos:
            
            p = Process(target=lectura_comprobacion_archivo_cola, args=(cola_arch, e, cola_res, funciones))
            
            # OBS: estoy dando como argumento la lista de funciones(condiciones) que debe ser establecida por el usuario
            # al comienzo del código.
            
            procesos.append(p)
            p.start()
            pr_actual += 1
    
        for proceso in procesos:
            
            proceso.join()
    
    e.set() # cuando tenga lo que quiero, inicio el evento
    
    resultado(cola_res)
    
    t2 = perf_counter()
    print(f"Tiempo: {t2 - t1} segundos")
