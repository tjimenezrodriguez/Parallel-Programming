"""
Programación Paralela

Práctica 1: Búsqueda en paralelo

Integrantes: Álex Carrillo Delgado, Alonso Delgado Morales, Teófilo Jiménez Rodríguez.

"""

# caso avanzado

from multiprocessing import Queue, Process, Event

# Manager me permite tener objetos compartidos.

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

# Creamos una lista con los elementos de la cola.
def sacar_lista(c: Queue) -> [int]:

    l = []    

    while not c.empty() :     

        x = sacar(c)
        l.append(x)       

    return l

# Comprueba si un string dado cumple todas las condiciones, dadas como una lista de funciones.
# En este caso el string representa un int y mis condiciones son si es primo y termina en 227.

def cumple_conds(num_str: str, funciones) -> bool:
    
    return all(f(num_str) for f in funciones)

# A diferencia del caso básico, sólo lee archivos y busca strs que cumplan las condiciones.
def buscador(cola_arch: Queue, e: Event, cola_coin: Queue, conds: [callable]) -> Queue:

    while not e.is_set():

        if not cola_arch.empty():

            nombre = sacar(cola_arch)
            archivo = open(nombre, "r")

            for line in archivo :                

                for num_str in line.split():                       

                    if cumple_conds(num_str, conds):
                        
                        cola_coin.put(int(num_str))

            archivo.close()

        else:

            break
        
    return cola_coin   # cuando se ha recibido el evento se devuelve lo que ha obtenido cada uno.


# Dada una lista y un número de datos requeridos, devuelvo los resultados del programa.
#OBS: lo hago mediante print pero podría hacerse de otro modo, al tenerlo en una función independiente.  
     
def resultados(lista: [int], nr_datos: int):
    if len(lista) < nr_datos:
        print(f'No se ha encontrado la cantidad de números que cumplen la condición requerida. Se pedían {nr_datos}. Se han encontrado {len(lista)}.')
    else:
        print(f'Se han encontrado al menos {len(lista)} números que cumplen la condición. Los primeros de ellos son:')
        print(lista)
        
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

    nr_datos_requeridos = 4

    cola_coin = Queue(nr_datos_requeridos)
    
    pr_buscadores = []
    
    while not cola_coin.full() and (any(proceso.is_alive() for proceso in pr_buscadores) or pr_buscadores==[]):
        
        # mientras no hayamos obtenido lo buscado y los procesos sigan trabajando o acabe de empezar.
        
        for _ in range(nr_procesos):
        
            p = Process(target=buscador, args=(cola_arch, e, cola_coin, funciones))
            # OBS: estoy dando como argumento la lista de funciones(condiciones) que debe ser establecida por el usuario
            # al comienzo del código.
            pr_buscadores.append(p)
            p.start()
    
        for pr in pr_buscadores:
    
            pr.join()
    
    e.set() # cuando tenga lo que quiero, inicio el evento        

    l = sacar_lista(cola_coin)
    
    resultados(l, nr_datos_requeridos)  # llamo a mi función dedicada a los resultados

    t2 = perf_counter()    
    print(f"Tiempo: {t2 - t1} segundos")

    



