import os
import csv
import argparse
from pathlib import Path
from datetime import datetime

def buscar_archivos_json(ruta_base, max_profundidad=2):
    """
    Busca archivos JSON en la ruta especificada hasta la profundidad máxima.
    
    Args:
        ruta_base (str): Ruta donde buscar los archivos
        max_profundidad (int): Profundidad máxima de búsqueda (default: 2)
    
    Returns:
        list: Lista de rutas completas de archivos JSON encontradosds
    """
    rutas_json = []
    ruta_base = Path(ruta_base)
    
    if not ruta_base.exists():
        print(f"Error: La ruta {ruta_base} no existe.")
        return rutas_json
    
    if not ruta_base.is_dir():
        print(f"Error: {ruta_base} no es un directorio.")
        return rutas_json
    
    def explorar_directorio(directorio, profundidad_actual=0):
        """Función recursiva para explorar directorios"""
        if profundidad_actual > max_profundidad:
            return
        
        try:
            for item in directorio.iterdir():
                if item.is_file() and item.suffix.lower() == '.json':
                    rutas_json.append(str(item.absolute()))
                    print(f"Encontrado: {item.absolute()}")
                elif item.is_dir() and profundidad_actual < max_profundidad:
                    explorar_directorio(item, profundidad_actual + 1)
        except PermissionError:
            print(f"Sin permisos para acceder a: {directorio}")
        except Exception as e:
            print(f"Error al explorar {directorio}: {e}")
    
    print(f"Buscando archivos JSON en: {ruta_base.absolute()}")
    print(f"Profundidad máxima: {max_profundidad} niveles")
    print("-" * 50)
    
    explorar_directorio(ruta_base)
    
    return rutas_json

def crear_csv(rutas_json, nombre_archivo="archivos_json_encontrados.csv"):
    """
    Crea un archivo CSV con las rutas de archivos JSON encontrados.
    
    Args:
        rutas_json (list): Lista de rutas de archivos JSON
        nombre_archivo (str): Nombre del archivo CSV a crear o reemplazar
    """
    if not rutas_json:
        print("No se encontraron archivos JSON.")
        return

    ruta_csv = Path(nombre_archivo).resolve()
    
    try:
        archivo_existente = ruta_csv.exists()
        
        with open(ruta_csv, 'w', newline='', encoding='utf-8') as archivo_csv:
            escritor = csv.writer(archivo_csv)
            
            # Escribir datos
            for i, ruta in enumerate(rutas_json, 1):
                escritor.writerow([ruta])
        
        print("\nArchivo CSV creado exitosamente.")
        print(f"Ruta: {ruta_csv}")
        if archivo_existente:
            print("El archivo ya existía y fue reemplazado.")
        print(f"Total de archivos JSON encontrados: {len(rutas_json)}")
        
    except Exception as e:
        print(f"Error al crear el archivo CSV: {e}")


def main():
    """Función principal del script"""
    parser = argparse.ArgumentParser(
        description='Busca archivos JSON en una ruta específica y genera un CSV con los resultados'
    )
    parser.add_argument(
        'ruta', 
        help='Ruta donde buscar los archivos JSON'
    )
    parser.add_argument(
        '-p', '--profundidad', 
        type=int, 
        default=3, 
        help='Profundidad máxima de búsqueda (default: 2)'
    )
    parser.add_argument(
        '-o', '--output', 
        default=f"archivos_json_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        help='Nombre del archivo CSV de salida'
    )
    
    args = parser.parse_args()
    
    # Buscar archivos JSON
    rutas_encontradas = buscar_archivos_json(args.ruta, args.profundidad)
    
    # Crear archivo CSV
    crear_csv(rutas_encontradas, args.output)

if __name__ == "__main__":
    # Ejemplo de uso directo (sin argumentos de línea de comandos)
    if len(os.sys.argv) == 1:
        print("=== MODO INTERACTIVO ===")
        ruta_busqueda = input("Ingresa la ruta donde buscar archivos JSON: ").strip()
        
        if ruta_busqueda:
            try:
                profundidad = int(input("Profundidad máxima (default 3): ") or "3")
            except ValueError:
                profundidad = 2
            
            nombre_csv = "../Proyect_Rips/Bases/Rutas_Json.csv"
            if not nombre_csv:
                nombre_csv = f"archivos_json_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            rutas_encontradas = buscar_archivos_json(ruta_busqueda, profundidad)
            crear_csv(rutas_encontradas, nombre_csv)
        else:
            print("Ruta no proporcionada.")
    else:
        main()