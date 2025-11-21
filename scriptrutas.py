import os
import csv
import argparse
from pathlib import Path
from datetime import datetime

def buscar_archivos_json(ruta_base, max_profundidad=5):
    """
    Busca archivos JSON en la ruta especificada hasta la profundidad máxima.
    Encuentra archivos en TODOS los niveles desde 0 hasta max_profundidad.
    
    Args:
        ruta_base (str): Ruta donde buscar los archivos
        max_profundidad (int): Profundidad máxima de búsqueda (default: 5)
    
    Returns:
        list: Lista de tuplas (ruta_completa, nivel) de archivos JSON encontrados
    """
    rutas_json = []
    ruta_base = Path(ruta_base)
    
    if not ruta_base.exists():
        print(f"  Error: La ruta {ruta_base} no existe.")
        return rutas_json
    
    if not ruta_base.is_dir():
        print(f"  Error: {ruta_base} no es un directorio.")
        return rutas_json
    
    def explorar_directorio(directorio, profundidad_actual=0):
        """Función recursiva para explorar directorios"""
        if profundidad_actual > max_profundidad:
            return
        
        try:
            for item in directorio.iterdir():
                if item.is_file() and item.suffix.lower() == '.json':
                    rutas_json.append((str(item.absolute()), profundidad_actual))
                    print(f"✓ [Nivel {profundidad_actual}] Encontrado: {item.name}")
                    print(f"  Ruta: {item.absolute()}")
                elif item.is_dir() and profundidad_actual < max_profundidad:
                    explorar_directorio(item, profundidad_actual + 1)
        except PermissionError:
            print(f"  Sin permisos para acceder a: {directorio}")
        except Exception as e:
            print(f"  Error al explorar {directorio}: {e}")
    
    print("=" * 70)
    print(f"  Buscando archivos JSON en: {ruta_base.absolute()}")
    print(f"  Profundidad máxima: {max_profundidad} niveles (0 a {max_profundidad})")
    print("=" * 70)
    
    explorar_directorio(ruta_base)
    
    return rutas_json

def crear_csv(rutas_json, nombre_archivo="archivos_json_encontrados.csv", incluir_nivel=True):
    """
    Crea un archivo CSV con las rutas de archivos JSON encontrados.
    
    Args:
        rutas_json (list): Lista de tuplas (ruta, nivel) de archivos JSON
        nombre_archivo (str): Nombre del archivo CSV a crear o reemplazar
        incluir_nivel (bool): Si True, incluye el nivel de profundidad en el CSV
    """
    if not rutas_json:
        print("\n" + "=" * 70)
        print("  NO SE ENCONTRARON ARCHIVOS JSON")
        print("=" * 70)
        print("  Sugerencias:")
        print("   - Verifica que la ruta sea correcta")
        print("   - Asegúrate de que existan archivos .json en la ubicación")
        print("   - Intenta aumentar la profundidad de búsqueda")
        return

    ruta_csv = Path(nombre_archivo).resolve()
    
    # Crear directorio si no existe
    ruta_csv.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        archivo_existente = ruta_csv.exists()
        
        with open(ruta_csv, 'w', newline='', encoding='utf-8') as archivo_csv:
            escritor = csv.writer(archivo_csv)
            
            # Escribir encabezados si se incluye nivel
            if incluir_nivel:
                escritor.writerow(['Ruta_Completa', 'Nivel_Profundidad'])
                # Escribir datos
                for ruta, nivel in rutas_json:
                    escritor.writerow([ruta, nivel])
            else:
                # Solo escribir rutas
                for ruta, nivel in rutas_json:
                    escritor.writerow([ruta])
        
        print("\n" + "=" * 70)
        print("  ARCHIVO CSV CREADO EXITOSAMENTE")
        print("=" * 70)
        print(f"  Ruta: {ruta_csv}")
        if archivo_existente:
            print("   El archivo ya existía y fue reemplazado.")
        print(f"  Total de archivos JSON encontrados: {len(rutas_json)}")
        
        # Resumen por niveles
        niveles_dict = {}
        for ruta, nivel in rutas_json:
            niveles_dict[nivel] = niveles_dict.get(nivel, 0) + 1
        
        print("\n  Distribución por nivel:")
        for nivel in sorted(niveles_dict.keys()):
            print(f"   Nivel {nivel}: {niveles_dict[nivel]} archivo(s)")
        
    except Exception as e:
        print(f"\n  Error al crear el archivo CSV: {e}")


def main():
    """Función principal del script"""
    parser = argparse.ArgumentParser(
        description='Busca archivos JSON en una ruta específica y genera un CSV con los resultados',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Ejemplos de uso:
  python scriptrutas.py /ruta/a/buscar
  python scriptrutas.py /ruta/a/buscar -p 5
  python scriptrutas.py /ruta/a/buscar -o mi_archivo.csv
  python scriptrutas.py /ruta/a/buscar -p 3 -o resultados.csv --sin-nivel
        '''
    )
    parser.add_argument(
        'ruta', 
        help='Ruta donde buscar los archivos JSON'
    )
    parser.add_argument(
        '-p', '--profundidad', 
        type=int, 
        default=5, 
        help='Profundidad máxima de búsqueda (default: 5)'
    )
    parser.add_argument(
        '-o', '--output', 
        default=f"archivos_json_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        help='Nombre del archivo CSV de salida'
    )
    parser.add_argument(
        '--sin-nivel',
        action='store_true',
        help='No incluir el nivel de profundidad en el CSV'
    )
    
    args = parser.parse_args()
    
    # Buscar archivos JSON
    rutas_encontradas = buscar_archivos_json(args.ruta, args.profundidad)
    
    # Crear archivo CSV
    crear_csv(rutas_encontradas, args.output, incluir_nivel=not args.sin_nivel)

if __name__ == "__main__":
    # Ejemplo de uso directo (sin argumentos de línea de comandos)
    if len(os.sys.argv) == 1:
        print("=" * 70)
        print("            BUSCADOR DE ARCHIVOS JSON - MODO INTERACTIVO")
        print("=" * 70)
        ruta_busqueda = input("\n  Ingresa la ruta donde buscar archivos JSON: ").strip()
        
        if ruta_busqueda:
            try:
                prof_input = input("  Profundidad máxima (default 5, presiona Enter): ").strip()
                profundidad = int(prof_input) if prof_input else 5
            except ValueError:
                print("   Valor inválido, usando profundidad por defecto: 5")
                profundidad = 5
            
            nombre_csv_input = "../Proyect_Rips/Bases/Rutas_Json.csv"
            if nombre_csv_input:
                nombre_csv = nombre_csv_input
            else:
                nombre_csv = f"archivos_json_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            print("\n Iniciando búsqueda...\n")
            rutas_encontradas = buscar_archivos_json(ruta_busqueda, profundidad)
            crear_csv(rutas_encontradas, nombre_csv)
        else:
            print("  Ruta no proporcionada. Terminando...")
    else:
        main()