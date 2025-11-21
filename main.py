import os
import pandas as pd
import logging
import traceback
from datetime import datetime
from typing import Dict, Any, List

# Importar las versiones corregidas
from controller.json_reader import JsonReader
from controller.diagnostic_completer import DiagnosticCompleter

# Configurar logging más detallado
def setup_logging():
    """Configura el sistema de logging"""
    
    # Crear formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Handler para archivo
    file_handler = logging.FileHandler('diagnostic_completion_debug.log', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Configurar logger principal
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

class MultiDiagnosticCompleter:
    """Procesador por lotes para múltiples archivos JSON con mejor manejo de errores"""
    
    def __init__(self):
        self.diagnosticos_dict = {}
        self.global_stats = {
            'archivos_procesados': 0,
            'archivos_exitosos': 0,
            'archivos_fallidos': 0,
            'total_usuarios_procesados': 0,
            'total_registros_procesados': 0,
            'total_cambios_realizados': 0,
            'total_diagnosticos_encontrados': 0,
            'errores_globales': []
        }
        self.archivos_stats = []
    
    def load_json_paths(self, ruta_csv: str) -> List[str]:
        """
        Carga las rutas de archivos JSON desde un CSV
        
        Args:
            ruta_csv: Ruta al archivo CSV con las rutas de los JSON
            
        Returns:
            List[str]: Lista de rutas de archivos JSON
        """
        try:
            logger.info(f"Cargando rutas desde: {ruta_csv}")
            
            # Verificar que el archivo existe
            if not os.path.exists(ruta_csv):
                logger.error(f"El archivo CSV no existe: {ruta_csv}")
                return []
            
            # Leer el CSV (asumiendo que tiene una sola columna con las rutas)
            try:
                # Intentar con diferentes separadores
                separadores = [',', ';', '\t']
                df = None
                
                for sep in separadores:
                    try:
                        df = pd.read_csv(ruta_csv, header=None, sep=sep)
                        if len(df.columns) >= 1 and len(df) > 0:
                            logger.info(f"CSV leído exitosamente con separador: '{sep}'")
                            break
                    except:
                        continue
                
                if df is None:
                    logger.error("No se pudo leer el archivo CSV con ningún separador")
                    return []
                    
            except Exception as e:
                logger.error(f"Error leyendo CSV: {e}")
                return []
            
            if df.empty:
                logger.error("El archivo CSV está vacío")
                return []
            
            # Tomar la primera columna
            rutas = df.iloc[:, 0].tolist()
            logger.info(f"Se encontraron {len(rutas)} rutas en el CSV")
            
            # Filtrar rutas que existen
            rutas_validas = []
            rutas_invalidas = []
            
            for ruta in rutas:
                ruta = str(ruta).strip()  # Limpiar espacios y convertir a string
                
                if pd.isna(ruta) or ruta == '' or ruta.lower() == 'nan':
                    continue
                
                if os.path.exists(ruta) and ruta.endswith('.json'):
                    rutas_validas.append(ruta)
                    logger.debug(f"Ruta válida: {ruta}")
                else:
                    rutas_invalidas.append(ruta)
                    logger.warning(f"Archivo no encontrado o no es JSON: {ruta}")
            
            logger.info(f"Se encontraron {len(rutas_validas)} archivos JSON válidos de {len(rutas)} rutas")
            
            if rutas_invalidas:
                logger.warning(f"Rutas inválidas encontradas: {len(rutas_invalidas)}")
                for ruta in rutas_invalidas[:5]:  # Mostrar solo las primeras 5
                    logger.warning(f"  - {ruta}")
            
            return rutas_validas
            
        except Exception as e:
            logger.error(f"Error cargando rutas del CSV: {e}")
            logger.error(traceback.format_exc())
            return []
    
    def load_excel_data(self, ruta_excel: str) -> bool:
        """
        Carga y procesa los datos del archivo Excel/CSV
        
        Args:
            ruta_excel: Ruta al archivo Excel/CSV
            
        Returns:
            bool: True si se carga correctamente, False en caso contrario
        """
        try:
            logger.info(f"Cargando archivo de diagnósticos: {ruta_excel}")
            
            # Verificar que el archivo existe
            if not os.path.exists(ruta_excel):
                logger.error(f"El archivo no existe: {ruta_excel}")
                return False
            
            # Crear una instancia temporal del completador para cargar los datos
            temp_completer = DiagnosticCompleter()
            
            if not temp_completer.load_excel_data(ruta_excel):
                logger.error("Error cargando datos del Excel")
                return False
            
            # Transferir el diccionario de diagnósticos
            self.diagnosticos_dict = temp_completer.diagnosticos_dict.copy()
            
            logger.info(f"Diccionario de diagnósticos cargado con {len(self.diagnosticos_dict)} entradas")
            
            return True
            
        except Exception as e:
            logger.error(f"Error cargando Excel: {e}")
            logger.error(traceback.format_exc())
            self.global_stats['errores_globales'].append(f"Error cargando Excel: {e}")
            return False

    def process_multiple_jsons(self, rutas_json: List[str]) -> bool:
        """
        Procesa múltiples archivos JSON
        
        Args:
            rutas_json: Lista de rutas de archivos JSON
            
        Returns:
            bool: True si al menos un archivo se procesó correctamente
        """
        if not rutas_json:
            logger.error("No hay archivos JSON para procesar")
            return False
        
        logger.info(f"Iniciando procesamiento de {len(rutas_json)} archivos JSON")
        logger.warning(f"IMPORTANTE: Los archivos originales serán REEMPLAZADOS (se crearán backups con extensión .backup)")
        
        for i, ruta_json in enumerate(rutas_json, 1):
            logger.info(f"\n{'='*80}")
            logger.info(f"PROCESANDO ARCHIVO {i}/{len(rutas_json)}: {os.path.basename(ruta_json)}")
            logger.info(f"{'='*80}")
            
            archivo_stats = {
                'ruta': ruta_json,
                'procesado': False,
                'usuarios_procesados': 0,
                'registros_procesados': 0,
                'cambios_realizados': 0,
                'errores': []
            }
            
            try:
                # Procesar archivo individual
                if self._process_single_json(ruta_json, archivo_stats):
                    self.global_stats['archivos_exitosos'] += 1
                    archivo_stats['procesado'] = True
                    logger.info(f" Archivo procesado exitosamente: {os.path.basename(ruta_json)}")
                else:
                    self.global_stats['archivos_fallidos'] += 1
                    logger.error(f"✗ Error procesando archivo: {os.path.basename(ruta_json)}")
                
            except Exception as e:
                self.global_stats['archivos_fallidos'] += 1
                error_msg = f"Error inesperado procesando {os.path.basename(ruta_json)}: {e}"
                logger.error(error_msg)
                logger.error(traceback.format_exc())
                archivo_stats['errores'].append(error_msg)
            
            finally:
                self.global_stats['archivos_procesados'] += 1
                self.archivos_stats.append(archivo_stats)
        
        # Mostrar resumen final
        self._print_final_summary()
        
        return self.global_stats['archivos_exitosos'] > 0

    def _process_single_json(self, ruta_json: str, archivo_stats: dict) -> bool:
        """Procesa un archivo JSON individual y REEMPLAZA el archivo original"""
        
        try:
            # Verificar archivo antes de procesar
            logger.info(f"Verificando archivo: {ruta_json}")
            
            if not os.path.exists(ruta_json):
                error_msg = f"El archivo no existe: {ruta_json}"
                logger.error(error_msg)
                archivo_stats['errores'].append(error_msg)
                return False
            
            file_size = os.path.getsize(ruta_json)
            logger.info(f"Tamaño del archivo: {file_size} bytes")
            
            if file_size == 0:
                error_msg = "El archivo está vacío"
                logger.error(error_msg)
                archivo_stats['errores'].append(error_msg)
                return False
            
            # Crear instancias para este archivo
            json_reader = JsonReader()
            completer = DiagnosticCompleter()
            
            # Cargar archivo JSON
            logger.info("Cargando datos JSON...")
            json_data = json_reader.load_json(ruta_json)
            
            if json_data is None:
                error_msg = "Error cargando archivo JSON"
                logger.error(error_msg)
                archivo_stats['errores'].append(error_msg)
                return False
            
            logger.info(f"JSON cargado exitosamente. Tipo: {type(json_data)}")
            
            # Transferir diccionario de diagnósticos al completer
            completer.diagnosticos_dict = self.diagnosticos_dict.copy()
            logger.info(f"Diccionario de diagnósticos transferido: {len(completer.diagnosticos_dict)} entradas")
            
            # Obtener información del archivo
            users_info = json_reader.get_users_info()
            logger.info(f"Información del JSON:")
            logger.info(f"  - Total usuarios: {users_info.get('total_usuarios', 0)}")
            logger.info(f"  - Usuarios válidos: {users_info.get('usuarios_validos', 0)}")
            logger.info(f"  - Usuarios inválidos: {users_info.get('usuarios_invalidos', 0)}")
            logger.info(f"  - Usuarios con servicios: {users_info.get('usuarios_con_servicios', 0)}")
            logger.info(f"  - Total servicios: {users_info.get('total_servicios', 0)}")
            logger.info(f"  - Diagnósticos vacíos: {users_info.get('diagnosticos_vacios', 0)}")
            
            # Completar diagnósticos
            logger.info("Iniciando completado de diagnósticos...")
            if not completer.complete_diagnostics(json_data):
                error_msg = "Error completando diagnósticos"
                logger.error(error_msg)
                archivo_stats['errores'].append(error_msg)
                return False
            
            # *** CAMBIO PRINCIPAL: REEMPLAZAR EL ARCHIVO ORIGINAL ***
            logger.warning(f"  REEMPLAZANDO ARCHIVO ORIGINAL: {ruta_json}")
            logger.info(f"   Se creará backup como: {ruta_json}.backup")
            
            # Guardar en el archivo original (el JsonReader automáticamente crea backup)
            if not json_reader.save_json(json_data, ruta_json):
                error_msg = "Error guardando archivo procesado"
                logger.error(error_msg)
                archivo_stats['errores'].append(error_msg)
                return False
            
            # Obtener estadísticas
            stats = completer.get_stats()
            archivo_stats['usuarios_procesados'] = stats['usuarios_procesados']
            archivo_stats['registros_procesados'] = stats['registros_procesados']
            archivo_stats['cambios_realizados'] = stats['cambios_realizados']
            
            # Actualizar estadísticas globales
            self.global_stats['total_usuarios_procesados'] += stats['usuarios_procesados']
            self.global_stats['total_registros_procesados'] += stats['registros_procesados']
            self.global_stats['total_cambios_realizados'] += stats['cambios_realizados']
            self.global_stats['total_diagnosticos_encontrados'] += stats['diagnosticos_encontrados']
            
            # Verificar resultados (ahora comparando con el backup)
            backup_path = f"{ruta_json}.backup"
            if os.path.exists(backup_path):
                self._verify_single_file(backup_path, ruta_json)
            else:
                logger.warning("No se encontró archivo backup para verificación")
            
            logger.info(f" Archivo REEMPLAZADO exitosamente: {ruta_json}")
            logger.info(f" Backup disponible en: {backup_path}")
            
            return True
            
        except Exception as e:
            error_msg = f"Error inesperado en _process_single_json: {e}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            archivo_stats['errores'].append(error_msg)
            return False

    def _verify_single_file(self, ruta_original: str, ruta_procesado: str):
        """Verifica los cambios realizados comparando archivos"""
        try:
            logger.info("Verificando cambios realizados...")
            
            reader_original = JsonReader()
            reader_procesado = JsonReader()
            
            data_original = reader_original.load_json(ruta_original)
            data_procesado = reader_procesado.load_json(ruta_procesado)
            
            if not data_original or not data_procesado:
                logger.error("Error cargando archivos para verificación")
                return
            
            def count_empty_diagnostics(data):
                """Cuenta diagnósticos vacíos en todos los tipos de servicios"""
                count = 0
                total = 0
                
                try:
                    for usuario in data.get('usuarios', []):
                        servicios = usuario.get('servicios', {})
                        
                        service_types = ['consultas', 'procedimientos', 'medicamentos', 'otrosServicios']
                        
                        for service_type in service_types:
                            for service in servicios.get(service_type, []):
                                total += 1
                                cod_diag = str(service.get('codDiagnosticoPrincipal', '')).strip()
                                if (cod_diag == '' or 
                                    cod_diag.lower() in ['none', 'null', 'nan', 'nat'] or
                                    cod_diag == '0'):
                                    count += 1
                except Exception as e:
                    logger.error(f"Error contando diagnósticos: {e}")
                
                return count, total
            
            vacios_original, total_original = count_empty_diagnostics(data_original)
            vacios_procesado, total_procesado = count_empty_diagnostics(data_procesado)
            
            completados = vacios_original - vacios_procesado
            
            logger.info(f" Verificación de resultados:")
            logger.info(f"   - Total servicios: {total_original}")
            logger.info(f"   - Vacíos antes: {vacios_original}")
            logger.info(f"   - Vacíos después: {vacios_procesado}")
            logger.info(f"   - Diagnósticos completados: {completados}")
            
        except Exception as e:
            logger.error(f"Error en verificación: {e}")
            logger.error(traceback.format_exc())

    def _print_final_summary(self):
        """Imprime resumen final del procesamiento múltiple"""
        print(f"\n{'='*80}")
        print(f"RESUMEN FINAL DEL PROCESAMIENTO MÚLTIPLE")
        print(f"{'='*80}")
        print(f"Archivos procesados: {self.global_stats['archivos_procesados']}")
        print(f"Archivos exitosos: {self.global_stats['archivos_exitosos']}")
        print(f"Archivos fallidos: {self.global_stats['archivos_fallidos']}")
        print(f"Total usuarios procesados: {self.global_stats['total_usuarios_procesados']}")
        print(f"Total registros procesados: {self.global_stats['total_registros_procesados']}")
        print(f"Total cambios realizados: {self.global_stats['total_cambios_realizados']}")
        print(f"Total diagnósticos encontrados: {self.global_stats['total_diagnosticos_encontrados']}")
        
        print(f"\n{'='*80}")
        print(f"DETALLE POR ARCHIVO (ARCHIVOS ORIGINALES REEMPLAZADOS):")
        print(f"{'='*80}")
        
        for i, archivo in enumerate(self.archivos_stats, 1):
            status = " EXITOSO" if archivo['procesado'] else "✗ FALLIDO"
            nombre = os.path.basename(archivo['ruta'])
            backup_name = f"{nombre}.backup"
            print(f"{i:2d}. {status} - {nombre}")
            print(f"    Usuarios: {archivo['usuarios_procesados']}, " +
                  f"Registros: {archivo['registros_procesados']}, " +
                  f"Cambios: {archivo['cambios_realizados']}")
            if archivo['procesado']:
                print(f"    Backup: {backup_name}")
            if archivo['errores']:
                for error in archivo['errores']:
                    print(f"    ERROR: {error}")


def validate_files(ruta_csv: str, ruta_excel: str) -> bool:
    """Valida que los archivos existan"""
    errors = []
    
    if not os.path.exists(ruta_csv):
        errors.append(f"No se encuentra el archivo CSV con rutas: {ruta_csv}")
    
    if not os.path.exists(ruta_excel):
        errors.append(f"No se encuentra el archivo Excel/CSV: {ruta_excel}")
    
    if errors:
        logger.error("Errores de validación de archivos:")
        for error in errors:
            logger.error(f"  - {error}")
        return False
    
    return True


def main():
    """Función principal mejorada"""
    
    print("="*80)
    print("PROCESADOR MÚLTIPLE DE CÓDIGOS DE DIAGNÓSTICO RIPS")
    print("IMPORTANTE: ESTE PROCESO REEMPLAZARÁ LOS ARCHIVOS ORIGINALES")
    print("="*80)
    
    
    
    # Configurar rutas - AJUSTAR SEGÚN NECESIDAD
    #rutas_datos = input("\nIngrese la ruta del archivo CSV con las rutas de los JSON: ").strip()
    #ruta_excel = input("Ingrese la ruta del archivo Excel/CSV con diagnósticos: ").strip()


    #Remplaza para ejecución automatica 
    rutas_datos = r"../Proyect_Rips/Bases/Rutas_Json.csv"
    ruta_excel = r"../Proyect_Rips/Bases/RIPS_3.csv"

    logger.info("Iniciando procesamiento con REEMPLAZO de archivos originales...")
    logger.info(f"Archivo de rutas: {rutas_datos}")
    #logger.info(f"Archivo de diagnósticos: {ruta_excel}")
    
    try:
        # Verificar archivos
        if not validate_files(rutas_datos, ruta_excel):
            logger.error(" Validación de archivos fallida")
            return
        
        # Crear procesador
        processor = MultiDiagnosticCompleter()
        
        # Cargar rutas de archivos JSON desde CSV
        logger.info(" Cargando rutas de archivos JSON...")
        rutas_json = processor.load_json_paths(rutas_datos)
        
        if not rutas_json:
            logger.error(" No se encontraron archivos JSON válidos")
            return
        
        logger.info(f" Se encontraron {len(rutas_json)} archivos JSON para procesar")
        
        # Última confirmación antes de procesar
        print(f"\n  Se procesarán {len(rutas_json)} archivos JSON")
        print("  Los archivos ORIGINALES serán REEMPLAZADOS")
        print("  Se crearán backups automáticamente")
        
    
        
        # Cargar datos del Excel/CSV con diagnósticos
        logger.info(" Cargando datos de diagnósticos...")
        if not processor.load_excel_data(ruta_excel):
            logger.error(" Error cargando datos del Excel")
            return
        
        logger.info(f" Datos de diagnósticos cargados: {len(processor.diagnosticos_dict)} registros")
        
        # Procesar múltiples archivos JSON
        logger.info(" Iniciando procesamiento de archivos JSON...")
        if processor.process_multiple_jsons(rutas_json):
            logger.info(" Procesamiento completado con éxito")
            print("\n ¡PROCESAMIENTO COMPLETADO CON ÉXITO!")
            print(" Los archivos originales han sido actualizados")
            print(" Los backups están disponibles con extensión .backup")
        else:
            logger.error(" No se pudo procesar ningún archivo correctamente")
            print("\n ERROR: No se pudo procesar ningún archivo correctamente")
    
    except Exception as e:
        logger.error(f" Error crítico en main: {e}")
        logger.error(traceback.format_exc())
        print(f"\n ERROR CRÍTICO: {e}")
    
    finally:
        logger.info(" Proceso finalizado")
        print("\n Proceso finalizado. Revisa el archivo 'diagnostic_completion_debug.log' para más detalles.")


if __name__ == "__main__":
    main()