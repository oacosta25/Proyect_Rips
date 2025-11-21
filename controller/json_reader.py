import json
import logging
import os
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class JsonReader:
    """Clase para leer y validar archivos JSON"""
    
    def __init__(self):
        self.data = None
        self.file_path = None
    
    def load_json(self, ruta_json: str) -> Optional[Dict[str, Any]]:
        """
        Carga un archivo JSON
        
        Args:
            ruta_json: Ruta al archivo JSON
            
        Returns:
            dict: Datos del JSON si se carga correctamente, None en caso contrario
        """
        try:
            logger.info(f"Cargando archivo JSON: {ruta_json}")
            
            # Verificar que el archivo existe
            if not os.path.exists(ruta_json):
                logger.error(f"El archivo no existe: {ruta_json}")
                return None
            
            # Verificar que es un archivo JSON
            if not ruta_json.lower().endswith('.json'):
                logger.warning(f"El archivo no tiene extensión .json: {ruta_json}")
            
            # Verificar tamaño del archivo
            file_size = os.path.getsize(ruta_json)
            logger.info(f"Tamaño del archivo: {file_size} bytes")
            
            if file_size == 0:
                logger.error(f"El archivo está vacío: {ruta_json}")
                return None
            
            # Intentar diferentes encodings
            encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
            
            for encoding in encodings:
                try:
                    logger.debug(f"Intentando cargar con encoding: {encoding}")
                    
                    with open(ruta_json, 'r', encoding=encoding) as file:
                        # Leer contenido completo
                        content = file.read()
                        logger.debug(f"Contenido leído, longitud: {len(content)} caracteres")
                        
                        # Verificar que no esté vacío
                        if not content.strip():
                            logger.error(f"El archivo está vacío o solo contiene espacios")
                            return None
                        
                        # Mostrar muestra del contenido para debug
                        logger.debug(f"Primeros 200 caracteres: {content[:200]}")
                        
                        # Parsear JSON
                        self.data = json.loads(content)
                        logger.info(f"JSON parseado exitosamente con encoding: {encoding}")
                        break
                        
                except UnicodeDecodeError:
                    logger.debug(f"Error de encoding con {encoding}, probando siguiente")
                    continue
                except json.JSONDecodeError as e:
                    logger.error(f"Error decodificando JSON con {encoding}: {e}")
                    logger.error(f"Línea {e.lineno}, Columna {e.colno}: {e.msg}")
                    return None
            else:
                logger.error("No se pudo cargar el archivo con ningún encoding")
                return None
            
            self.file_path = ruta_json
            
            # Validar estructura básica
            if not self._validate_structure():
                logger.error("Estructura del JSON no válida")
                return None
            
            self._log_json_info()
            
            logger.info("Archivo JSON cargado exitosamente")
            return self.data
            
        except FileNotFoundError:
            logger.error(f"Archivo no encontrado: {ruta_json}")
            return None
        except PermissionError:
            logger.error(f"Sin permisos para leer el archivo: {ruta_json}")
            return None
        except Exception as e:
            logger.error(f"Error inesperado cargando JSON: {e}")
            logger.error(f"Tipo de error: {type(e).__name__}")
            return None
    
    def _validate_structure(self) -> bool:
        """Valida que el JSON tenga la estructura esperada"""
        try:
            # Verificar tipo de datos principal
            if not isinstance(self.data, dict):
                logger.error(f"El JSON debe ser un objeto, recibido: {type(self.data)}")
                return False
            
            # Verificar claves principales
            logger.info(f"Claves principales del JSON: {list(self.data.keys())}")
            
            if 'usuarios' not in self.data:
                logger.error("El JSON debe contener una clave 'usuarios'")
                return False
            
            usuarios = self.data.get('usuarios', [])
            
            # Verificar tipo de usuarios
            if not isinstance(usuarios, list):
                logger.error(f"'usuarios' debe ser una lista, recibido: {type(usuarios)}")
                return False
            
            # Verificar estructura de algunos usuarios
            if usuarios:
                logger.info("Validando estructura de usuarios...")
                for i, usuario in enumerate(usuarios[:3]):  # Validar primeros 3
                    if not isinstance(usuario, dict):
                        logger.error(f"Usuario {i+1} no es un diccionario: {type(usuario)}")
                        return False
                    
                    # Verificar campos básicos
                    campos_requeridos = ['tipoDocumentoIdentificacion', 'numDocumentoIdentificacion']
                    for campo in campos_requeridos:
                        if campo not in usuario:
                            logger.warning(f"Usuario {i+1} no tiene campo '{campo}'")
                    
                    # Verificar servicios si existen
                    if 'servicios' in usuario:
                        servicios = usuario['servicios']
                        if not isinstance(servicios, dict):
                            logger.error(f"Usuario {i+1}: 'servicios' debe ser un diccionario")
                            return False
                        
                        # Verificar tipos de servicios
                        tipos_servicios = ['consultas', 'procedimientos', 'medicamentos', 'otrosServicios']
                        for tipo in tipos_servicios:
                            if tipo in servicios:
                                if not isinstance(servicios[tipo], list):
                                    logger.error(f"Usuario {i+1}: '{tipo}' debe ser una lista")
                                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validando estructura: {e}")
            return False
    
    def _log_json_info(self):
        """Registra información sobre el JSON cargado"""
        try:
            usuarios = self.data.get('usuarios', [])
            logger.info(f"Total de usuarios: {len(usuarios)}")
            
            # Contar servicios totales
            total_servicios = 0
            servicios_por_tipo = {
                'consultas': 0,
                'procedimientos': 0,
                'medicamentos': 0,
                'otrosServicios': 0
            }
            
            for usuario in usuarios:
                servicios = usuario.get('servicios', {})
                if isinstance(servicios, dict):
                    for tipo in servicios_por_tipo.keys():
                        servicios_tipo = servicios.get(tipo, [])
                        if isinstance(servicios_tipo, list):
                            count = len(servicios_tipo)
                            servicios_por_tipo[tipo] += count
                            total_servicios += count
            
            logger.info(f"Total de servicios: {total_servicios}")
            logger.info(f"Servicios por tipo: {servicios_por_tipo}")
            
            # Mostrar muestra de usuarios
            if usuarios:
                logger.info("Muestra de usuarios:")
                for i, usuario in enumerate(usuarios[:3]):
                    if isinstance(usuario, dict):
                        tipo_doc = usuario.get('tipoDocumentoIdentificacion', 'N/A')
                        num_doc = usuario.get('numDocumentoIdentificacion', 'N/A')
                        logger.info(f"  Usuario {i+1}: {tipo_doc} - {num_doc}")
                    else:
                        logger.warning(f"  Usuario {i+1}: Estructura inválida")
                        
        except Exception as e:
            logger.error(f"Error registrando información del JSON: {e}")
    
    def save_json(self, data: Dict[str, Any], ruta_salida: Optional[str] = None) -> bool:
        """
        Guarda datos en un archivo JSON
        
        Args:
            data: Datos a guardar
            ruta_salida: Ruta donde guardar el archivo. Si es None, usa self.file_path
            
        Returns:
            bool: True si se guarda correctamente, False en caso contrario
        """
        try:
            # Si no se especifica ruta, usar la ruta del archivo cargado
            if ruta_salida is None:
                if self.file_path is None:
                    logger.error("No hay archivo cargado y no se especificó ruta de salida")
                    return False
                ruta_salida = self.file_path
                logger.info(f"Guardando en el archivo original: {ruta_salida}")
            
            logger.info(f"Guardando archivo JSON en: {ruta_salida}")
            
            # Validar datos de entrada
            if not isinstance(data, dict):
                logger.error(f"Los datos deben ser un diccionario, recibido: {type(data)}")
                return False
            
            # Crear directorio si no existe
            dir_salida = os.path.dirname(ruta_salida)
            if dir_salida and not os.path.exists(dir_salida):
                try:
                    os.makedirs(dir_salida, exist_ok=True)
                    logger.info(f"Directorio creado: {dir_salida}")
                except Exception as e:
                    logger.error(f"Error creando directorio {dir_salida}: {e}")
                    return False
            
            # # Crear backup si el archivo ya existe
            # if os.path.exists(ruta_salida):
            #     backup_path = f"{ruta_salida}.backup"
            #     try:
            #         os.rename(ruta_salida, backup_path)
            #         logger.info(f"Backup creado: {backup_path}")
            #     except Exception as e:
            #         logger.warning(f"No se pudo crear backup: {e}")
            
            # Guardar archivo
            with open(ruta_salida, 'w', encoding='utf-8') as file:
                json.dump(data, file, indent=2, ensure_ascii=False, separators=(',', ': '))
            
            # Verificar que se guardó correctamente
            if os.path.exists(ruta_salida):
                file_size = os.path.getsize(ruta_salida)
                logger.info(f"Archivo guardado exitosamente - Tamaño: {file_size} bytes")
                
                # Verificar que se puede leer
                try:
                    with open(ruta_salida, 'r', encoding='utf-8') as file:
                        test_data = json.load(file)
                    logger.info("Verificación de lectura exitosa")
                except Exception as e:
                    logger.error(f"Error verificando archivo guardado: {e}")
                    return False
                    
                return True
            else:
                logger.error("El archivo no se guardó correctamente")
                return False
            
        except Exception as e:
            logger.error(f"Error guardando archivo JSON: {e}")
            return False
    
    def save_current_data(self) -> bool:
        """
        Guarda los datos actuales en el archivo original
        
        Returns:
            bool: True si se guarda correctamente, False en caso contrario
        """
        if self.data is None:
            logger.error("No hay datos cargados para guardar")
            return False
        
        if self.file_path is None:
            logger.error("No hay archivo original cargado")
            return False
        
        logger.info("Guardando datos modificados en el archivo original")
        return self.save_json(self.data)
    
    def update_data(self, new_data: Dict[str, Any], auto_save: bool = False) -> bool:
        """
        Actualiza los datos en memoria y opcionalmente los guarda
        
        Args:
            new_data: Nuevos datos para actualizar
            auto_save: Si True, guarda automáticamente después de actualizar
            
        Returns:
            bool: True si la actualización fue exitosa
        """
        try:
            if not isinstance(new_data, dict):
                logger.error(f"Los nuevos datos deben ser un diccionario, recibido: {type(new_data)}")
                return False
            
            # Crear backup de los datos actuales
            data_backup = self.data.copy() if self.data else None
            
            # Actualizar datos
            self.data = new_data
            logger.info("Datos actualizados en memoria")
            
            # Validar nueva estructura
            if not self._validate_structure():
                logger.error("La nueva estructura no es válida, revirtiendo cambios")
                self.data = data_backup
                return False
            
            # Guardar automáticamente si se solicita
            if auto_save:
                if not self.save_current_data():
                    logger.error("Error guardando datos, revirtiendo cambios")
                    self.data = data_backup
                    return False
                logger.info("Datos guardados automáticamente")
            
            return True
            
        except Exception as e:
            logger.error(f"Error actualizando datos: {e}")
            return False
    
    def has_unsaved_changes(self) -> bool:
        """
        Verifica si hay cambios sin guardar comparando con el archivo original
        
        Returns:
            bool: True si hay cambios sin guardar
        """
        if self.file_path is None or self.data is None:
            return False
        
        try:
            # Cargar datos del archivo original
            with open(self.file_path, 'r', encoding='utf-8') as file:
                original_data = json.load(file)
            
            # Comparar con datos actuales
            return self.data != original_data
            
        except Exception as e:
            logger.error(f"Error verificando cambios: {e}")
            return False
    
    def get_users_info(self) -> Dict[str, Any]:
        """
        Obtiene información resumida de los usuarios
        
        Returns:
            dict: Información de los usuarios
        """
        if not self.data:
            return {}
        
        try:
            usuarios = self.data.get('usuarios', [])
            info = {
                'total_usuarios': len(usuarios),
                'usuarios_con_servicios': 0,
                'servicios_por_tipo': {
                    'consultas': 0,
                    'procedimientos': 0,
                    'medicamentos': 0,
                    'otrosServicios': 0
                },
                'diagnosticos_vacios': 0,
                'total_servicios': 0,
                'usuarios_validos': 0,
                'usuarios_invalidos': 0
            }
            
            for usuario in usuarios:
                if not isinstance(usuario, dict):
                    info['usuarios_invalidos'] += 1
                    continue
                
                info['usuarios_validos'] += 1
                servicios = usuario.get('servicios', {})
                
                if not isinstance(servicios, dict):
                    continue
                    
                tiene_servicios = False
                
                for tipo_servicio in info['servicios_por_tipo'].keys():
                    servicios_tipo = servicios.get(tipo_servicio, [])
                    if isinstance(servicios_tipo, list) and servicios_tipo:
                        tiene_servicios = True
                        info['servicios_por_tipo'][tipo_servicio] += len(servicios_tipo)
                        info['total_servicios'] += len(servicios_tipo)
                        
                        # Contar diagnósticos vacíos
                        for servicio in servicios_tipo:
                            if isinstance(servicio, dict):
                                cod_diag = str(servicio.get('codDiagnosticoPrincipal', '')).strip()
                                if (cod_diag == '' or 
                                    cod_diag.lower() in ['none', 'null', 'nan', 'nat'] or
                                    cod_diag == '0'):
                                    info['diagnosticos_vacios'] += 1
                
                if tiene_servicios:
                    info['usuarios_con_servicios'] += 1
            
            return info
            
        except Exception as e:
            logger.error(f"Error obteniendo información de usuarios: {e}")
            return {}
    
    def validate_json_file(self, ruta_json: str) -> Dict[str, Any]:
        """
        Valida un archivo JSON sin cargarlo completamente
        
        Args:
            ruta_json: Ruta al archivo JSON
            
        Returns:
            dict: Información de validación
        """
        validation_info = {
            'valid': False,
            'errors': [],
            'warnings': [],
            'file_size': 0,
            'structure_valid': False
        }
        
        try:
            # Verificar existencia
            if not os.path.exists(ruta_json):
                validation_info['errors'].append(f"Archivo no existe: {ruta_json}")
                return validation_info
            
            # Verificar tamaño
            validation_info['file_size'] = os.path.getsize(ruta_json)
            if validation_info['file_size'] == 0:
                validation_info['errors'].append("Archivo vacío")
                return validation_info
            
            # Intentar parsear
            temp_data = self.load_json(ruta_json)
            if temp_data is None:
                validation_info['errors'].append("No se pudo parsear el JSON")
                return validation_info
            
            validation_info['structure_valid'] = True
            validation_info['valid'] = True
            
        except Exception as e:
            validation_info['errors'].append(f"Error validando: {e}")
        
        return validation_info