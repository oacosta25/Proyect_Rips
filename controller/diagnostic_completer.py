import pandas as pd
import os
import logging
import re
import json
from typing import Dict, Tuple, Optional, List, Any, Set

logger = logging.getLogger(__name__)

class DiagnosticCompleter:
    """Clase para completar códigos de diagnóstico principal desde Excel/CSV"""
    
    def __init__(self):
        self.diagnosticos_dict = {}
        self.codigos_a_eliminar = set()  # Nueva propiedad para almacenar códigos del CSV
        self.stats = {
            'usuarios_procesados': 0,
            'registros_procesados': 0,
            'cambios_realizados': 0,
            'diagnosticos_encontrados': 0,
            'cambios_diagnostico_relacionado': 0,
            'cambios_finalidad_tecnologia': 0,
            'cambios_tipo_documento': 0,
            'cambios_tipo_medicamento': 0,
            'cambios_modalidad_grupo': 0,
            'cambios_pais_residencia': 0,
            'cambios_tipo_documento_profesional': 0,  
            'cambios_num_documento_profesional': 0,
            'cambios_cod_consulta': 0,
            'cambios_tipo_diagnostico_principal': 0,
            'errores': []
        }
    
    def load_codigos_csv(self, ruta_codigos_csv: str) -> bool:
        """
        Carga los códigos que deben ser eliminados (cambiados a null) desde un archivo CSV
        
        Args:
            ruta_codigos_csv: Ruta al archivo CSV con los códigos a eliminar
            
        Returns:
            bool: True si se carga correctamente, False en caso contrario
        """
        try:
            logger.info(f"Cargando códigos a eliminar desde: {ruta_codigos_csv}")
            
            if not os.path.exists(ruta_codigos_csv):
                logger.warning(f"El archivo de códigos no existe: {ruta_codigos_csv}")
                logger.warning("Se continuará sin eliminar códigos de diagnóstico relacionado")
                return False
            
            # Intentar leer el CSV con diferentes configuraciones
            df_codigos = None
            separadores = [',', ';', '\t', '|']
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            
            for encoding in encodings:
                for sep in separadores:
                    try:
                        df_codigos = pd.read_csv(ruta_codigos_csv, sep=sep, encoding=encoding)
                        if len(df_codigos.columns) >= 1 and len(df_codigos) > 0:
                            logger.info(f"CSV de códigos leído exitosamente - Separador: '{sep}', Encoding: {encoding}")
                            break
                    except:
                        continue
                if df_codigos is not None:
                    break
            
            if df_codigos is None or df_codigos.empty:
                logger.warning("No se pudo leer el archivo de códigos o está vacío")
                return False
            
            # Limpiar nombres de columnas
            df_codigos.columns = df_codigos.columns.str.strip()
            logger.info(f"Columnas encontradas en CSV de códigos: {list(df_codigos.columns)}")
            
            # Buscar la columna "Codigos" (case insensitive)
            columna_codigos = None
            for col in df_codigos.columns:
                if col.lower() == 'codigos':
                    columna_codigos = col
                    break
            
            if columna_codigos is None:
                # Si no existe columna "Codigos", usar la primera columna
                columna_codigos = df_codigos.columns[0]
                logger.warning(f"No se encontró columna 'Codigos', usando primera columna: '{columna_codigos}'")
            
            # Extraer los códigos y limpiarlos
            codigos = df_codigos[columna_codigos].dropna().astype(str)
            
            # Limpiar códigos: eliminar espacios, \r, \n, BOM, etc.
            codigos_limpios = []
            for codigo in codigos:
                # Eliminar BOM, espacios, \r, \n, \t
                codigo_limpio = codigo.strip().replace('\r', '').replace('\n', '').replace('\t', '')
                # Eliminar BOM UTF-8 si existe
                codigo_limpio = codigo_limpio.replace('\ufeff', '').strip()
                
                # Filtrar valores vacíos o no válidos
                if (codigo_limpio != '' and 
                    codigo_limpio.lower() not in ['nan', 'null', 'none', 'codigos']):
                    codigos_limpios.append(codigo_limpio)
            
            # Convertir a set para búsqueda rápida
            self.codigos_a_eliminar = set(codigos_limpios)
            
            logger.info(f"Se cargaron {len(self.codigos_a_eliminar)} códigos para eliminar")
            logger.info(f"Primeros 10 códigos: {list(self.codigos_a_eliminar)[:10]}")
            
            # Debug: verificar si A15 y A18 están cargados
            if 'A15' in self.codigos_a_eliminar:
                logger.info("✓ Código 'A15' encontrado en la lista")
            else:
                logger.warning("⚠ Código 'A15' NO encontrado en la lista")
            
            if 'A18' in self.codigos_a_eliminar:
                logger.info("✓ Código 'A18' encontrado en la lista")
            else:
                logger.warning("⚠ Código 'A18' NO encontrado en la lista")
            
            return True
            
        except Exception as e:
            logger.error(f"Error cargando códigos desde CSV: {e}")
            logger.warning("Se continuará sin eliminar códigos de diagnóstico relacionado")
            return False
    
    def load_excel_data(self, ruta_excel: str) -> bool:
        """
        Carga y procesa los datos del archivo Excel/CSV
        
        Args:
            ruta_excel: Ruta al archivo Excel/CSV
            
        Returns:
            bool: True si se carga correctamente, False en caso contrario
        """
        try:
            logger.info(f"Cargando archivo: {ruta_excel}")
            
            # Cargar archivo
            df_excel = self._load_file(ruta_excel)
            if df_excel is None:
                return False
            
            # Limpiar nombres de columnas
            df_excel.columns = df_excel.columns.str.strip()
            
            logger.info(f"Columnas encontradas: {list(df_excel.columns)}")
            logger.info(f"Total de registros: {len(df_excel)}")
            
            # Mostrar muestra de datos
            logger.info("Muestra de datos:")
            logger.info(df_excel.head(3).to_string())
            
            # Identificar columnas necesarias
            col_mapping = self._identify_columns(df_excel.columns)
            
            if not all(col_mapping.values()):
                logger.error("No se encontraron todas las columnas necesarias")
                logger.error(f"Mapeo de columnas: {col_mapping}")
                logger.error(f"Columnas disponibles: {list(df_excel.columns)}")
                return False
            
            logger.info(f"Mapeo de columnas exitoso: {col_mapping}")
            
            # Crear diccionario de diagnósticos
            self._create_diagnostics_dict(df_excel, col_mapping)
            
            logger.info(f"Diccionario de diagnósticos creado con {len(self.diagnosticos_dict)} entradas")
            
            return True
            
        except Exception as e:
            logger.error(f"Error cargando Excel: {e}")
            self.stats['errores'].append(f"Error cargando Excel: {e}")
            return False
    
    def _load_file(self, ruta_archivo: str) -> Optional[pd.DataFrame]:
        """Carga archivo Excel o CSV"""
        
        if not os.path.exists(ruta_archivo):
            logger.error(f"El archivo no existe: {ruta_archivo}")
            return None
        
        # Intentar Excel primero
        if ruta_archivo.endswith(('.xlsx', '.xls')):
            try:
                df = pd.read_excel(ruta_archivo)
                logger.info("Archivo cargado como Excel")
                return df
            except Exception as e:
                logger.warning(f"Error leyendo como Excel: {e}")
        
        # Intentar CSV con diferentes configuraciones
        separadores = [';', ',', '\t', '|']
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            for sep in separadores:
                try:
                    df = pd.read_csv(ruta_archivo, sep=sep, encoding=encoding)
                    if len(df.columns) > 2:  # Verificar que se separó bien
                        logger.info(f"Archivo cargado como CSV - Separador: '{sep}', Encoding: {encoding}")
                        return df
                except:
                    continue
        
        logger.error("No se pudo cargar el archivo con ninguna configuración")
        return None
    
    def _identify_columns(self, columns: List[str]) -> Dict[str, Optional[str]]:
        """Identifica las columnas necesarias del archivo"""
        col_mapping = {
            'tipo_doc': None,
            'num_doc': None,
            'cod_diagnostico': None,
            'tipo_doc_profesional': None,
            'num_doc_profesional': None
        }
        
        # Mapeo de posibles nombres de columnas (case insensitive)
        column_patterns = {
            'tipo_doc': ['tipo.*doc.*identificacion', 'tipodocumento', 'tipo_doc'],
            'num_doc': ['num.*doc.*identificacion', 'numerodocumento', 'num_doc', 'documento'],
            'cod_diagnostico': ['cod.*diagnostico.*principal', 'codigodiagnostico', 'cod_diag', 'diagnostico'],
            'tipo_doc_profesional': ['tipo.*doc.*profesional', 'tipodocumentoprofesional'],
            'num_doc_profesional': ['num.*doc.*profesional', 'numerodocumentoprofesional']
        }
        
        for col in columns:
            col_lower = col.lower().strip()
            for key, patterns in column_patterns.items():
                if col_mapping[key] is None:  # Si no se ha encontrado aún
                    for pattern in patterns:
                        if re.search(pattern, col_lower):
                            col_mapping[key] = col
                            break
        
        return col_mapping
    
    def _create_diagnostics_dict(self, df: pd.DataFrame, col_mapping: Dict[str, str]):
        """Crea diccionario de diagnósticos desde el DataFrame"""
        for _, row in df.iterrows():
            tipo_doc = str(row[col_mapping['tipo_doc']]).strip().upper()
            num_doc = str(row[col_mapping['num_doc']]).strip()
            
            # Limpiar número de documento (solo dígitos)
            num_doc = re.sub(r'[^0-9]', '', num_doc)
            
            if tipo_doc and num_doc and tipo_doc not in ['NAN', 'NONE', '']:
                key = (tipo_doc, num_doc)
                
                self.diagnosticos_dict[key] = {
                    'cod_diagnostico': str(row[col_mapping['cod_diagnostico']]).strip(),
                    'tipo_doc_profesional': str(row[col_mapping['tipo_doc_profesional']]).strip() if col_mapping['tipo_doc_profesional'] else None,
                    'num_doc_profesional': str(row[col_mapping['num_doc_profesional']]).strip() if col_mapping['num_doc_profesional'] else None
                }
    
    def _process_user_level_changes(self, usuarios: List[dict]):
        """Procesa cambios a nivel de usuario"""
        logger.info("Procesando cambios a nivel de usuario...")
        
        for usuario in usuarios:
            # Cambiar tipoDocumentoIdentificacion
            tipo_doc_actual = str(usuario.get('tipoDocumentoIdentificacion', '')).strip().upper()
            
            if tipo_doc_actual in ['NI', '', '00', 'NULL', 'NONE']:
                usuario['tipoDocumentoIdentificacion'] = 'CC'
                self.stats['cambios_tipo_documento'] += 1
                logger.info(f"Usuario: tipoDocumentoIdentificacion cambiado de '{tipo_doc_actual}' a 'CC'")
            
            # Cambiar codPaisResidencia
            cod_pais_actual = str(usuario.get('codPaisResidencia', '')).strip()
            
            if cod_pais_actual in ['', '00', 'NULL', 'NONE'] or usuario.get('codPaisResidencia') is None:
                usuario['codPaisResidencia'] = '170'
                self.stats['cambios_pais_residencia'] += 1
                logger.info(f"Usuario: codPaisResidencia cambiado de '{cod_pais_actual}' a '170'")
            
            # Cambiar codPaisOrigen si es diferente a "170"
            if 'codPaisOrigen' in usuario:
                cod_pais_origen_actual = str(usuario.get('codPaisOrigen', '')).strip()
                
                if cod_pais_origen_actual != '170' and cod_pais_origen_actual != '':
                    usuario['codPaisOrigen'] = '170'
                    self.stats['cambios_pais_residencia'] += 1
                    logger.info(f"Usuario: codPaisOrigen cambiado de '{cod_pais_origen_actual}' a '170'")

    def _clean_numeric_field(self, value: str, field_name: str) -> str:
        """
        Limpia un campo para que solo contenga dígitos numéricos
        Elimina: puntos (.), guiones (-), espacios, letras, comas (,) y cualquier carácter no numérico
        
        Args:
            value: Valor original del campo
            field_name: Nombre del campo para logging
            
        Returns:
            str: Valor limpiado solo con dígitos
        """
        if value is None:
            return ''
        
        # Convertir a string y limpiar
        value_str = str(value).strip()
        
        # Si está vacío o es 'null', 'none', etc., retornar vacío
        if value_str.lower() in ['', 'null', 'none', 'nan', 'nat']:
            return ''
        
        # Extraer solo dígitos usando regex - elimina TODO excepto números (0-9)
        # Esto incluye: puntos (.), guiones (-), espacios, letras, comas (,), etc.
        numeric_only = re.sub(r'[^0-9]', '', value_str)
        
        # Si el valor cambió, registrar el cambio
        if numeric_only != value_str and numeric_only != '':
            logger.info(f"Campo {field_name} limpiado: '{value_str}' -> '{numeric_only}'")
            return numeric_only
        
        return value_str
    
    def _clean_cod_consulta(self, service: dict, service_name: str, index: int):
        """
        Limpia el campo codConsulta para que sea SOLO numérico
        Elimina puntos (.), guiones (-), letras, espacios y cualquier carácter no numérico
        
        Args:
            service: Diccionario del servicio
            service_name: Nombre del tipo de servicio
            index: Índice del servicio
        """
        if 'codConsulta' in service:
            cod_actual = service.get('codConsulta')
            
            if cod_actual is not None:
                cod_limpio = self._clean_numeric_field(cod_actual, f'{service_name}[{index}].codConsulta')
                
                # Solo actualizar si el valor cambió
                if str(cod_limpio) != str(cod_actual) and cod_limpio != '':
                    service['codConsulta'] = cod_limpio
                    self.stats['cambios_cod_consulta'] += 1
                    logger.info(f"      codConsulta limpiado: '{cod_actual}' -> '{cod_limpio}'")
    
    def _process_diagnostico_relacionado(self, service: dict, service_name: str, index: int):
        """
        Procesa los campos codDiagnosticoRelacionado1 y codDiagnosticoRelacionado2
        Cambia a null los códigos que estén en el conjunto de códigos a eliminar
        
        Args:
            service: Diccionario del servicio
            service_name: Nombre del tipo de servicio
            index: Índice del servicio
        """
        # Solo procesar si hay códigos cargados para eliminar
        if not self.codigos_a_eliminar:
            return
        
        # Procesar codDiagnosticoRelacionado1
        if 'codDiagnosticoRelacionado1' in service:
            codigo_actual = service.get('codDiagnosticoRelacionado1')
            
            if codigo_actual is not None:
                # Limpiar el código antes de comparar
                codigo_limpio = str(codigo_actual).strip().replace('\r', '').replace('\n', '')
                
                logger.debug(f"      Verificando codDiagnosticoRelacionado1: '{codigo_limpio}' contra {len(self.codigos_a_eliminar)} códigos")
                
                if codigo_limpio in self.codigos_a_eliminar:
                    service['codDiagnosticoRelacionado1'] = None
                    self.stats['cambios_diagnostico_relacionado'] += 1
                    logger.info(f"      ✓ codDiagnosticoRelacionado1 cambiado de '{codigo_limpio}' a null")
        
        # Procesar codDiagnosticoRelacionado2
        if 'codDiagnosticoRelacionado2' in service:
            codigo_actual = service.get('codDiagnosticoRelacionado2')
            
            if codigo_actual is not None:
                # Limpiar el código antes de comparar
                codigo_limpio = str(codigo_actual).strip().replace('\r', '').replace('\n', '')
                
                logger.debug(f"      Verificando codDiagnosticoRelacionado2: '{codigo_limpio}' contra {len(self.codigos_a_eliminar)} códigos")
                
                if codigo_limpio in self.codigos_a_eliminar:
                    service['codDiagnosticoRelacionado2'] = None
                    self.stats['cambios_diagnostico_relacionado'] += 1
                    logger.info(f"      ✓ codDiagnosticoRelacionado2 cambiado de '{codigo_limpio}' a null")
    
    def _process_service_list(self, services: List[dict], diagnostico_info: Dict, service_type: str):
        """Procesa una lista de servicios (consultas, procedimientos, medicamentos)"""
        logger.info(f"    Total de {service_type}: {len(services)}")
        
        for idx, service in enumerate(services):
            self.stats['registros_procesados'] += 1
            logger.info(f"    Procesando {service_type}[{idx}]...")
            
            # Limpiar codConsulta si existe
            self._clean_cod_consulta(service, service_type, idx)
            
            # Procesar diagnósticos relacionados (nueva funcionalidad)
            self._process_diagnostico_relacionado(service, service_type, idx)
            
            # Procesar finalidadTecnologiaSalud si existe
            if 'finalidadTecnologiaSalud' in service:
                finalidad_actual = str(service.get('finalidadTecnologiaSalud', '')).strip()
                if finalidad_actual in ['', '00', 'null', 'none'] or service['finalidadTecnologiaSalud'] is None:
                    service['finalidadTecnologiaSalud'] = '01'
                    self.stats['cambios_finalidad_tecnologia'] += 1
                    logger.info(f"      finalidadTecnologiaSalud cambiado a '01'")
            
            # Completar codDiagnosticoPrincipal si está vacío
            cod_diag = str(service.get('codDiagnosticoPrincipal', '')).strip()
            
            if (cod_diag == '' or 
                cod_diag.lower() in ['none', 'null', 'nan', 'nat'] or 
                cod_diag == '0'):
                
                if diagnostico_info and diagnostico_info.get('cod_diagnostico'):
                    service['codDiagnosticoPrincipal'] = diagnostico_info['cod_diagnostico']
                    self.stats['cambios_realizados'] += 1
                    self.stats['diagnosticos_encontrados'] += 1
                    logger.info(f"      codDiagnosticoPrincipal completado: {diagnostico_info['cod_diagnostico']}")
                else:
                    logger.warning(f"      codDiagnosticoPrincipal vacío pero no hay diagnóstico disponible")
            
            # Verificar y completar tipoDiagnosticoPrincipal
            tipo_diag = str(service.get('tipoDiagnosticoPrincipal', '')).strip()
            
            if tipo_diag in ['', '00', 'null', 'none'] or service.get('tipoDiagnosticoPrincipal') is None:
                service['tipoDiagnosticoPrincipal'] = '1'
                self.stats['cambios_tipo_diagnostico_principal'] += 1
                logger.info(f"      tipoDiagnosticoPrincipal cambiado a '1'")
            
            # Procesar tipoDocumentoIdentificacionProfesional si existe
            if 'tipoDocumentoIdentificacionProfesional' in service:
                tipo_doc_prof_actual = str(service.get('tipoDocumentoIdentificacionProfesional', '')).strip().upper()
                
                if tipo_doc_prof_actual in ['', '00', 'NULL', 'NONE', 'NI']:
                    nuevo_valor = diagnostico_info.get('tipo_doc_profesional', 'CC')
                    service['tipoDocumentoIdentificacionProfesional'] = nuevo_valor
                    self.stats['cambios_tipo_documento_profesional'] += 1
                    logger.info(f"      tipoDocumentoIdentificacionProfesional cambiado a '{nuevo_valor}'")
            
            # Procesar numDocumentoIdentificacionProfesional si existe
            if 'numDocumentoIdentificacionProfesional' in service:
                num_doc_prof_actual = str(service.get('numDocumentoIdentificacionProfesional', '')).strip()
                
                if num_doc_prof_actual in ['', '00', 'NULL', 'NONE', '0']:
                    nuevo_valor = diagnostico_info.get('num_doc_profesional', '0')
                    service['numDocumentoIdentificacionProfesional'] = nuevo_valor
                    self.stats['cambios_num_documento_profesional'] += 1
                    logger.info(f"      numDocumentoIdentificacionProfesional cambiado a '{nuevo_valor}'")
    
    def _process_other_services(self, other_services: List[dict], diagnostico_info: Dict):
        """Procesa la lista de otros servicios"""
        logger.info(f"    Total de otrosServicios: {len(other_services)}")
        
        for idx, service in enumerate(other_services):
            self.stats['registros_procesados'] += 1
            logger.info(f"    Procesando otrosServicios[{idx}]...")
            
            # Limpiar codConsulta si existe
            self._clean_cod_consulta(service, 'otrosServicios', idx)
            
            # Procesar diagnósticos relacionados (nueva funcionalidad)
            self._process_diagnostico_relacionado(service, 'otrosServicios', idx)
            
            # Completar codDiagnosticoPrincipal si está vacío
            cod_diag = str(service.get('codDiagnosticoPrincipal', '')).strip()
            
            if (cod_diag == '' or 
                cod_diag.lower() in ['none', 'null', 'nan', 'nat'] or 
                cod_diag == '0'):
                
                if diagnostico_info and diagnostico_info.get('cod_diagnostico'):
                    service['codDiagnosticoPrincipal'] = diagnostico_info['cod_diagnostico']
                    self.stats['cambios_realizados'] += 1
                    self.stats['diagnosticos_encontrados'] += 1
                    logger.info(f"      codDiagnosticoPrincipal completado: {diagnostico_info['cod_diagnostico']}")
                else:
                    logger.warning(f"      codDiagnosticoPrincipal vacío pero no hay diagnóstico disponible")
            
            # Verificar y completar tipoDiagnosticoPrincipal
            tipo_diag = str(service.get('tipoDiagnosticoPrincipal', '')).strip()
            
            if tipo_diag in ['', '00', 'null', 'none'] or service.get('tipoDiagnosticoPrincipal') is None:
                service['tipoDiagnosticoPrincipal'] = '1'
                self.stats['cambios_tipo_diagnostico_principal'] += 1
                logger.info(f"      tipoDiagnosticoPrincipal cambiado a '1'")
            
            if 'tipoMedicamento' in service:
                tipo_med_actual = str(service.get('tipoMedicamento', '')).strip()
                if tipo_med_actual in ['', '00', 'null', 'none'] or service['tipoMedicamento'] is None:
                    service['tipoMedicamento'] = '01'
                    self.stats['cambios_tipo_medicamento'] += 1
                    logger.info(f"      tipoMedicamento cambiado a '01'")
            
            if 'modalidadGrupoServicioTecSal' in service:
                modalidad_actual = str(service.get('modalidadGrupoServicioTecSal', '')).strip()
                if modalidad_actual in ['', '00', 'null', 'none'] or service['modalidadGrupoServicioTecSal'] is None:
                    service['modalidadGrupoServicioTecSal'] = '01'
                    self.stats['cambios_modalidad_grupo'] += 1
                    logger.info(f"      modalidadGrupoServicioTecSal cambiado a '01'")

    def debug_matching(self):
        """Función para debuggear el matching de datos"""
        logger.info("=== DEBUG: Análisis de coincidencias ===")
        
        if not self.diagnosticos_dict:
            logger.warning("No hay diagnósticos cargados para mostrar")
            return
        
        # Mostrar algunos números de documento del CSV
        logger.info("Números de documento en CSV (primeros 10):")
        for i, (key, diag_info) in enumerate(list(self.diagnosticos_dict.items())[:10]):
            logger.info(f"  {key[0]} - {key[1]} -> {diag_info}")
        
        logger.info("=== Fin DEBUG ===")
    
    def get_stats(self) -> Dict[str, Any]:
        """Obtiene las estadísticas del proceso"""
        return self.stats.copy()
    
    def process_json_file(self, json_input_path: str, json_output_path: str = None) -> bool:
        """
        Procesa un archivo JSON completo aplicando todos los cambios
        
        Args:
            json_input_path: Ruta al archivo JSON de entrada
            json_output_path: Ruta al archivo JSON de salida (opcional, por defecto agrega _modificado)
            
        Returns:
            bool: True si se procesó correctamente, False en caso contrario
        """
        try:
            logger.info(f"{'='*80}")
            logger.info(f"PROCESANDO ARCHIVO JSON")
            logger.info(f"{'='*80}")
            logger.info(f"Archivo de entrada: {json_input_path}")
            
            # Cargar JSON
            with open(json_input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"JSON cargado exitosamente")
            logger.info(f"Estructura del JSON: {list(data.keys())}")
            
            # Verificar que tenga usuarios
            if 'usuarios' not in data:
                logger.error("El JSON no tiene la clave 'usuarios'")
                return False
            
            usuarios = data['usuarios']
            logger.info(f"Total de usuarios a procesar: {len(usuarios)}")
            
            # Procesar cambios a nivel de usuario (tipo documento y país residencia)
            self._process_user_level_changes(usuarios)
            
            # Procesar cada usuario
            for idx, usuario in enumerate(usuarios, 1):
                logger.info(f"\n{'='*70}")
                logger.info(f"PROCESANDO USUARIO {idx}/{len(usuarios)}")
                logger.info(f"{'='*70}")
                
                self.stats['usuarios_procesados'] += 1
                
                # Mostrar información del usuario
                tipo_doc = usuario.get('tipoDocumentoIdentificacion', 'N/A')
                num_doc = usuario.get('numDocumentoIdentificacion', 'N/A')
                logger.info(f"Documento: {tipo_doc} - {num_doc}")
                
                # Obtener servicios
                if 'servicios' not in usuario:
                    logger.warning(f"Usuario {idx} no tiene servicios")
                    continue
                
                servicios = usuario['servicios']
                logger.info(f"Secciones de servicios: {list(servicios.keys())}")
                
                # Buscar diagnóstico en el diccionario
                tipo_doc_clean = str(tipo_doc).strip().upper()
                num_doc_clean = re.sub(r'[^0-9]', '', str(num_doc))
                key = (tipo_doc_clean, num_doc_clean)
                
                diagnostico_info = self.diagnosticos_dict.get(key, {
                    'cod_diagnostico': None,
                    'tipo_doc_profesional': None,
                    'num_doc_profesional': None
                })
                
                # Procesar consultas, procedimientos, medicamentos
                service_types = ['consultas', 'procedimientos', 'medicamentos']
                for service_type in service_types:
                    if service_type in servicios and servicios[service_type]:
                        logger.info(f"\n  Procesando {service_type}...")
                        self._process_service_list(
                            servicios[service_type], 
                            diagnostico_info, 
                            service_type
                        )
                
                # Procesar otrosServicios
                if 'otrosServicios' in servicios and servicios['otrosServicios']:
                    logger.info(f"\n  Procesando otrosServicios...")
                    self._process_other_services(
                        servicios['otrosServicios'], 
                        diagnostico_info
                    )
            
            # Determinar ruta de salida
            if json_output_path is None:
                base_name = os.path.splitext(json_input_path)[0]
                json_output_path = f"{base_name}_modificado.json"
            
            # Guardar JSON modificado
            with open(json_output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"\n{'='*80}")
            logger.info(f"PROCESO COMPLETADO")
            logger.info(f"{'='*80}")
            logger.info(f"Archivo de salida: {json_output_path}")
            
            return True
            
        except FileNotFoundError:
            logger.error(f"Archivo no encontrado: {json_input_path}")
            self.stats['errores'].append(f"Archivo no encontrado: {json_input_path}")
            return False
        except json.JSONDecodeError as e:
            logger.error(f"Error al decodificar JSON: {e}")
            self.stats['errores'].append(f"Error al decodificar JSON: {e}")
            return False
        except Exception as e:
            logger.error(f"Error procesando JSON: {e}")
            self.stats['errores'].append(f"Error procesando JSON: {e}")
            return False

    def print_summary(self):
        """Imprime resumen del proceso"""
        print(f"\n{'='*60}")
        print(f"RESUMEN DEL PROCESO")
        print(f"{'='*60}")
      
        if hasattr(self, 'archivos_stats'):
            for i, archivo in enumerate(self.archivos_stats, 1):
                status = "✓ EXITOSO" if archivo['procesado'] else "✗ FALLIDO"
                nombre = os.path.basename(archivo['ruta'])
                print(f"{i:2d}. {status} - {nombre}")
                print(f"    Usuarios: {archivo['usuarios_procesados']}, " +
                      f"Registros: {archivo['registros_procesados']}, " +
                      f"Cambios: {archivo['cambios_realizados']}")
                if archivo['errores']:
                    for error in archivo['errores']:
                        print(f"    ERROR: {error}")
        
        # Imprimir estadísticas detalladas
        print(f"\nESTADÍSTICAS DETALLADAS:")
        print(f"- Usuarios procesados: {self.stats['usuarios_procesados']}")
        print(f"- Registros procesados: {self.stats['registros_procesados']}")
        print(f"- Cambios realizados: {self.stats['cambios_realizados']}")
        print(f"- Diagnósticos encontrados: {self.stats['diagnosticos_encontrados']}")
        print(f"- Cambios diagnósticos relacionados: {self.stats['cambios_diagnostico_relacionado']}")
        print(f"- Cambios codConsulta: {self.stats['cambios_cod_consulta']}")
        print(f"- Cambios tipoDiagnosticoPrincipal: {self.stats['cambios_tipo_diagnostico_principal']}")
        print(f"- Cambios tipo documento: {self.stats['cambios_tipo_documento']}")
        print(f"- Cambios país residencia: {self.stats['cambios_pais_residencia']}")
        
        if self.stats['errores']:
            print(f"\nERRORES:")
            for error in self.stats['errores']:
                print(f"  - {error}")