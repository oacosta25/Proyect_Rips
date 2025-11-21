import pandas as pd
import os
import logging
import re
import json
from typing import Dict, Tuple, Optional, List, Any

logger = logging.getLogger(__name__)

class DiagnosticCompleter:
    """Clase para completar códigos de diagnóstico principal desde Excel/CSV"""
    
    def __init__(self):
        self.diagnosticos_dict = {}
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
            'cambios_tipo_diagnostico_principal': 0,  # Nuevo contador
            'errores': []
        }
    
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
        if 'codConsulta' not in service:
            return
        
        original_value = service['codConsulta']
        
        # Validar que existe el campo y no está vacío
        if original_value is None:
            logger.debug(f"      codConsulta en {service_name} {index+1} es None, se mantiene")
            return
            
        # Convertir a string para procesamiento
        original_str = str(original_value).strip()
        
        # Si está vacío o es valor nulo, mantener como está
        if original_str.lower() in ['', 'null', 'none', 'nan', 'nat']:
            logger.debug(f"      codConsulta en {service_name} {index+1} está vacío, se mantiene")
            return
        
        # LIMPIEZA ESPECÍFICA: Eliminar TODO excepto números (0-9)
        # Esto incluye puntos (.), guiones (-), espacios, letras, comas (,), etc.
        cleaned_value = re.sub(r'[^0-9]', '', original_str)
        
        # Log específico para detección de puntos
        if '.' in original_str:
            logger.info(f"        PUNTO DETECTADO y ELIMINADO en codConsulta: '{original_str}' -> '{cleaned_value}' en {service_name} {index+1}")
        
        # Validar que después de la limpieza quedó algo
        if cleaned_value == '':
            logger.warning(f"         codConsulta en {service_name} {index+1} no contiene números válidos: '{original_value}' -> se deja vacío")
            service['codConsulta'] = ''
            self.stats['cambios_cod_consulta'] += 1
            return
        
        # Actualizar el valor SIEMPRE (forzar actualización)
        service['codConsulta'] = cleaned_value
        
        # Registrar cambios si el valor era diferente
        if original_str != cleaned_value:
            self.stats['cambios_cod_consulta'] += 1
            logger.info(f"      codConsulta LIMPIADO: '{original_value}' -> '{cleaned_value}' en {service_name} {index+1}")
            
            # Log específico para diferentes tipos de servicios
            if service_name == 'consultas':
                logger.info(f"         CONSULTA: Código '{original_value}' -> '{cleaned_value}'")
            elif service_name == 'procedimientos':
                logger.info(f"         PROCEDIMIENTO: Código '{original_value}' -> '{cleaned_value}'")
            elif service_name == 'medicamentos':
                logger.info(f"         MEDICAMENTO: Código '{original_value}' -> '{cleaned_value}'")
            elif service_name == 'otros servicios':
                logger.info(f"          OTRO SERVICIO: Código '{original_value}' -> '{cleaned_value}'")
        else:
            logger.debug(f"      ✓ codConsulta ya estaba limpio: '{cleaned_value}' en {service_name} {index+1}")

    def _clean_all_cod_consulta_fields(self, usuario: dict):
        """
        Función auxiliar para limpiar TODOS los campos codConsulta de un usuario
        Procesa consultas, procedimientos, medicamentos y otrosServicios
        
        Args:
            usuario: Diccionario del usuario con sus servicios
        """
        logger.info(f"   Limpiando TODOS los campos codConsulta del usuario...")
        
        servicios = usuario.get('servicios', {})
        
        # Procesar consultas
        if 'consultas' in servicios and servicios['consultas']:
            logger.info(f"    Limpiando codConsulta en {len(servicios['consultas'])} consultas...")
            for i, consulta in enumerate(servicios['consultas']):
                self._clean_cod_consulta(consulta, 'consultas', i)
        
        # Procesar procedimientos
        if 'procedimientos' in servicios and servicios['procedimientos']:
            logger.info(f"    Limpiando codConsulta en {len(servicios['procedimientos'])} procedimientos...")
            for i, procedimiento in enumerate(servicios['procedimientos']):
                self._clean_cod_consulta(procedimiento, 'procedimientos', i)
        
        # Procesar medicamentos
        if 'medicamentos' in servicios and servicios['medicamentos']:
            logger.info(f"    Limpiando codConsulta en {len(servicios['medicamentos'])} medicamentos...")
            for i, medicamento in enumerate(servicios['medicamentos']):
                self._clean_cod_consulta(medicamento, 'medicamentos', i)
        
        # Procesar otros servicios
        if 'otrosServicios' in servicios and servicios['otrosServicios']:
            logger.info(f"    Limpiando codConsulta en {len(servicios['otrosServicios'])} otros servicios...")
            for i, otro_servicio in enumerate(servicios['otrosServicios']):
                self._clean_cod_consulta(otro_servicio, 'otros servicios', i)

    def complete_diagnostics(self, json_data: Dict[str, Any]) -> bool:
        """
        Completa los diagnósticos faltantes en el JSON
        
        Args:
            json_data: Datos del JSON cargado
            
        Returns:
            bool: True si se completó correctamente
        """
        try:
            usuarios = json_data.get('usuarios', [])
            logger.info(f"Procesando {len(usuarios)} usuarios...")
            
            # Procesar cambios a nivel de usuario primero
            self._process_user_level_changes(usuarios)
            
            for usuario in usuarios:
                self.stats['usuarios_procesados'] += 1
                self._process_user(usuario)
            
            return True
            
        except Exception as e:
            logger.error(f"Error completando diagnósticos: {e}")
            self.stats['errores'].append(f"Error completando diagnósticos: {e}")
            return False
    
    def _process_user(self, usuario: dict):
        """Procesa un usuario individual - VERSIÓN CORREGIDA Y OPTIMIZADA"""
        tipo_doc = str(usuario.get('tipoDocumentoIdentificacion', '')).strip().upper()
        num_doc = str(usuario.get('numDocumentoIdentificacion', '')).strip()
        
        # Limpiar número de documento
        num_doc = num_doc.replace('.', '').replace('-', '').replace(' ', '').replace(',', '')
        
        logger.info(f"Procesando usuario: {tipo_doc} - {num_doc}")
        
        # PRIMERO: Limpiar TODOS los campos codConsulta
        self._clean_all_cod_consulta_fields(usuario)
        
        # Buscar diagnóstico para este usuario
        diagnostico_info = self._find_diagnostic(tipo_doc, num_doc)
        
        if not diagnostico_info:
            logger.warning(f"No se encontró diagnóstico para usuario: {tipo_doc} - {num_doc}")
            logger.warning(f"Se aplicarán cambios estándar (NI->CC, codPaisResidencia, etc.) sin completar diagnósticos")
            # Crear diagnostico_info vacío para permitir procesamiento de otros cambios
            diagnostico_info = {
                'cod_diagnostico': None,
                'tipo_doc_profesional': None,
                'num_doc_profesional': None
            }
        else:
            logger.info(f"Diagnóstico encontrado: {diagnostico_info['cod_diagnostico']}")
            self.stats['diagnosticos_encontrados'] += 1
        
        servicios = usuario.get('servicios', {})
        
        # Procesar consultas, procedimientos y medicamentos
        service_types = ['consultas', 'procedimientos', 'medicamentos']
        
        for service_type in service_types:
            if service_type in servicios:
                logger.info(f"  Procesando {service_type}...")
                self._process_service_list(
                    servicios[service_type], 
                    diagnostico_info, 
                    service_type
                )
        
        # Procesar otros servicios (se procesa por separado con su propio método)
        if 'otrosServicios' in servicios:
            logger.info(f"  Procesando otrosServicios...")
            self._process_other_services(
                servicios['otrosServicios'], 
                diagnostico_info
            )
    
    def _identify_columns(self, columns: pd.Index) -> Dict[str, Optional[str]]:
        """Identifica las columnas necesarias en el DataFrame"""
        
        # Patrones para cada columna (más flexible)
        column_patterns = {
            'tipo_doc': [
                'TipoDocumentoPaciente', 'tipodocumentopaciente', 'tipo_documento_paciente'
            ],
            'num_doc': [
                'NumeroDocumentoPaciente', 'numerodocumentopaciente', 'numero_documento_paciente'
            ],
            'cod_diag': [
                'CodDiagnostico', 'coddiagnostico', 'codigo_diagnostico'
            ],
            'tipo_doc_profesional': [
                'TipoDocumentoProfesional', 'tipodocumentoprofesional', 'tipo_documento_profesional'
            ],
            'num_doc_profesional': [
                'numDocumentoIdentificacion', 'numdocumentoidentificacion', 'numero_documento_profesional'
            ]
        }
        
        col_mapping = {
            'tipo_doc': None, 
            'num_doc': None, 
            'cod_diag': None,
            'tipo_doc_profesional': None, 
            'num_doc_profesional': None
        }
        
        # Buscar coincidencias exactas primero
        for col in columns:
            col_clean = col.lower().strip().replace(' ', '').replace('_', '')
            
            for key, patterns in column_patterns.items():
                if col_mapping[key] is None:
                    for pattern in patterns:
                        pattern_clean = pattern.lower().replace('_', '')
                        if col_clean == pattern_clean:
                            col_mapping[key] = col
                            logger.info(f"Columna '{key}' mapeada a '{col}' (coincidencia exacta)")
                            break
        
        # Buscar coincidencias parciales si no se encontraron exactas
        for col in columns:
            col_clean = col.lower().strip()
            
            for key, patterns in column_patterns.items():
                if col_mapping[key] is None:
                    for pattern in patterns:
                        if pattern.lower() in col_clean or col_clean in pattern.lower():
                            col_mapping[key] = col
                            logger.info(f"Columna '{key}' mapeada a '{col}' (coincidencia parcial)")
                            break
        
        return col_mapping
    
    def _create_diagnostics_dict(self, df: pd.DataFrame, col_mapping: Dict[str, str]):
        """Crea el diccionario de diagnósticos desde el DataFrame"""
        
        logger.info("Creando diccionario de diagnósticos...")
        diagnosticos_por_paciente = {}
        
        for index, row in df.iterrows():
            try:
                tipo_doc = str(row.get(col_mapping['tipo_doc'], '')).strip().upper()
                num_doc = str(row.get(col_mapping['num_doc'], '')).strip()
                cod_diag = str(row.get(col_mapping['cod_diag'], '')).strip().upper()
                
                # Datos del profesional (si existen las columnas)
                tipo_doc_profesional = None
                num_doc_profesional = None
                
                if col_mapping.get('tipo_doc_profesional'):
                    tipo_doc_profesional = str(row.get(col_mapping['tipo_doc_profesional'], '')).strip().upper()
                    if tipo_doc_profesional and tipo_doc_profesional.lower() not in ['nan', '', 'none', 'null']:
                        pass  # Mantener el valor
                    else:
                        tipo_doc_profesional = None
                        
                if col_mapping.get('num_doc_profesional'):
                    num_doc_profesional = str(row.get(col_mapping['num_doc_profesional'], '')).strip()
                    if num_doc_profesional and num_doc_profesional.lower() not in ['nan', '', 'none', 'null']:
                        # Limpiar número de documento profesional
                        num_doc_profesional = num_doc_profesional.replace('.', '').replace('-', '').replace(' ', '').replace(',', '')
                    else:
                        num_doc_profesional = None

                # Validar datos básicos
                if not tipo_doc or not num_doc or not cod_diag:
                    logger.debug(f"Fila {index}: Datos incompletos - tipo: '{tipo_doc}', num: '{num_doc}', diag: '{cod_diag}'")
                    continue
                
                if cod_diag.lower() in ['nan', '', 'none', 'null', 'nat']:
                    logger.debug(f"Fila {index}: Diagnóstico inválido: '{cod_diag}'")
                    continue
                
                # Limpiar número de documento del paciente
                num_doc = num_doc.replace('.', '').replace('-', '').replace(' ', '').replace(',', '')

                key = (tipo_doc, num_doc)

                cod_diag = cod_diag.replace('.', '').replace('-', '').replace(' ', '').replace(',', '')

                # Almacenar información completa para cada paciente
                if key not in diagnosticos_por_paciente:
                    diagnosticos_por_paciente[key] = {
                        'cod_diagnostico': cod_diag,
                        'tipo_doc_profesional': tipo_doc_profesional,
                        'num_doc_profesional': num_doc_profesional
                    }
                    logger.debug(f"Diagnóstico agregado: {key} -> {cod_diag}")
                
            except Exception as e:
                logger.warning(f"Error procesando fila {index}: {e}")
                continue
        
        self.diagnosticos_dict = diagnosticos_por_paciente
        logger.info(f"Diagnósticos únicos por paciente: {len(self.diagnosticos_dict)}")

        # Mostrar algunos ejemplos
        if self.diagnosticos_dict:
            logger.info("Ejemplos de diagnósticos cargados:")
            for i, (key, diag_info) in enumerate(list(self.diagnosticos_dict.items())[:5]):
                logger.info(f"  {key[0]} - {key[1]} -> {diag_info['cod_diagnostico']} (Prof: {diag_info.get('tipo_doc_profesional', 'N/A')} - {diag_info.get('num_doc_profesional', 'N/A')})")
    
    def _find_diagnostic(self, tipo_doc: str, num_doc: str) -> Optional[Dict[str, str]]:
        """Busca el diagnóstico para un paciente específico"""
        
        # Búsqueda exacta
        key_exact = (tipo_doc, num_doc)
        if key_exact in self.diagnosticos_dict:
            logger.debug(f"Coincidencia exacta encontrada: {key_exact}")
            return self.diagnosticos_dict[key_exact]
        
        # Búsqueda por número de documento solamente (más flexible)
        for (t_doc, n_doc), diagnostico_info in self.diagnosticos_dict.items():
            if n_doc == num_doc:
                logger.info(f"Coincidencia por número de documento: {num_doc}")
                return diagnostico_info
        
        # Búsqueda parcial (últimos dígitos) - solo si el documento es largo
        if len(num_doc) > 6:
            num_doc_partial = num_doc[-6:]  # Últimos 6 dígitos
            for (t_doc, n_doc), diagnostico_info in self.diagnosticos_dict.items():
                if len(n_doc) > 6 and (n_doc.endswith(num_doc_partial) or num_doc_partial in n_doc):
                    logger.info(f"Coincidencia parcial: {num_doc} -> {n_doc}")
                    return diagnostico_info
        
        # Búsqueda flexible por tipo de documento similar
        tipo_doc_variants = {
            'CC': ['CC', 'CEDULA', 'C.C.', 'CI'],
            'TI': ['TI', 'TARJETA', 'T.I.'],
            'CE': ['CE', 'C.E.', 'EXTRANJERIA'],
            'RC': ['RC', 'R.C.', 'REGISTRO'],
            'PA': ['PA', 'PASAPORTE']
        }
        
        if tipo_doc in tipo_doc_variants:
            for variant in tipo_doc_variants[tipo_doc]:
                key_variant = (variant, num_doc)
                if key_variant in self.diagnosticos_dict:
                    logger.info(f"Coincidencia por tipo de documento similar: {tipo_doc} -> {variant}")
                    return self.diagnosticos_dict[key_variant]
        
        return None
    
    def _process_service_list(self, services: List[dict], diagnostico_info: Dict[str, str], service_type: str):
        """Procesa una lista de servicios (consultas, procedimientos, medicamentos)"""
        if not services:
            return
            
        logger.info(f"  Procesando {len(services)} {service_type}...")
        
        # Log específico para procedimientos
        if service_type == 'procedimientos':
            logger.info(f"   PROCEDIMIENTOS: Validando y limpiando codConsulta en {len(services)} procedimientos...")
        elif service_type == 'consultas':
            logger.info(f"   CONSULTAS: Validando y limpiando codConsulta en {len(services)} consultas...")
            
        if diagnostico_info:
            logger.info(f"  Datos disponibles - Diagnóstico: {diagnostico_info.get('cod_diagnostico')}, Tipo doc prof: {diagnostico_info.get('tipo_doc_profesional')}, Num doc prof: {diagnostico_info.get('num_doc_profesional')}")
        else:
            logger.warning(f"  No hay diagnóstico disponible para {service_type}")

        for i, service in enumerate(services):
            self.stats['registros_procesados'] += 1
            
            # LIMPIAR CAMPO codConsulta PARA QUE SEA NUMÉRICO
            self._clean_cod_consulta(service, service_type, i)
            
            # COMPLETAR DIAGNÓSTICO PRINCIPAL
            cod_diag_actual = service.get('codDiagnosticoPrincipal', '')
            cod_diag_actual = str(cod_diag_actual).strip()
            
            if (cod_diag_actual == '' or 
                cod_diag_actual.lower() in ['none', 'null', 'nan', 'nat'] or
                cod_diag_actual == '0'):
                
                service['codDiagnosticoPrincipal'] = diagnostico_info['cod_diagnostico']
                self.stats['cambios_realizados'] += 1
                logger.info(f"    {service_type.capitalize()} {i+1}: Completado con {diagnostico_info['cod_diagnostico']}")
            else:
                logger.info(f"    {service_type.capitalize()} {i+1}: Ya tiene diagnóstico ({cod_diag_actual})")

            # COMPLETAR TIPO DOCUMENTO PROFESIONAL
            if 'tipoDocumentoIdentificacion' in service:
                tipo_doc_prof_actual = str(service.get('tipoDocumentoIdentificacion', '')).strip()
                
                # Nota: 'NI' se maneja en el bloque siguiente para cambiarlo a 'CC'
                if (tipo_doc_prof_actual in ['', '00', 'null', 'none'] or 
                    service.get('tipoDocumentoIdentificacion') is None):
                    
                    if diagnostico_info.get('tipo_doc_profesional'):
                        service['tipoDocumentoIdentificacion'] = diagnostico_info['tipo_doc_profesional']
                        self.stats['cambios_tipo_documento_profesional'] += 1
                        logger.info(f"      ✓ tipoDocumentoIdentificacion completado con '{diagnostico_info['tipo_doc_profesional']}'")
                    else:
                        logger.debug(f"      No hay tipo_doc_profesional disponible")
                else:
                    logger.debug(f"      tipoDocumentoIdentificacion ya tiene valor: '{tipo_doc_prof_actual}'")
            
            # COMPLETAR NÚMERO DOCUMENTO PROFESIONAL
            if 'numDocumentoIdentificacion' in service:
                num_doc_prof_actual = str(service.get('numDocumentoIdentificacion', '')).strip()
                
                if (num_doc_prof_actual in ['', '00', 'null', 'none'] or 
                    service.get('numDocumentoIdentificacion') is None):
                    
                    if diagnostico_info.get('num_doc_profesional'):
                        service['numDocumentoIdentificacion'] = diagnostico_info['num_doc_profesional']
                        self.stats['cambios_num_documento_profesional'] += 1
                        logger.info(f"      ✓ numDocumentoIdentificacion completado con '{diagnostico_info['num_doc_profesional']}'")
                    else:
                        logger.debug(f"      No hay num_doc_profesional disponible")
                else:
                    logger.debug(f"      numDocumentoIdentificacion ya tiene valor: '{num_doc_prof_actual}'")


            # CAMBIAR tipoDocumentoIdentificacion de "NI" a "CC"
            
            if 'tipoDocumentoIdentificacion' in service:
                tipo_doc_prof_actual = str(service.get('tipoDocumentoIdentificacion', '')).strip().upper()
                
                if tipo_doc_prof_actual == 'NI':
                    service['tipoDocumentoIdentificacion'] = 'CC'
                    self.stats['cambios_tipo_documento_profesional'] += 1
                    logger.info(f"      ✓ tipoDocumentoIdentificacion cambiado de 'NI' a 'CC'")

            # CAMBIAR tipoDiagnosticoPrincipal de "00" a "03"
            if 'tipoDiagnosticoPrincipal' in service:
                tipo_diag_actual = str(service.get('tipoDiagnosticoPrincipal', '')).strip()
                if tipo_diag_actual == '00':
                    service['tipoDiagnosticoPrincipal'] = '03'
                    self.stats['cambios_tipo_diagnostico_principal'] += 1
                    logger.info(f"      ✓ tipoDiagnosticoPrincipal cambiado de '00' a '03'")
                else:
                    logger.debug(f"      tipoDiagnosticoPrincipal ya tiene valor: '{tipo_diag_actual}'")
            
            # OTROS CAMPOS ESTÁNDAR
            # Modificar codDiagnosticoRelacionado1 y codDiagnosticoRelacionado2
            if 'codDiagnosticoRelacionado1' in service:
                if service['codDiagnosticoRelacionado1'] == 'A15':
                    service['codDiagnosticoRelacionado1'] = None
                    self.stats['cambios_diagnostico_relacionado'] += 1
                    logger.info(f"      codDiagnosticoRelacionado1 cambiado de A15 a null")
            if 'codDiagnosticoRelacionado1' in service:
                if service['codDiagnosticoRelacionado1'] == 'A15.':
                    service['codDiagnosticoRelacionado1'] = None
                    self.stats['cambios_diagnostico_relacionado'] += 1
                    logger.info(f"      codDiagnosticoRelacionado1 cambiado de A15 a null")
            if 'codDiagnosticoRelacionado1' in service:
                if service['codDiagnosticoRelacionado1'] == 'A18.':
                    service['codDiagnosticoRelacionado1'] = None
                    self.stats['cambios_diagnostico_relacionado'] += 1
                    logger.info(f"      codDiagnosticoRelacionado1 cambiado de A18. a null")
            if 'codDiagnosticoRelacionado1' in service:
                if service['codDiagnosticoRelacionado1'] == 'M32.':
                    service['codDiagnosticoRelacionado1'] = None
                    self.stats['cambios_diagnostico_relacionado'] += 1
                    logger.info(f"      codDiagnosticoRelacionado1 cambiado de M32. a null")

            if 'codDiagnosticoRelacionado2' in service:
                if service['codDiagnosticoRelacionado2'] == 'A15':
                    service['codDiagnosticoRelacionado2'] = None
                    self.stats['cambios_diagnostico_relacionado'] += 1
                    logger.info(f"      codDiagnosticoRelacionado2 cambiado de A15 a null")
            
            # Modificar codDiagnosticoRelacionado1 UCI1 a NULL
            if 'codDiagnosticoRelacionado1' in service:
                if service['codDiagnosticoRelacionado1'] == 'UCI1':
                    service['codDiagnosticoRelacionado1'] = None
                    self.stats['cambios_diagnostico_relacionado'] += 1
                    logger.info(f"      codDiagnosticoRelacionado1 cambiado de UCI1 a null")
            
            # Modificar finalidadTecnologiaSalud
            if 'finalidadTecnologiaSalud' in service:
                if service['finalidadTecnologiaSalud'] == '' or service['finalidadTecnologiaSalud'] is None:
                    service['finalidadTecnologiaSalud'] = '44'
                    self.stats['cambios_finalidad_tecnologia'] += 1
                    logger.info(f"      finalidadTecnologiaSalud cambiado a '44'")
            
            # Modificar tipoMedicamento
            if 'tipoMedicamento' in service:
                tipo_med_actual = str(service.get('tipoMedicamento', '')).strip()
                if tipo_med_actual in ['', '00', 'null', 'none'] or service['tipoMedicamento'] is None:
                    service['tipoMedicamento'] = '01'
                    self.stats['cambios_tipo_medicamento'] += 1
                    logger.info(f"      tipoMedicamento cambiado a '01'")
            
            # Modificar modalidadGrupoServicioTecSal
            if 'modalidadGrupoServicioTecSal' in service:
                modalidad_actual = str(service.get('modalidadGrupoServicioTecSal', '')).strip()
                if modalidad_actual in ['', '00', 'null', 'none'] or service['modalidadGrupoServicioTecSal'] is None:
                    service['modalidadGrupoServicioTecSal'] = '01'
                    self.stats['cambios_modalidad_grupo'] += 1
                    logger.info(f"      modalidadGrupoServicioTecSal cambiado a '01'")

    def _process_other_services(self, other_services: List[dict], diagnostico_info: Dict[str, str]):
        """Procesa otros servicios"""
        if not other_services:
            return
            
        logger.info(f"  Procesando {len(other_services)} otros servicios...")
        
        for i, service in enumerate(other_services):
            self.stats['registros_procesados'] += 1
            
            # LIMPIAR CAMPO codConsulta PARA QUE SEA NUMÉRICO
            self._clean_cod_consulta(service, "otros servicios", i)
            # CAMBIAR tipoDocumentoIdentificacion de "NI" a "CC" en otrosServicios
            if 'tipoDocumentoIdentificacion' in service:
                tipo_doc_prof_actual = str(service.get('tipoDocumentoIdentificacion', '')).strip().upper()                
                if tipo_doc_prof_actual == 'NI':
                    service['tipoDocumentoIdentificacion'] = 'CC'
                    self.stats['cambios_tipo_documento_profesional'] += 1
                    logger.info(f"      ✓ tipoDocumentoIdentificacion cambiado de 'NI' a 'CC'")
            
            # COMPLETAR DATOS DEL PROFESIONAL (igual que en otros servicios)
            
            if 'tipoDocumentoIdentificacion' in service:
                tipo_doc_prof_actual = str(service.get('tipoDocumentoIdentificacion', '')).strip()
                
                # Nota: 'NI' se maneja en el bloque siguiente para cambiarlo a 'CC'
                if (tipo_doc_prof_actual in ['', '00', 'null', 'none'] or 
                    service.get('tipoDocumentoIdentificacion') is None):
                    
                    if diagnostico_info.get('tipo_doc_profesional'):
                        service['tipoDocumentoIdentificacion'] = diagnostico_info['tipo_doc_profesional']
                        self.stats['cambios_tipo_documento_profesional'] += 1
                        logger.info(f"      ✓ tipoDocumentoIdentificacion completado con '{diagnostico_info['tipo_doc_profesional']}'")


           
            
            if 'numDocumentoIdentificacion' in service:
                num_doc_prof_actual = str(service.get('numDocumentoIdentificacion', '')).strip()
                
                if (num_doc_prof_actual in ['', '00', 'null', 'none'] or 
                    service.get('numDocumentoIdentificacion') is None):
                    
                    if diagnostico_info.get('num_doc_profesional'):
                        service['numDocumentoIdentificacion'] = diagnostico_info['num_doc_profesional']
                        self.stats['cambios_num_documento_profesional'] += 1
                        logger.info(f"      ✓ numDocumentoIdentificacion completado con '{diagnostico_info['num_doc_profesional']}'")

            # CAMBIAR tipoDiagnosticoPrincipal de "00" a "03"
            if 'tipoDiagnosticoPrincipal' in service:
                tipo_diag_actual = str(service.get('tipoDiagnosticoPrincipal', '')).strip()
                if tipo_diag_actual == '00':
                    service['tipoDiagnosticoPrincipal'] = '03'
                    self.stats['cambios_tipo_diagnostico_principal'] += 1
                    logger.info(f"      ✓ tipoDiagnosticoPrincipal cambiado de '00' a '03'")
                else:
                    logger.debug(f"      tipoDiagnosticoPrincipal ya tiene valor: '{tipo_diag_actual}'")

            # OTROS CAMPOS ESTÁNDAR (igual que en otros servicios)
            if 'codDiagnosticoRelacionado1' in service:
                if service['codDiagnosticoRelacionado1'] == 'A15':
                    service['codDiagnosticoRelacionado1'] = None
                    self.stats['cambios_diagnostico_relacionado'] += 1
                    logger.info(f"      codDiagnosticoRelacionado1 cambiado de A15 a null")
            if 'codDiagnosticoRelacionado1' in service:
                if service['codDiagnosticoRelacionado1'] == 'A15.':
                    service['codDiagnosticoRelacionado1'] = None
                    self.stats['cambios_diagnostico_relacionado'] += 1
                    logger.info(f"      codDiagnosticoRelacionado1 cambiado de A15 a null")
            if 'codDiagnosticoRelacionado1' in service:
                if service['codDiagnosticoRelacionado1'] == 'A18.':
                    service['codDiagnosticoRelacionado1'] = None
                    self.stats['cambios_diagnostico_relacionado'] += 1
                    logger.info(f"      codDiagnosticoRelacionado1 cambiado de A18. a null")
            if 'codDiagnosticoRelacionado1' in service:
                if service['codDiagnosticoRelacionado1'] == 'M32.':
                    service['codDiagnosticoRelacionado1'] = None
                    self.stats['cambios_diagnostico_relacionado'] += 1
                    logger.info(f"      codDiagnosticoRelacionado1 cambiado de M32. a null")

            if 'codDiagnosticoRelacionado2' in service:
                if service['codDiagnosticoRelacionado2'] == 'A15':
                    service['codDiagnosticoRelacionado2'] = None
                    self.stats['cambios_diagnostico_relacionado'] += 1
                    logger.info(f"      codDiagnosticoRelacionado2 cambiado de A15 a null")

            if 'codDiagnosticoRelacionado1' in service:
                if service['codDiagnosticoRelacionado1'] == 'UCI1':
                    service['codDiagnosticoRelacionado1'] = None
                    self.stats['cambios_diagnostico_relacionado'] += 1
                    logger.info(f"      codDiagnosticoRelacionado1 cambiado de UCI1 a null")
            
            if 'finalidadTecnologiaSalud' in service:
                if service['finalidadTecnologiaSalud'] == '' or service['finalidadTecnologiaSalud'] is None:
                    service['finalidadTecnologiaSalud'] = '44'
                    self.stats['cambios_finalidad_tecnologia'] += 1
                    logger.info(f"      finalidadTecnologiaSalud cambiado a '44'")
            
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
                
                # Crear diagnostico_info vacío (no tenemos Excel, pero podemos usar valores por defecto)
                diagnostico_info = {
                    'cod_diagnostico': None,
                    'tipo_doc_profesional': None,
                    'num_doc_profesional': None
                }
                
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
                status = "EXITOSO" if archivo['procesado'] else " FALLIDO"
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
        print(f"- Cambios codConsulta: {self.stats['cambios_cod_consulta']}")
        print(f"- Cambios tipoDiagnosticoPrincipal: {self.stats['cambios_tipo_diagnostico_principal']}")
        print(f"- Cambios tipo documento: {self.stats['cambios_tipo_documento']}")
        print(f"- Cambios país residencia: {self.stats['cambios_pais_residencia']}")
        
        if self.stats['errores']:
            print(f"\nERRORES:")
            for error in self.stats['errores']:
                print(f"  - {error}")