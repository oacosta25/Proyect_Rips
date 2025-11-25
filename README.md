# Proyect_Rips

## IMPORTANTE INSTALAT
1. Visual studio code
2. Python 
3. libreria pandas

## 📊 ESTRUCTURA DEL PROYECTO
├── controller/
│   ├── diagnostic_completer.py  (NO cambió - ya estaba bien)
│   └── json_reader.py            (NO cambió)
├── main.py                       (⭐ ESTE SE CORRIGIÓ)
├── Bases/
│   ├── Rutas_Json.csv           (Lista de archivos JSON)
│   └── RIPS_3.csv               (Diagnósticos)
│   └── Codigos.csv              (Lista de archivos con errores)
└── diagnostic_completion_debug.log  (Se genera al ejecutar)

## ⚙️ CÓMO FUNCIONA
1. Se ejecuta el archivo scriptrutas
2. Se ejecuta luego el archivo main
3. Lee lista de archivos JSON    
4. Para cada archivo JSON:
   ├─ Carga el JSON en memoria
   ├─ Procesa usuarios y servicios
   ├─ Aplica todos los cambios 
   ├─ Crea backup del original (.backup) //opcional se habilitar linea json render 240 -247
   └─ ⭐ GUARDA el archivo modificado (ESTO ESTABA COMENTADO)
   ↓
5. Genera log con todos los cambios


