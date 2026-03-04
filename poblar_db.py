import pandas as pd
from sqlalchemy import create_engine
import os

# 1. Configuración de la conexión (Usa la misma URL de tu .env)
# Si tu URL de Neon no tiene 'postgresql://', asegúrate de agregarla.
DATABASE_URL='postgresql://neondb_owner:npg_5wf1ynJgDzCv@ep-winter-poetry-a8qdgezz-pooler.eastus2.azure.neon.tech/neondb?sslmode=require&channel_binding=require'

# 2. Ruta de tu archivo Excel
EXCEL_PATH = "CPdescarga.xls" 

def cargar_datos():
    try:
        engine = create_engine(DATABASE_URL)
        print("Conectado a Neon exitosamente.")

        # Leer todas las pestañas del Excel
        print("Leyendo Excel... esto puede tardar unos segundos.")
        dict_excel = pd.read_excel(EXCEL_PATH, sheet_name=None, engine='xlrd')
        
        all_data = []

        for sheet_name, df in dict_excel.items():
            if sheet_name == 'Nota': # Ignorar la pestaña de notas
                continue
            
            print(f"Procesando estado: {sheet_name}")
            
            # Seleccionamos y renombramos las columnas para que coincidan con tu schema de Prisma
            # Prisma suele poner la primera letra en mayúscula o minúscula según tu archivo .prisma
            # Si en tu schema usaste 'd_codigo', aquí debe mapearse igual.
            df_temp = df[['d_codigo', 'd_asenta', 'd_tipo_asenta', 'D_mnpio', 'd_estado', 'd_ciudad', 'd_zona']].copy()
            
            # Limpieza: Rellenar CPs con ceros a la izquierda (ej. 1000 -> 01000)
            df_temp['d_codigo'] = df_temp['d_codigo'].astype(str).str.zfill(5)
            
            all_data.append(df_temp)

        # Unificar todos los estados
        df_final = pd.concat(all_data, ignore_index=True)

        # 3. Insertar en la tabla creada por Prisma
        # NOTA: Prisma por defecto nombra la tabla igual que el modelo (ej: "CodigoPostal")
        print(f"Subiendo {len(df_final)} registros a Neon...")
        df_final.to_sql('CodigoPostal', engine, if_exists='append', index=False)
        
        print("¡Éxito! Datos cargados correctamente.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    cargar_datos()