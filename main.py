from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prisma import Prisma
from contextlib import asynccontextmanager
import uvicorn

# 1. Configuración del Cliente de Prisma
prisma = Prisma()

# 2. Manejo del Ciclo de Vida (Lifespan)
@asynccontextmanager
async def lifespan(app: FastAPI):
    await prisma.connect()
    yield
    await prisma.disconnect()

# 3. Inicialización de FastAPI
app = FastAPI(
    title="Romi SEPOMEX API - México",
    description="API interna para búsqueda de códigos postales, colonias, estados y municipios",
    version="1.0.0",
    lifespan=lifespan
)

# 4. Configuración de CORS
# Esto permite que tu app de gestión de equipos (React/Astro) haga peticiones
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 5. Endpoint Principal: Búsqueda por CP
@app.get("/api/v1/cp/{cp}")
async def buscar_cp(cp: str):
    """
    Busca un código postal en la base de datos de Neon y devuelve:
    Estado, Municipio, Ciudad y la lista de Colonias (Asentamientos).
    """
    cp_limpio = cp.zfill(5)
    
    try:
        registros = await prisma.codigopostal.find_many(
            where={'d_codigo': cp_limpio}
        )
    except Exception as e:
        print(f"Error de base de datos: {e}")
        raise HTTPException(status_code=500, detail="Error interno al consultar la base de datos")

    if not registros:
        raise HTTPException(
            status_code=404, 
            detail=f"El Código Postal {cp_limpio} no existe en la base de datos."
        )

    base = registros[0]
    
    return {
        "codigo_postal": cp_limpio,
        "estado": base.d_estado,
        "municipio": base.D_mnpio,
        "ciudad": base.d_ciudad or "N/A",
        "total_colonias": len(registros),
        "colonias": [
            {
                "nombre": r.d_asenta,
                "tipo": r.d_tipo_asenta,
                "zona": r.d_zona
            } for r in registros
        ]
    }

# --- Búsqueda por Estado ---
@app.get("/api/v1/estados/{nombre}")
async def buscar_por_estado(nombre: str):
    registros = await prisma.codigopostal.find_many(
        where={
            'd_estado': {
                'contains': nombre,
                'mode': 'insensitive'
            }
        },
        distinct=['d_estado']
    )
    return {"resultados": [r.d_estado for r in registros]}

@app.get("/api/v1/municipios/{nombre}")
async def buscar_por_municipio(nombre: str, estado: str = None):
    filtros = {
        'D_mnpio': {'contains': nombre, 'mode': 'insensitive'}
    }
    if estado:
        filtros['d_estado'] = {'contains': estado, 'mode': 'insensitive'}

    registros = await prisma.codigopostal.find_many(
        where=filtros,
        distinct=['D_mnpio', 'd_estado']
    )
    return {
        "resultados": [
            {"municipio": r.D_mnpio, "estado": r.d_estado} 
            for r in registros
        ]
    }

# 6. Ejecución del servidor
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
