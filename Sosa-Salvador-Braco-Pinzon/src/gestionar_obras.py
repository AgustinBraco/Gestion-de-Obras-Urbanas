from abc import ABC, abstractmethod
from typing import Optional
from .modelo_orm import db, Obra
import pandas as pd
from peewee import IntegrityError, fn, OperationalError

# Definir superclase abstracta
class GestionarObra(ABC):
  # Definir métodos abstractos
  @abstractmethod
  def extraer_datos(self, archivo: str) -> pd.DataFrame:
    pass
  
  @abstractmethod
  def conectar_db(self) -> bool:
    pass
  
  @abstractmethod
  def mapear_orm(self) -> bool:
    pass
  
  @abstractmethod
  def limpiar_datos(self) -> pd.DataFrame:
    pass
  
  @abstractmethod
  def cargar_datos(self) -> bool:
    pass
  
  @abstractmethod
  def nueva_obra(self) -> Optional[Obra]:
    pass
  
  @abstractmethod
  def obtener_indicadores(self) -> dict:
    pass

# Implementar subclase
class GestionarObraCSV(GestionarObra):
  # Campos requeridos (definidos previamente)
  __REQUERIDO = [
      "nombre",
      "tipo",
      "area",
      "barrio",
      "comuna",
      "monto_contrato",
      "etapa",
      "contratacion_tipo",
      "nro_contratacion",
      "licitacion_oferta_empresa",
      "expediente_numero",
      "destacada",
      "fecha_inicio",
      "fecha_fin_inicial",
      "financiamiento",
      "porcentaje_avance",
      "plazo_meses",
      "mano_obra",
  ]

  def __init__(self):
    self.datos = None

  def __str__(self) -> str:
    return 'Gestor de obras iniciado correctamente'

# Implementar métodos
  def extraer_datos(self, archivo: str) -> pd.DataFrame:
    try:
      self.datos = pd.read_csv(archivo, delimiter=';')

      print('Datos extraídos con éxito')
      return self.datos
    except Exception as error:
      print(f'Error al extraer datos: {error}')
      return pd.DataFrame()
    
  def conectar_db(self) -> bool:
    try:
      db.connect()

      print('Base de datos conectada exitosamente')
      return True
    except Exception as error:
      print('Error al conectarse con la base de datos:', error)
      return False

  def mapear_orm(self) -> bool:
    try:
      db.create_tables([Obra], safe=True)
      
      print('Tablas creadas exitosamente')
      return True
    except Exception as error:
      print('Error al mapear ORM:', error)
      return False

  def limpiar_datos(self, datos: pd.DataFrame) -> pd.DataFrame:
    try:
      REQUERIDO = self.__REQUERIDO
      
      # Renombrar columna y campos por compatibilidad 
      datos = datos.rename(columns={'expediente-numero': 'expediente_numero'})
      datos['destacada'] = datos['destacada'].apply(lambda x: True if x == 'SI' else False)

      # Filtrar y limpiar datos
      datos_filtrados = datos.dropna(how='all')
      datos_limpios = datos_filtrados.dropna(subset=REQUERIDO)
      self.datos = datos_limpios

      print('Datos limpios generados con éxito')
      return self.datos
    except Exception as error:
      print(f'Error al limpiar datos: {error}')
      return datos
    
  def cargar_datos(self, datos: pd.DataFrame) -> bool:
    try:
      REQUERIDO = self.__REQUERIDO

      # Iterar cada fila para crear entradas sin duplicados
      for _, row in datos.iterrows():
        if Obra.select().where(Obra.nombre == row['nombre']).exists():
          print(f"La obra '{row['nombre']}' ya existe.")
          continue

        if all(key in row for key in REQUERIDO):
          Obra.create(
            nombre = row['nombre'],
            tipo = row['tipo'],
            area = row['area'],
            barrio = row['barrio'],
            comuna = row['comuna'],
            monto_contrato = row['monto_contrato'],
            etapa = row['etapa'],
            contratacion_tipo = row['contratacion_tipo'],
            nro_contratacion = row['nro_contratacion'],
            licitacion_oferta_empresa = row['licitacion_oferta_empresa'],
            expediente_numero = row['expediente_numero'],
            destacada = row['destacada'],
            fecha_inicio = row['fecha_inicio'],
            fecha_fin_inicial = row['fecha_fin_inicial'],
            financiamiento = row['financiamiento'],
            porcentaje_avance = row['porcentaje_avance'],
            plazo_meses = row['plazo_meses'],
            mano_obra = row['mano_obra']
          )

      print('Datos cargados exitosamente')
      return True
    except Exception as error:
      print(f"Error al cargar los datos: {error}")
      return False

  def nueva_obra(self) -> Optional[Obra]:
    try:
      # Solicitar datos al usuario
      nombre = input("Ingrese el nombre de la obra: ")
      tipo = input("Ingrese el tipo de obra: ")
      area = input("Ingrese el área responsable: ")
      barrio = input("Ingrese el barrio: ")
      comuna = int(input('Ingrese la comuna: '))
      monto_contrato = float(input("Ingrese el monto del contrato: "))

      # Crear nueva entrada con esos datos
      nueva_obra = Obra(
        nombre = nombre,
        tipo = tipo,
        area = area,
        barrio = barrio,
        comuna = comuna,
        monto_contrato = monto_contrato
      )
      nueva_obra.save()

      print('Obra creada exitosamente')
      return nueva_obra
    except Exception as error:
      print(f'Error al crear nueva obra: {error}')
      return None

  def obtener_indicadores(self) -> dict:
    print("Indicadores de gestión de obras:")
    try:
            areas_responsables = Obra.select(Obra.area_responsable).distinct()
            print("Áreas responsables:")
            for area in areas_responsables:
                print(area.area_responsable)
    except OperationalError as e:
            print(f"Error al obtener áreas responsables: {e}")

    try:
            tipos_obra = Obra.select(Obra.tipo_obra).distinct()
            print("Tipos de obra:")
            for tipo in tipos_obra:
                print(tipo.tipo_obra)
    except OperationalError as e:
            print(f"Error al obtener tipos de obra: {e}")

    try:
            etapas = Obra.select(Obra.etapa, fn.COUNT(Obra.id).alias('cantidad')).group_by(Obra.etapa)
            print("Cantidad de obras por etapa:")
            for etapa in etapas:
                print(f"{etapa.etapa}: {etapa.cantidad}")
    except OperationalError as e:
            print(f"Error al obtener obras por etapa: {e}")

    try:
            inversiones_por_tipo = Obra.select(
                Obra.tipo_obra,
                fn.COUNT(Obra.id).alias('cantidad'),
                fn.SUM(Obra.monto_inversion).alias('total_inversion')
            ).group_by(Obra.tipo_obra)
            print("Inversiones por tipo de obra:")
            for tipo in inversiones_por_tipo:
                print(f"{tipo.tipo_obra}: Cantidad={tipo.cantidad}, Inversión Total={tipo.total_inversion}")
    except OperationalError as e:
            print(f"Error al obtener inversiones por tipo de obra: {e}")

    try:
            barrios_comunas = Obra.select(Obra.barrio).where(Obra.barrio.in_(['Comuna 1', 'Comuna 2', 'Comuna 3'])).distinct()
            print("Barrios en comunas 1, 2 y 3:")
            for barrio in barrios_comunas:
                print(barrio.barrio)
    except OperationalError as e:
            print(f"Error al obtener barrios en comunas específicas: {e}")

    try:
            obras_comuna_1 = Obra.select().where((Obra.barrio == 'Comuna 1') & (Obra.etapa == 'Finalizada'))
            cantidad_obras = obras_comuna_1.count()
            total_inversion_comuna_1 = obras_comuna_1.aggregate(fn.SUM(Obra.monto_inversion))
            print(f"Obras finalizadas en comuna 1: Cantidad={cantidad_obras}, Inversión Total={total_inversion_comuna_1}")
    except OperationalError as e:
            print(f"Error al obtener obras finalizadas en comuna 1: {e}")

    try:
            obras_menos_24_meses = Obra.select().where((Obra.etapa == 'Finalizada') & (Obra.plazo_meses <= 24)).count()
            print(f"Obras finalizadas en <= 24 meses: {obras_menos_24_meses}")
    except OperationalError as e:
            print(f"Error al obtener obras finalizadas en 24 meses o menos: {e}")

    try:
            total_obras = Obra.select().count()
            obras_finalizadas = Obra.select().where(Obra.etapa == 'Finalizada').count()
            porcentaje_finalizadas = (obras_finalizadas / total_obras) * 100 if total_obras > 0 else 0
            print(f"Porcentaje de obras finalizadas: {porcentaje_finalizadas:.2f}%")
    except OperationalError as e:
            print(f"Error al calcular porcentaje de obras finalizadas: {e}")

    try:
            total_mano_obra = Obra.select(fn.SUM(Obra.mano_obra)).scalar() or 0
            print(f"Total de mano de obra empleada: {total_mano_obra}")
    except OperationalError as e:
            print(f"Error al obtener cantidad total de mano de obra empleada: {e}")

    try:
            monto_total_inversion = Obra.select(fn.SUM(Obra.monto_inversion)).scalar() or 0
            print(f"Monto total de inversión: {monto_total_inversion}")
    except OperationalError as e:
            print(f"Error al obtener monto total de inversión: {e}")
