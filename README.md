# procesamientonube-referencia

Repositorio de referencia del laboratorio de la Unidad 3 del curso Procesamiento de Datos en la Nube (Maestría en Ciencia de Datos, Icesi, 2026-2).

Contiene la solución completa del laboratorio, guardada **paso por paso**: cada commit corresponde a un paso de la guía. No se clona ni se modifica: desde el repositorio de tu grupo traes, en cada paso, los archivos de ese paso.

---

## El caso

Una empresa de telecomunicaciones pierde clientes, y el área de Retención solo puede llamar a unos pocos cada semana. Hoy los elige al azar. El objetivo es que, cada lunes, Retención reciba una lista priorizada de clientes en riesgo, con datos confiables y un modelo en producción.

La Unidad 3 responde la primera pregunta: **¿podemos confiar en lo que sabemos de cada cliente?** El resultado es un perfil del cliente en BigQuery, que es el punto de partida de las unidades 4 a 8.

## Los datos

Están en el bucket común `gs://data-telco-bronze-20260926/`, de solo lectura para todos.

| Fuente | Carpeta | Qué es |
|---|---|---|
| info churn | `base/` | 7.043 clientes con la variable Churn (si se fueron o no). Se usa para entrenar el modelo |
| crm | `crm/` | Lotes semanales de clientes nuevos, sin Churn. En la U3 se usan los 5 lotes de agosto |
| uso | `uso/` | 95 millones de eventos (llamadas, datos y tickets de soporte) de 13.563 clientes en 435 días, en parquet, una carpeta por día |

## Arquitectura

```
bronze (datos crudos, solo lectura)
   │
   ▼  churn/silver.py   ¿el dato es confiable?
silver (tipado, sin duplicados, con linaje)  +  cuarentena (lo ilegible, con su motivo)
   │
   ▼  churn/gold.py     ¿qué necesita el negocio?
gold (1 fila por cliente y foto + resumen de 90 días de uso)
   │
   ▼  jobs/cargar_bq.sh
BigQuery: perfil_entrenamiento · perfil_lotes · cuarentena
```

El procesamiento completo corre como un batch de Spark serverless en Google Cloud (`jobs/u3_batch.py`), lanzado con la cuenta de servicio del grupo.

## Estructura del repositorio

| Ruta | Qué contiene |
|---|---|
| `exploracion/` | 3 scripts de perfilado: `revisar_calidad.py`, `buscar_sucios.py` y `explorar_uso.py` |
| `churn/silver.py` | Limpieza de bronze a silver y cuarentena (8 funciones) |
| `churn/gold.py` | Perfil del cliente con la ventana de 90 días anteriores a cada foto |
| `tests/` | 76 reglas con pytest: 48 de silver y 28 de gold |
| `jobs/u3_batch.py` | Programa completo que corre en la nube |
| `jobs/lanzar_batch.sh` | Lanza el batch de Spark serverless con todos sus parámetros |
| `jobs/cargar_bq.sh` | Carga idempotente de gold y cuarentena a BigQuery |
| `pytest.ini`, `requirements-dev.txt`, `.gitignore` | Configuración de las pruebas y del repositorio |

## Cómo se usa

### 1. Una sola vez, en el repositorio de tu grupo (Paso 2.1 de la guía)

Este repositorio es público: no necesitas token para descargarlo.

```
git remote add referencia https://github.com/djaimesf/procesamientonube-referencia.git
git fetch referencia
git log --oneline referencia/main
```

`fetch` descarga la historia de la referencia, pero no cambia nada en tu carpeta. Cuando haces `push`, solo sube lo tuyo.

### 2. En cada paso

Primero mira qué trae el commit y después trae exactamente esos archivos:

```
git show --stat --oneline <hash>
git checkout <hash> -- <archivos>
git status -s
```

Luego corres lo que indica la guía y haces **tu propio commit** en tu repositorio. Tus commits tendrán otros hashes, aunque el contenido sea el mismo: el hash depende también del autor, la fecha y el commit anterior.

### Mapa de commits

| Paso | Hash | Qué traer | Resultado esperado |
|---|---|---|---|
| 1 | `3dcecff` | No se trae: cada grupo hace su propio commit de prueba | — |
| 2 | `4776bfc` | `exploracion` | 3 scripts nuevos |
| 3 (rojo) | `47c27b5` | `.gitignore pytest.ini requirements-dev.txt churn tests` | `38 failed, 4 passed, 6 errors` |
| 4 (verde) | `2cc9dd0` | `churn/silver.py` | `48 passed` |
| 5 (rojo) | `0bdbff7` | `churn/gold.py tests/test_gold.py` | `25 failed, 51 passed` |
| 5 (verde) | `1ac8fd4` | `churn/gold.py jobs/u3_batch.py` | `76 passed` |
| 5 (lanzador) | `dba4ac9` | `jobs/lanzar_batch.sh` | — |
| 6 | `2594e91` | `jobs/cargar_bq.sh` | — |

Ejemplo (Paso 3):

```
git checkout 47c27b5 -- .gitignore pytest.ini requirements-dev.txt churn tests && git status -s
```

## Por qué rojo y después verde

El laboratorio sigue desarrollo guiado por pruebas (TDD). Primero llegan las reglas con las funciones vacías, y las pruebas fallan: eso es el **rojo**. Después llega el código, y las mismas reglas pasan: eso es el **verde**. Por eso tu historial debe mostrar el commit en rojo **antes** del verde. Es parte de la evaluación.

## Requisitos

- Cloud Shell del proyecto del curso, con tu cuenta.
- Entorno virtual con PySpark 3.5.3 y Java 17, igual que el runtime 2.2 de Spark serverless. Se crea en el Paso 2.6 de la guía.
- Los valores de tu grupo (lake, dataset, cuenta de servicio, secreto y número de grupo), que te envía la profesora.

## Lo que cambias y lo que no

- **No cambias el código.** Es el mismo para todos los grupos.
- **Sí cambias los valores en los comandos:** tu lake, tu dataset, tu cuenta de servicio, tu grupo y el id de tu batch. La guía los marca con "Ojo".
- El id del batch nunca se repite: si lanzas otra vez, cambia `-01` por `-02`.

## Seguridad

- Este repositorio no contiene tokens, contraseñas ni llaves. El tuyo tampoco debe contenerlos.
- El token de GitHub de tu grupo se guarda solo en Secret Manager (Paso 1 de la guía).
- Antes de entregar, corre la comprobación del Paso 1.7: el conteo debe ser 0.

## Documentos del curso

- Guía del laboratorio paso a paso (U3).
- Pautas de trabajo en GCP y nomenclatura (U3 a U8).
- Presentación de la Unidad 3.

---

Profesora: Diana Jaimes · Procesamiento de Datos en la Nube · 2026-2
