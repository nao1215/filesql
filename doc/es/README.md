# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

[English](../../README.md) | [Русский](../ru/README.md) | [中文](../zh-cn/README.md) | [한국어](../ko/README.md) | [日本語](../ja/README.md) | [Français](../fr/README.md)

![logo](../image/filesql-logo.png)

**filesql** es un controlador SQL para Go que te permite consultar archivos CSV, TSV, LTSV, Parquet y Excel (XLSX) usando la sintaxis SQL de SQLite3. ¡Consulta tus archivos de datos directamente sin importaciones o transformaciones!

**¿Quieres probar las capacidades de filesql?** ¡Prueba **[sqly](https://github.com/nao1215/sqly)** - una herramienta de línea de comandos que utiliza filesql para ejecutar fácilmente consultas SQL contra archivos CSV, TSV, LTSV y Excel directamente desde tu shell! ¡Es la forma perfecta de experimentar el poder de filesql en acción!

## 🎯 ¿Por qué filesql?

Esta librería nació de la experiencia de mantener dos herramientas CLI separadas - [sqly](https://github.com/nao1215/sqly) y [sqluv](https://github.com/nao1215/sqluv). Ambas herramientas compartían una característica común: ejecutar consultas SQL contra archivos CSV, TSV y otros formatos.

En lugar de mantener código duplicado en ambos proyectos, extrajimos la funcionalidad principal en este controlador SQL reutilizable. ¡Ahora, cualquier desarrollador de Go puede aprovechar esta capacidad en sus propias aplicaciones!

## ✨ Características

- 🔍 **Interfaz SQL SQLite3** - Usa el poderoso dialecto SQL de SQLite3 para consultar tus archivos
- 📁 **Múltiples formatos de archivo** - Soporte para archivos CSV, TSV, LTSV, Parquet y Excel (XLSX)
- 🗜️ **Soporte de compresión** - Maneja automáticamente archivos comprimidos .gz, .bz2, .xz y .zst
- 🌊 **Procesamiento de flujos** - Maneja eficientemente archivos grandes a través de streaming con tamaños de chunk configurables
- 📖 **Fuentes de entrada flexibles** - Soporte para rutas de archivos, directorios, io.Reader y embed.FS
- 🚀 **Configuración cero** - No se requiere servidor de base de datos, todo funciona en memoria
- 💾 **Auto-guardado** - Persiste automáticamente los cambios en archivos
- 🌍 **Multiplataforma** - Funciona perfectamente en Linux, macOS y Windows
- ⚡ **Impulsado por SQLite3** - Construido sobre el robusto motor SQLite3 para procesamiento SQL confiable

## 📋 Formatos de archivo soportados

| Extensión | Formato | Descripción |
|-----------|--------|-------------|
| `.csv` | CSV | Valores separados por comas |
| `.tsv` | TSV | Valores separados por tabulaciones |
| `.ltsv` | LTSV | Valores con etiquetas separados por tabulaciones |
| `.parquet` | Parquet | Formato columnar Apache Parquet |
| `.xlsx` | Excel XLSX | Formato de libro de Excel de Microsoft |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz`, `.parquet.gz`, `.xlsx.gz` | Compresión Gzip | Archivos comprimidos con Gzip |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2`, `.parquet.bz2`, `.xlsx.bz2` | Compresión Bzip2 | Archivos comprimidos con Bzip2 |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz`, `.parquet.xz`, `.xlsx.xz` | Compresión XZ | Archivos comprimidos con XZ |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst`, `.parquet.zst`, `.xlsx.zst` | Compresión Zstandard | Archivos comprimidos con Zstandard |

## 📦 Instalación

```bash
go get github.com/nao1215/filesql
```

## 🔧 Requisitos

- **Versión de Go**: 1.24 o posterior
- **Sistemas Operativos Soportados**:
  - Linux
  - macOS  
  - Windows

## 🚀 Inicio rápido

### Uso simple

La forma recomendada de empezar es con `OpenContext` para un manejo adecuado de timeouts:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    
    "github.com/nao1215/filesql"
)

func main() {
    // Crear contexto con timeout para operaciones con archivos grandes
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    // Abrir un archivo CSV como una base de datos
    db, err := filesql.OpenContext(ctx, "data.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Consultar los datos (nombre de tabla = nombre de archivo sin extensión)
    rows, err := db.QueryContext(ctx, "SELECT * FROM data WHERE age > 25")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    // Procesar resultados
    for rows.Next() {
        var name string
        var age int
        if err := rows.Scan(&name, &age); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("Nombre: %s, Edad: %d\n", name, age)
    }
}
```

### Múltiples archivos y formatos

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Abrir múltiples archivos a la vez (incluyendo Parquet)
db, err := filesql.OpenContext(ctx, "users.csv", "orders.tsv", "logs.ltsv.gz", "analytics.parquet")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Unir datos de diferentes formatos de archivo
rows, err := db.QueryContext(ctx, `
    SELECT u.name, o.order_date, l.event, a.metrics
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN logs l ON u.id = l.user_id
    JOIN analytics a ON u.id = a.user_id
    WHERE o.order_date > '2024-01-01'
`)
```

### Trabajar con directorios

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Cargar todos los archivos soportados de un directorio (recursivo)
db, err := filesql.OpenContext(ctx, "/path/to/data/directory")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Ver qué tablas están disponibles
rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
```

## 🔧 Uso avanzado

### Patrón Builder

Para escenarios avanzados, usa el patrón builder:

```go
package main

import (
    "context"
    "embed"
    "log"
    
    "github.com/nao1215/filesql"
)

//go:embed data/*.csv
var embeddedFiles embed.FS

func main() {
    ctx := context.Background()
    
    // Configurar fuentes de datos con builder
    validatedBuilder, err := filesql.NewBuilder().
        AddPath("local_file.csv").      // Archivo local
        AddFS(embeddedFiles).           // Archivos embebidos
        SetDefaultChunkSize(5000). // 5000 filas por chunk
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }
    
    db, err := validatedBuilder.Open(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Consultar todas las fuentes de datos
    rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
}
```

### Funciones de Auto-guardado

#### Auto-guardado al cerrar la base de datos

```go
// Auto-guardar cambios cuando se cierra la base de datos
validatedBuilder, err := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSave("./backup"). // Guardar en directorio de backup
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close() // Los cambios se guardan automáticamente aquí

// Hacer cambios
db.Exec("UPDATE data SET status = 'processed' WHERE id = 1")
db.Exec("INSERT INTO data (name, age) VALUES ('Juan', 30)")
```

#### Auto-guardado en commit de transacción

```go
// Auto-guardar después de cada transacción
validatedBuilder, err := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSaveOnCommit(""). // Vacío = sobrescribir archivos originales
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Los cambios se guardan después de cada commit
tx, _ := db.Begin()
tx.Exec("UPDATE data SET status = 'processed' WHERE id = 1")
tx.Commit() // El auto-guardado ocurre aquí
```

### Trabajar con io.Reader y datos de red

```go
import (
    "net/http"
    "github.com/nao1215/filesql"
)

// Cargar datos desde respuesta HTTP
resp, err := http.Get("https://example.com/data.csv")
if err != nil {
    log.Fatal(err)
}
defer resp.Body.Close()

validatedBuilder, err := filesql.NewBuilder().
    AddReader(resp.Body, "remote_data", filesql.FileTypeCSV).
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Consultar datos remotos
rows, err := db.QueryContext(ctx, "SELECT * FROM remote_data LIMIT 10")
```

### Exportación manual de datos

Si prefieres control manual sobre el guardado:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "data.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Hacer modificaciones
db.Exec("UPDATE data SET status = 'processed'")

// Exportar cambios manualmente
err = filesql.DumpDatabase(db, "./output")
if err != nil {
    log.Fatal(err)
}

// O con formato y compresión personalizados
options := filesql.NewDumpOptions().
    WithFormat(filesql.OutputFormatTSV).
    WithCompression(filesql.CompressionGZ)
err = filesql.DumpDatabase(db, "./output", options)

// Exportar a formato Parquet (cuando esté disponible)
parquetOptions := filesql.NewDumpOptions().
    WithFormat(filesql.OutputFormatParquet)
// Nota: La funcionalidad de exportación está implementada (compresión externa no soportada, use la compresión integrada de Parquet)
```

## 📝 Reglas de nomenclatura de tablas

filesql deriva automáticamente los nombres de las tablas de las rutas de archivo:

- `users.csv` → tabla `users`
- `data.tsv.gz` → tabla `data`
- `/path/to/sales.csv` → tabla `sales`
- `products.ltsv.bz2` → tabla `products`
- `analytics.parquet` → tabla `analytics`
- `sales.xlsx` (con hojas 'Q1', 'Q2') → tablas `sales_Q1`, `sales_Q2`

## ⚠️ Notas importantes

### Sintaxis SQL
Dado que filesql usa SQLite3 como su motor subyacente, toda la sintaxis SQL sigue el [dialecto SQL de SQLite3](https://www.sqlite.org/lang.html). Esto incluye:
- Funciones (ej., `date()`, `substr()`, `json_extract()`)
- Funciones de ventana
- Expresiones de tabla común (CTE)
- Triggers y views

### Modificaciones de datos
- Las operaciones `INSERT`, `UPDATE` y `DELETE` afectan la base de datos en memoria
- **Los archivos originales permanecen inalterados por defecto**
- Usa funciones de auto-guardado o `DumpDatabase()` para persistir cambios
- Esto hace que sea seguro experimentar con transformaciones de datos

### Consejos de rendimiento
- Usa `OpenContext()` con timeouts para archivos grandes
- Configura tamaños de chunk (filas por chunk) con `SetDefaultChunkSize()` para optimización de memoria
- Una sola conexión SQLite funciona mejor para la mayoría de escenarios
- Usa streaming para archivos más grandes que la memoria disponible

### Limitaciones de concurrencia
⚠️ **IMPORTANTE**: Esta biblioteca **NO es thread-safe** y tiene **limitaciones de concurrencia**:
- **NO** compartas conexiones de base de datos entre goroutines
- **NO** realices operaciones concurrentes en la misma instancia de base de datos
- **NO** llames `db.Close()` mientras hay consultas activas en otras goroutines
- Usa instancias de base de datos separadas para operaciones concurrentes si es necesario
- Las condiciones de carrera pueden causar fallos de segmentación o corrupción de datos

**Patrón recomendado para acceso concurrente**:
```go
// ✅ BUENO: Instancias de base de datos separadas por goroutine
func processFileConcurrently(filename string) error {
    db, err := filesql.Open(filename)  // Cada goroutine obtiene su propia instancia
    if err != nil {
        return err
    }
    defer db.Close()
    
    // Seguro de usar dentro de esta goroutine
    return processData(db)
}

// ❌ MALO: Compartir instancia de base de datos entre goroutines
var sharedDB *sql.DB  // Esto causará condiciones de carrera
```

### Soporte de Excel (XLSX)
- **Estructura 1-Hoja-1-Tabla**: Cada hoja en un libro de Excel se convierte en una tabla SQL separada
- **Nomenclatura de tablas**: Los nombres de las tablas SQL siguen el formato `{nombre_archivo}_{nombre_hoja}` (ej., "ventas_T1", "ventas_T2")
- **Procesamiento de fila de encabezado**: La primera fila de cada hoja se convierte en los encabezados de columna para esa tabla
- **Operaciones SQL estándar**: Consulta cada hoja independientemente o usa JOINs para combinar datos entre hojas
- **Requisitos de memoria**: Los archivos XLSX requieren carga completa en memoria debido a la estructura de formato basado en ZIP, incluso durante operaciones de streaming
- **Carga completa en memoria**: Los archivos XLSX se cargan completamente en memoria debido a su estructura ZIP, y se procesan todas las hojas (no solo la primera). Los analizadores de streaming de CSV/TSV no son aplicables a archivos XLSX
- **Funcionalidad de exportación**: Al exportar a formato XLSX, los nombres de tabla se convierten automáticamente en nombres de hoja
- **Soporte de compresión**: Soporte completo para archivos XLSX comprimidos (.xlsx.gz, .xlsx.bz2, .xlsx.xz, .xlsx.zst)

#### Ejemplo de estructura de archivo Excel
```
Archivo Excel con múltiples hojas:

┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Hoja1       │    │ Hoja2       │    │ Hoja3       │
│ Nombre Edad │    │ Producto    │    │ Region      │
│ Ana     25  │    │ Laptop      │    │ Norte       │
│ Luis    30  │    │ Mouse       │    │ Sur         │
└─────────────┘    └─────────────┘    └─────────────┘

Resulta en 3 tablas SQL separadas:

ventas_Hoja1:           ventas_Hoja2:           ventas_Hoja3:
┌────────┬──────┐       ┌──────────┐            ┌────────┐
│ Nombre │ Edad │       │ Producto │            │ Region │
├────────┼──────┤       ├──────────┤            ├────────┤
│ Ana    │   25 │       │ Laptop   │            │ Norte  │
│ Luis   │   30 │       │ Mouse    │            │ Sur    │
└────────┴──────┘       └──────────┘            └────────┘

Ejemplos SQL:
SELECT * FROM ventas_Hoja1 WHERE Edad > 27;
SELECT h1.Nombre, h2.Producto FROM ventas_Hoja1 h1 
  JOIN ventas_Hoja2 h2 ON h1.rowid = h2.rowid;
```

### Soporte de Parquet
- **Lectura**: Soporte completo para archivos Apache Parquet con tipos de datos complejos
- **Escritura**: La funcionalidad de exportación está implementada (compresión externa no soportada, use la compresión integrada de Parquet)
- **Mapeo de tipos**: Los tipos Parquet se mapean a tipos SQLite
- **Compresión**: Se utiliza la compresión integrada de Parquet en lugar de compresión externa
- **Datos grandes**: Los archivos Parquet se procesan eficientemente con el formato columnar de Arrow

## 🎨 Ejemplos avanzados

### Consultas SQL complejas

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "employees.csv", "departments.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Usar características avanzadas de SQLite
query := `
    WITH dept_stats AS (
        SELECT 
            department_id,
            AVG(salary) as avg_salary,
            COUNT(*) as emp_count
        FROM employees
        GROUP BY department_id
    )
    SELECT 
        e.name,
        e.salary,
        d.name as department,
        ds.avg_salary as dept_avg,
        RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) as salary_rank
    FROM employees e
    JOIN departments d ON e.department_id = d.id
    JOIN dept_stats ds ON e.department_id = ds.department_id
    WHERE e.salary > ds.avg_salary * 0.8
    ORDER BY d.name, salary_rank
`

rows, err := db.QueryContext(ctx, query)
```

### Contexto y cancelación

```go
import (
    "context"
    "time"
)

// Establecer timeout para operaciones con archivos grandes
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
defer cancel()

db, err := filesql.OpenContext(ctx, "huge_dataset.csv.gz")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Consulta con contexto para soporte de cancelación
rows, err := db.QueryContext(ctx, "SELECT * FROM huge_dataset WHERE status = 'active'")
```

## 🤝 Contribuir

¡Las contribuciones son bienvenidas! Por favor, consulta la [Guía de Contribución](../../CONTRIBUTING.md) para más detalles.

## 💖 Soporte

Si encuentras útil este proyecto, por favor considera:

- ⭐ Darle una estrella en GitHub - ayuda a otros a descubrir el proyecto
- 💝 [Convertirte en patrocinador](https://github.com/sponsors/nao1215) - tu apoyo mantiene el proyecto vivo y motiva el desarrollo continuo

Tu apoyo, ya sea a través de estrellas, patrocinios o contribuciones, es lo que impulsa este proyecto hacia adelante. ¡Gracias!

## 📄 Licencia

Este proyecto está licenciado bajo la Licencia MIT - consulta el archivo [LICENSE](../../LICENSE) para más detalles.