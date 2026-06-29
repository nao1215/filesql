# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

[English](../../README.md) | [Русский](../ru/README.md) | [中文](../zh-cn/README.md) | [한국어](../ko/README.md) | [日本語](../ja/README.md) | [Français](../fr/README.md)

![logo](../image/filesql-logo.png)

**filesql** es un controlador SQL para Go que te permite consultar archivos CSV, TSV, LTSV, Parquet y Excel (XLSX) usando la sintaxis SQL de SQLite3. Carga tus archivos en una base de datos SQLite en memoria por ti, para que escribas SQL contra tus archivos sin un paso de importación manual, una definición de esquema, ni un servidor de base de datos que ejecutar.

**¿Quieres probar las capacidades de filesql?** ¡Prueba **[sqly](https://github.com/nao1215/sqly)** - una herramienta de línea de comandos que utiliza filesql para ejecutar fácilmente consultas SQL contra archivos CSV, TSV, LTSV y Excel directamente desde tu shell! ¡Es la forma perfecta de experimentar el poder de filesql en acción!

## ¿Por qué filesql?

Esta librería nació de la experiencia de mantener dos herramientas CLI separadas - [sqly](https://github.com/nao1215/sqly) y [sqluv](https://github.com/nao1215/sqluv). Ambas herramientas compartían una característica común: ejecutar consultas SQL contra archivos CSV, TSV y otros formatos.

En lugar de mantener código duplicado en ambos proyectos, extrajimos la funcionalidad principal en este controlador SQL reutilizable. ¡Ahora, cualquier desarrollador de Go puede aprovechar esta capacidad en sus propias aplicaciones!

## Características

- Interfaz SQL SQLite3 - Usa el poderoso dialecto SQL de SQLite3 para consultar tus archivos
- Múltiples formatos de archivo - Soporte para archivos CSV, TSV, LTSV, Parquet y Excel (XLSX)
- Soporte de compresión - Maneja automáticamente archivos comprimidos .gz, .bz2, .xz, .zst, .z, .snappy, .s2 y .lz4
- Procesamiento de flujos - Maneja eficientemente archivos grandes a través de streaming con tamaños de chunk configurables
- Fuentes de entrada flexibles - Soporte para rutas de archivos, directorios, io.Reader y embed.FS
- Configuración cero - No se requiere servidor de base de datos, todo funciona en memoria
- Auto-guardado - Persiste automáticamente los cambios en archivos
- Multiplataforma - Funciona perfectamente en Linux, macOS y Windows
- Impulsado por SQLite3 - Construido sobre el robusto motor SQLite3 para procesamiento SQL confiable

## Formatos de archivo soportados

| Extensión | Formato | Descripción |
|-----------|--------|-------------|
| `.csv` | CSV | Valores separados por comas |
| `.tsv` | TSV | Valores separados por tabulaciones |
| `.ltsv` | LTSV | Valores con etiquetas separados por tabulaciones |
| `.parquet` | Parquet | Formato columnar Apache Parquet |
| `.xlsx` | Excel XLSX | Formato de libro de Excel de Microsoft |
| `.json` | JSON | Formato JSON (use `json_extract()` para acceder a campos) |
| `.jsonl` | JSONL | Formato JSON Lines (un objeto JSON por línea) |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz`, `.parquet.gz`, `.xlsx.gz`, `.json.gz`, `.jsonl.gz` | Compresión Gzip | Archivos comprimidos con Gzip |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2`, `.parquet.bz2`, `.xlsx.bz2`, `.json.bz2`, `.jsonl.bz2` | Compresión Bzip2 | Archivos comprimidos con Bzip2 |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz`, `.parquet.xz`, `.xlsx.xz`, `.json.xz`, `.jsonl.xz` | Compresión XZ | Archivos comprimidos con XZ |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst`, `.parquet.zst`, `.xlsx.zst`, `.json.zst`, `.jsonl.zst` | Compresión Zstandard | Archivos comprimidos con Zstandard |
| `.csv.z`, `.tsv.z`, `.ltsv.z`, `.parquet.z`, `.xlsx.z`, `.json.z`, `.jsonl.z` | Compresión Zlib | Archivos comprimidos con Zlib |
| `.csv.snappy`, `.tsv.snappy`, `.ltsv.snappy`, `.parquet.snappy`, `.xlsx.snappy`, `.json.snappy`, `.jsonl.snappy` | Compresión Snappy | Archivos comprimidos con Snappy |
| `.csv.s2`, `.tsv.s2`, `.ltsv.s2`, `.parquet.s2`, `.xlsx.s2`, `.json.s2`, `.jsonl.s2` | Compresión S2 | Archivos comprimidos con S2 (compatible con Snappy) |
| `.csv.lz4`, `.tsv.lz4`, `.ltsv.lz4`, `.parquet.lz4`, `.xlsx.lz4`, `.json.lz4`, `.jsonl.lz4` | Compresión LZ4 | Archivos comprimidos con LZ4 |
| `.fed` | Fedwire | Archivos de mensajes Fedwire heredados (**Experimental**) |

## Instalación

```bash
go get github.com/nao1215/filesql
```

## Requisitos

- **Versión de Go**: 1.25 o posterior
- **Sistemas Operativos Soportados**:
  - Linux
  - macOS  
  - Windows

## Inicio rápido

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

### Soporte JSON / JSONL

Los archivos JSON y JSONL se almacenan como JSON sin procesar en una columna `data` de tipo TEXT. Use la función `json_extract()` de SQLite para consultar campos:

```go
db, err := filesql.OpenContext(ctx, "users.json")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

rows, err := db.QueryContext(ctx, `
    SELECT json_extract(data, '$.name') AS name,
           json_extract(data, '$.age') AS age
    FROM users
    WHERE json_extract(data, '$.age') > 25
`)
```

## Uso avanzado

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

### Logger Personalizado

filesql soporta logging conectable a través de la interfaz `Logger`. Por defecto, se usa un logger no-op con cero sobrecarga de rendimiento. Puedes inyectar tu propio logger (por ejemplo, `slog`) para depuración y monitoreo.

```go
import (
    "log/slog"
    "os"
    "github.com/nao1215/filesql"
)

// Crear un logger slog
slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug,
}))

// Envolverlo con SlogAdapter y pasarlo al builder
logger := filesql.NewSlogAdapter(slogLogger)

validatedBuilder, err := filesql.NewBuilder().
    WithLogger(logger).
    AddPath("data.csv").
    Build(ctx)
```

#### Interfaz Logger

```go
type Logger interface {
    Debug(msg string, args ...any)
    Info(msg string, args ...any)
    Warn(msg string, args ...any)
    Error(msg string, args ...any)
    With(args ...any) Logger
}
```

#### Logger con Contexto

Para logging con contexto, usa `ContextLogger`:

```go
type ContextLogger interface {
    Logger
    DebugContext(ctx context.Context, msg string, args ...any)
    InfoContext(ctx context.Context, msg string, args ...any)
    WarnContext(ctx context.Context, msg string, args ...any)
    ErrorContext(ctx context.Context, msg string, args ...any)
}

// Usa SlogContextAdapter para logging con contexto
logger := filesql.NewSlogContextAdapter(slogLogger)
```

#### Rendimiento

| Tipo de Logger | Rendimiento | Memoria |
|----------------|-------------|---------|
| nopLogger (por defecto) | ~0.2 ns/op | 0 B/op |
| SlogAdapter | ~1000 ns/op | ~630 B/op |

El logger no-op por defecto tiene prácticamente cero sobrecarga, haciendo seguro dejar llamadas de logging en código de producción.

## Reglas de nomenclatura de tablas

filesql deriva automáticamente los nombres de las tablas de las rutas de archivo:

- `users.csv` → tabla `users`
- `data.tsv.gz` → tabla `data`
- `/path/to/sales.csv` → tabla `sales`
- `products.ltsv.bz2` → tabla `products`
- `analytics.parquet` → tabla `analytics`
- `sales.xlsx` (con hojas 'Q1', 'Q2') → tablas `sales_Q1`, `sales_Q2`

## Notas importantes

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
- Todos los datos se cargan en una base de datos SQLite en memoria, así que planifica una memoria aproximadamente proporcional al tamaño del conjunto de datos

#### Memoria y streaming
filesql transmite arreglos CSV, TSV y JSON en chunks durante la carga, por lo que el parser en sí no retiene el archivo completo de una vez. Los demás formatos se leen completamente en memoria durante la carga porque su estructura lo requiere:

- LTSV, valores JSON/JSONL que no son arreglos, Parquet (necesita acceso aleatorio) y Excel (XLSX, basado en ZIP) se leen por completo antes de la carga.

De cualquier manera, las filas analizadas terminan en la base de datos SQLite en memoria, por lo que el uso total de memoria está determinado por el tamaño del conjunto de datos, no solo por el tamaño del chunk. Para datos más grandes que la memoria disponible, divide los archivos previamente o carga un subconjunto en lugar de depender únicamente del streaming.

## Benchmark

Rendimiento con un **archivo CSV de 100,000 filas**:

| Métrica | Valor |
|---------|-------|
| Tiempo de ejecución | ~430 ms |
| Uso de memoria | ~141 MB |

Ejecuta los benchmarks tú mismo:
```bash
make benchmark
```

### Concurrencia
El `*sql.DB` devuelto por `Open`/`OpenContext` se puede compartir de forma segura entre goroutines. Está respaldado por una única conexión SQLite en memoria, por lo que filesql fija el pool a una sola conexión (`SetMaxOpenConns(1)`) y `database/sql` serializa el acceso por ti:

```go
// Seguro: compartir un único *sql.DB entre goroutines.
db, err := filesql.Open("data.csv")
if err != nil {
    return err
}
defer db.Close()

var wg sync.WaitGroup
for range 8 {
    wg.Go(func() {
        rows, err := db.Query("SELECT * FROM data")
        // ... usar rows ...
    })
}
wg.Wait()
```

Ten en cuenta que, como el acceso se serializa a través de una única conexión, las consultas no se ejecutan en paralelo; la concurrencia es segura pero no más rápida. Si necesitas paralelismo real, abre un `*sql.DB` separado por goroutine.

> Cuando usas `LoadInto` con tu propio `*sql.DB`, eres responsable de la configuración del pool. Para una base de datos en memoria, llama a `db.SetMaxOpenConns(1)` tú mismo, porque los datos de SQLite `:memory:` son privados para cada conexión.

### Soporte de Excel (XLSX)
- **Estructura 1-Hoja-1-Tabla**: Cada hoja en un libro de Excel se convierte en una tabla SQL separada
- **Nomenclatura de tablas**: Los nombres de las tablas SQL siguen el formato `{nombre_archivo}_{nombre_hoja}` (ej., "ventas_T1", "ventas_T2")
- **Procesamiento de fila de encabezado**: La primera fila de cada hoja se convierte en los encabezados de columna para esa tabla
- **Operaciones SQL estándar**: Consulta cada hoja independientemente o usa JOINs para combinar datos entre hojas
- **Requisitos de memoria**: Los archivos XLSX requieren carga completa en memoria debido a la estructura de formato basado en ZIP, incluso durante operaciones de streaming
- **Carga completa en memoria**: Los archivos XLSX se cargan completamente en memoria debido a su estructura ZIP, y se procesan todas las hojas (no solo la primera). Los analizadores de streaming de CSV/TSV no son aplicables a archivos XLSX
- **Funcionalidad de exportación**: Al exportar a formato XLSX, los nombres de tabla se convierten automáticamente en nombres de hoja
- **Soporte de compresión**: Soporte completo para archivos XLSX comprimidos (.xlsx.gz, .xlsx.bz2, .xlsx.xz, .xlsx.zst, .xlsx.z, .xlsx.snappy, .xlsx.s2, .xlsx.lz4)

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

### Soporte Fedwire - Experimental

> **Advertencia**: El soporte de archivos Fedwire es **experimental**. La API puede cambiar en futuras versiones.

Los archivos de mensajes Fedwire heredados (`.fed`) pueden cargarse, consultarse, modificarse y exportarse de vuelta al formato Fedwire. Cada archivo Fedwire contiene un único FEDWireMessage y se convierte en una única tabla plana con aproximadamente 326 columnas.

| Nombre de tabla | Descripción |
|----------------|-------------|
| `{nombre_archivo}_message` | Tabla plana con todos los campos de FEDWireMessage (~326 columnas, 1 fila) |

Todas las columnas son de tipo TEXT ya que el formato wire almacena todos los valores como cadenas de ancho fijo.

#### Limitaciones

**Solo UPDATE**: Solo se admiten operaciones UPDATE en filas existentes para la edición de ida y vuelta. Las operaciones INSERT/DELETE en SQL no se reflejan en el archivo wire de salida.

**Sin nuevas secciones**: Las secciones de mensaje opcionales que no estaban presentes en el archivo original no pueden añadirse mediante modificaciones SQL.

**Compresión**: Los archivos Fedwire no soportan envoltorios de compresión (`.fed.gz`, etc.).

**Seguridad**: Los datos Fedwire contienen información bancaria sensible, incluyendo números de enrutamiento, números de cuenta, nombres y montos de transacciones. No registre ni exporte los datos de tablas wire de forma literal en entornos de producción.

#### Ejemplo

```go
ctx := context.Background()
db, err := filesql.OpenContext(ctx, "payment.fed")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Consultar información del remitente y receptor
rows, err := db.QueryContext(ctx, `
    SELECT sender_di_routing_number, receiver_di_routing_number, amount
    FROM payment_message
`)

// Modificar y exportar de vuelta al formato Fedwire
db.ExecContext(ctx, "UPDATE payment_message SET amount = '000005000000'")
filesql.DumpFedWire(ctx, db, "payment", "modified.fed")
```

## Ejemplos avanzados

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

## Ejemplos

El directorio [examples](../../examples) contiene código de ejemplo que demuestra varias características de filesql:

| Ejemplo | Descripción |
|---------|-------------|
| [basic](../../examples/basic) | Operaciones básicas de consulta CSV |
| [multi-format](../../examples/multi-format) | Trabajo con múltiples formatos de archivo (CSV, TSV, LTSV, Parquet) |
| [sqlc](../../examples/sqlc) | Integración con [sqlc](https://sqlc.dev/) - generador de código SQL con tipos seguros |
| [gorm](../../examples/gorm) | Integración con [GORM](https://gorm.io/) - ORM completo |
| [sqlx](../../examples/sqlx) | Integración con [sqlx](https://github.com/jmoiron/sqlx) - extensiones de database/sql |
| [bun](../../examples/bun) | Integración con [Bun](https://bun.uptrace.dev/) - ORM SQL-first |
| [squirrel](../../examples/squirrel) | Integración con [Squirrel](https://github.com/Masterminds/squirrel) - constructor de consultas SQL |
| [ent](../../examples/ent) | Integración con [Ent](https://entgo.io/) - framework de entidades de Facebook |

## Preprocesamiento de datos con fileprep

Para la validación y preprocesamiento de datos antes de consultar con filesql, recomendamos usar **[nao1215/fileprep](https://github.com/nao1215/fileprep)**.

fileprep es una biblioteca complementaria que proporciona:
- **Preprocesamiento basado en etiquetas de struct** (etiqueta `prep`): trim, minúsculas, mayúsculas, valores por defecto, y más
- **Validación basada en etiquetas de struct** (etiqueta `validate`): campos requeridos, validación de formato, validación entre campos
- **Integración perfecta con filesql**: Devuelve `io.Reader` para uso directo con el patrón Builder de filesql

```go
// Definir struct con etiquetas de preprocesamiento y validación
type User struct {
    // Name: eliminar espacios, requerir no vacío
    Name  string `prep:"trim" validate:"required"`
    // Email: eliminar espacios, convertir a minúsculas, validar formato de email
    Email string `prep:"trim,lowercase" validate:"required,email"`
    // Age: establecer valor por defecto si está vacío, validar rango 0-150
    Age   string `prep:"default=0" validate:"numeric,gte=0,lte=150"`
    // Role: eliminar espacios, mayúsculas, debe ser uno de los valores permitidos
    Role  string `prep:"trim,uppercase" validate:"oneof=ADMIN USER GUEST"`
}

func main() {
    // Datos CSV con entrada desordenada
    csvData := `name,email,age,role
  John Doe  ,JOHN@EXAMPLE.COM,25,admin
Alice,alice@example.com,,user`

    // Crear procesador y procesar el CSV
    processor := fileprep.NewProcessor(fileprep.FileTypeCSV)
    var users []User

    reader, result, err := processor.Process(strings.NewReader(csvData), &users)
    if err != nil {
        log.Fatal(err)
    }

    // Verificar resultados de validación
    fmt.Printf("Procesados: %d filas, Válidos: %d filas\n", result.RowCount, result.ValidRowCount)
    if result.HasErrors() {
        for _, e := range result.ValidationErrors() {
            log.Printf("Fila %d, Columna %s: %s", e.Row, e.Column, e.Message)
        }
    }

    // Pasar datos preprocesados a filesql
    // Los datos ahora están limpios: espacios eliminados, emails en minúsculas, valores por defecto aplicados
    ctx := context.Background()
    db, err := filesql.NewBuilder().
        AddReader(reader, "users", filesql.FileTypeCSV).
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Consultar los datos limpios
    rows, _ := db.QueryContext(ctx, "SELECT * FROM users WHERE role = 'ADMIN'")
    // ...
}
```

Para la lista completa de opciones de preprocesamiento y validación, consulte la [documentación de fileprep](https://github.com/nao1215/fileprep).

## Proyectos Relacionados

¿Usas filesql en tu proyecto? ¡Nos encantaría saberlo! Por favor [abre un issue](https://github.com/nao1215/filesql/issues) para informarnos, y añadiremos tu proyecto a la lista a continuación.

### Bibliotecas relacionadas

| Proyecto | Descripción |
|----------|-------------|
| [nao1215/fileprep](https://github.com/nao1215/fileprep) | Biblioteca de preprocesamiento de datos con validación mediante etiquetas de struct |

### Herramientas CLI que usan filesql

| Proyecto | Descripción |
|----------|-------------|
| [nao1215/sqly](https://github.com/nao1215/sqly) | Shell interactivo para ejecutar consultas SQL contra archivos CSV, TSV, LTSV, JSON y Excel |
| [kanmu/gocon2025-ctf](https://github.com/kanmu/gocon2025-ctf) | Repositorio CTF de Go Conference 2025 (en japonés) |

## Contribuir

¡Las contribuciones son bienvenidas! Por favor, consulta la [Guía de Contribución](../../CONTRIBUTING.md) para más detalles.

## Soporte

Si encuentras útil este proyecto, por favor considera:

- Darle una estrella en GitHub - ayuda a otros a descubrir el proyecto
- [Convertirte en patrocinador](https://github.com/sponsors/nao1215) - tu apoyo mantiene el proyecto vivo y motiva el desarrollo continuo

Tu apoyo, ya sea a través de estrellas, patrocinios o contribuciones, es lo que impulsa este proyecto hacia adelante. ¡Gracias!

### Historial de Estrellas

[![Star History Chart](https://api.star-history.com/svg?repos=nao1215/filesql&type=date&legend=top-left)](https://www.star-history.com/#nao1215/filesql&Date)

## Licencia

Este proyecto está licenciado bajo la Licencia MIT - consulta el archivo [LICENSE](../../LICENSE) para más detalles.
