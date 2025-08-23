# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

**filesql** es un controlador SQL para Go que te permite consultar archivos CSV, TSV y LTSV usando la sintaxis SQL de SQLite3. ¡Consulta tus archivos de datos directamente sin importaciones o transformaciones!

## 🎯 ¿Por qué filesql?

Esta librería nació de la experiencia de mantener dos herramientas CLI separadas - [sqly](https://github.com/nao1215/sqly) y [sqluv](https://github.com/nao1215/sqluv). Ambas herramientas compartían una característica común: ejecutar consultas SQL contra archivos CSV, TSV y otros formatos de archivo.

En lugar de mantener código duplicado en ambos proyectos, extrajimos la funcionalidad principal en este controlador SQL reutilizable. ¡Ahora, cualquier desarrollador de Go puede aprovechar esta capacidad en sus propias aplicaciones!

## ✨ Características

- 🔍 **Interfaz SQL SQLite3** - Usa el poderoso dialecto SQL de SQLite3 para consultar tus archivos
- 📁 **Múltiples formatos de archivo** - Soporte para archivos CSV, TSV y LTSV
- 🗜️ **Soporte de compresión** - Maneja automáticamente archivos comprimidos .gz, .bz2, .xz y .zst
- 🚀 **Configuración cero** - No se requiere servidor de base de datos, todo funciona en memoria
- 🌍 **Multiplataforma** - Funciona perfectamente en Linux, macOS y Windows
- 💾 **Impulsado por SQLite3** - Construido sobre el robusto motor SQLite3 para procesamiento SQL confiable

## 📋 Formatos de archivo soportados

| Extensión | Formato | Descripción |
|-----------|--------|-------------|
| `.csv` | CSV | Valores separados por comas |
| `.tsv` | TSV | Valores separados por tabulaciones |
| `.ltsv` | LTSV | Valores con etiquetas separados por tabulaciones |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz` | Compresión Gzip | Archivos comprimidos con Gzip |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2` | Compresión Bzip2 | Archivos comprimidos con Bzip2 |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz` | Compresión XZ | Archivos comprimidos con XZ |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst` | Compresión Zstandard | Archivos comprimidos con Zstandard |


## 📦 Instalación

```bash
go get github.com/nao1215/filesql
```

## 🚀 Inicio rápido

[El código de ejemplo está aquí](../../example_test.go).

### Uso básico

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    "github.com/nao1215/filesql"
)

func main() {
    // Abrir un archivo CSV como una base de datos
    db, err := filesql.Open("data.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Ejecutar consulta SQL (el nombre de la tabla se deriva del nombre del archivo sin extensión)
    rows, err := db.QueryContext(context.Background(), "SELECT * FROM data WHERE age > 25 ORDER BY name")
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
        fmt.Printf("Name: %s, Age: %d\n", name, age)
    }
}
```

### Abrir múltiples archivos

```go
// Abrir múltiples archivos en una sola base de datos
db, err := filesql.Open("users.csv", "orders.tsv", "products.ltsv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// ¡Unir datos de diferentes formatos de archivo!
rows, err := db.QueryContext(context.Background(), `
    SELECT u.name, o.order_date, p.product_name
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN products p ON o.product_id = p.id
    WHERE o.order_date > '2024-01-01'
`)
```

### Trabajar con directorios

```go
// Abrir todos los archivos soportados en un directorio (recursivamente)
db, err := filesql.Open("/path/to/data/directory")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Consultar todas las tablas cargadas
rows, err := db.QueryContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table'")
```

### Soporte de archivos comprimidos

```go
// Maneja automáticamente archivos comprimidos
db, err := filesql.Open("large_dataset.csv.gz", "archive.tsv.bz2")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Consultar datos comprimidos sin problemas
rows, err := db.QueryContext(context.Background(), "SELECT COUNT(*) FROM large_dataset")
```

### Reglas de nomenclatura de tablas

filesql deriva automáticamente los nombres de las tablas de las rutas de archivo:

```go
// Ejemplos de nomenclatura de tablas:
// "users.csv"           -> nombre de tabla: "users"
// "data.tsv"            -> nombre de tabla: "data"
// "logs.ltsv"           -> nombre de tabla: "logs"
// "archive.csv.gz"      -> nombre de tabla: "archive"
// "backup.tsv.bz2"      -> nombre de tabla: "backup"
// "/path/to/sales.csv"  -> nombre de tabla: "sales"

db, err := filesql.Open("employees.csv", "departments.tsv.gz")
if err != nil {
    log.Fatal(err)
}

// Usar los nombres de tabla derivados en consultas
rows, err := db.QueryContext(context.Background(), `
    SELECT * FROM employees 
    JOIN departments ON employees.dept_id = departments.id
`)
```

## ⚠️ Notas importantes

### Sintaxis SQL
Dado que filesql usa SQLite3 como su motor subyacente, toda la sintaxis SQL sigue el [dialecto SQL de SQLite3](https://www.sqlite.org/lang.html). Esto incluye:
- Funciones (ej., `date()`, `substr()`, `json_extract()`)
- Funciones de ventana
- Expresiones de tabla común (CTE)
- ¡Y mucho más!

### Modificaciones de datos
- Las operaciones `INSERT`, `UPDATE` y `DELETE` solo afectan la base de datos en memoria
- **Los archivos originales permanecen inalterados** - filesql nunca modifica tus archivos fuente
- Esto hace que sea seguro experimentar con transformaciones de datos

### Características SQL avanzadas

Dado que filesql usa SQLite3, puedes aprovechar todo su poder:

```go
db, err := filesql.Open("employees.csv", "departments.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Usar funciones de ventana, CTE y consultas complejas
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
        RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) as rank
    FROM employees e
    JOIN departments d ON e.department_id = d.id
    JOIN dept_stats ds ON e.department_id = ds.department_id
    WHERE e.salary > ds.avg_salary * 0.8
`

rows, err := db.QueryContext(context.Background(), query)
```

### Exportar datos modificados

Si necesitas persistir cambios hechos a la base de datos en memoria:

```go
db, err := filesql.Open("data.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Hacer modificaciones
_, err = db.ExecContext(context.Background(), "UPDATE data SET status = 'processed' WHERE status = 'pending'")
if err != nil {
    log.Fatal(err)
}

// Exportar los datos modificados a un nuevo directorio
err = filesql.DumpDatabase(db, "/path/to/output/directory")
if err != nil {
    log.Fatal(err)
}
```

## 💖 Soporte

Si encuentras útil este proyecto, por favor considera:

- ⭐ Darle una estrella en GitHub - ayuda a otros a descubrir el proyecto
- 💝 [Convertirte en patrocinador](https://github.com/sponsors/nao1215) - tu apoyo mantiene el proyecto vivo y motiva el desarrollo continuo

Tu apoyo, ya sea a través de estrellas, patrocinios o contribuciones, es lo que impulsa este proyecto hacia adelante. ¡Gracias!

## 📄 Licencia

Este proyecto está licenciado bajo la Licencia MIT - consulta el archivo [LICENSE](../../LICENSE) para más detalles.