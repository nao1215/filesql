# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

**filesql** — это драйвер SQL для Go, который позволяет выполнять запросы к файлам CSV, TSV и LTSV, используя синтаксис SQL SQLite3. Выполняйте запросы к вашим файлам данных напрямую без импорта или преобразований!

## 🎯 Почему filesql?

Эта библиотека родилась из опыта поддержки двух отдельных CLI-инструментов — [sqly](https://github.com/nao1215/sqly) и [sqluv](https://github.com/nao1215/sqluv). Оба инструмента имели общую функцию: выполнение SQL-запросов к файлам CSV, TSV и другим форматам.

Вместо поддержки дублирующегося кода в обоих проектах, мы извлекли основную функциональность в этот переиспользуемый SQL-драйвер. Теперь любой Go-разработчик может использовать эту возможность в своих приложениях!

## ✨ Возможности

- 🔍 **SQL-интерфейс SQLite3** — используйте мощный SQL-диалект SQLite3 для запросов к вашим файлам
- 📁 **Несколько форматов файлов** — поддержка файлов CSV, TSV и LTSV
- 🗜️ **Поддержка сжатия** — автоматически обрабатывает сжатые файлы .gz, .bz2, .xz и .zst
- 🌊 **Потоковая обработка** - Эффективно обрабатывает большие файлы через потоковую передачу с настраиваемыми размерами чанков
- 📖 **Гибкие источники ввода** - Поддержка путей к файлам, каталогов, io.Reader и embed.FS
- 🚀 **Нулевая настройка** — не требуется сервер базы данных, всё работает в памяти
- 🌍 **Кроссплатформенность** — безупречно работает на Linux, macOS и Windows
- 💾 **На базе SQLite3** — построен на надёжном движке SQLite3 для надёжной обработки SQL

## 📋 Поддерживаемые форматы файлов

| Расширение | Формат | Описание |
|-----------|--------|-------------|
| `.csv` | CSV | Значения, разделённые запятыми |
| `.tsv` | TSV | Значения, разделённые табуляцией |
| `.ltsv` | LTSV | Помеченные значения, разделённые табуляцией |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz` | Сжатие Gzip | Файлы, сжатые Gzip |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2` | Сжатие Bzip2 | Файлы, сжатые Bzip2 |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz` | Сжатие XZ | Файлы, сжатые XZ |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst` | Сжатие Zstandard | Файлы, сжатые Zstandard |


## 📦 Установка

```bash
go get github.com/nao1215/filesql
```

## 🚀 Быстрый старт

[Примеры кода находятся здесь](../../example_test.go).

### Простое использование (Файлы)

Для простого доступа к файлам используйте удобные функции `Open` или `OpenContext`:

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
    // Открываем CSV-файл как базу данных с контекстом
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    db, err := filesql.OpenContext(ctx, "data.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Выполняем SQL-запрос (имя таблицы происходит от имени файла без расширения)
    rows, err := db.QueryContext(ctx, "SELECT * FROM data WHERE age > 25 ORDER BY name")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    // Обрабатываем результаты
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

### Паттерн Builder (Требуется для fs.FS)

Для продвинутых случаев использования, таких как встроенные файлы (`go:embed`) или пользовательские файловые системы, используйте **паттерн Builder**:

```go
package main

import (
    "context"
    "embed"
    "io/fs"
    "log"
    
    "github.com/nao1215/filesql"
)

//go:embed data/*.csv data/*.tsv
var dataFS embed.FS

func main() {
    ctx := context.Background()
    
    // Используем паттерн Builder для встроенной файловой системы
    subFS, _ := fs.Sub(dataFS, "data")
    
    validatedBuilder, err := filesql.NewBuilder().
        AddPath("local_file.csv").  // Обычный файл
        AddFS(subFS).               // Встроенная файловая система
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }
    
    connection, err := validatedBuilder.Open(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer connection.Close()
    
    
    // Запросы к файлам из разных источников
    rows, err := connection.Query("SELECT name FROM sqlite_master WHERE type='table'")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    // Обрабатываем результаты...
}
```

### Открытие с поддержкой контекста

```go
// Открываем файлы с контролем таймаута
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "large_dataset.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Запрос с контекстом для поддержки отмены
rows, err := db.QueryContext(ctx, "SELECT * FROM large_dataset WHERE status = 'active'")
```

### Открытие нескольких файлов

```go
// Открываем несколько файлов в одной базе данных
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "users.csv", "orders.tsv", "products.ltsv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Объединяем данные из разных форматов файлов!
rows, err := db.QueryContext(ctx, `
    SELECT u.name, o.order_date, p.product_name
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN products p ON o.product_id = p.id
    WHERE o.order_date > '2024-01-01'
`)
```

### Работа с директориями

```go
// Открываем все поддерживаемые файлы в директории (рекурсивно)
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "/path/to/data/directory")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Запрашиваем все загруженные таблицы
rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
```

### Поддержка сжатых файлов

```go
// Автоматически обрабатывает сжатые файлы
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "large_dataset.csv.gz", "archive.tsv.bz2")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Выполняем запросы к сжатым данным без проблем
rows, err := db.QueryContext(ctx, "SELECT COUNT(*) FROM large_dataset")
```

### Правила именования таблиц

filesql автоматически определяет имена таблиц из путей к файлам:

```go
// Примеры именования таблиц:
// "users.csv"           -> имя таблицы: "users"
// "data.tsv"            -> имя таблицы: "data"
// "logs.ltsv"           -> имя таблицы: "logs"
// "archive.csv.gz"      -> имя таблицы: "archive"
// "backup.tsv.bz2"      -> имя таблицы: "backup"
// "/path/to/sales.csv"  -> имя таблицы: "sales"

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "employees.csv", "departments.tsv.gz")
if err != nil {
    log.Fatal(err)
}

// Используем определённые имена таблиц в запросах
rows, err := db.QueryContext(ctx, `
    SELECT * FROM employees 
    JOIN departments ON employees.dept_id = departments.id
`)
```

## ⚠️ Важные замечания

### SQL-синтаксис
Поскольку filesql использует SQLite3 в качестве базового движка, весь SQL-синтаксис соответствует [SQL-диалекту SQLite3](https://www.sqlite.org/lang.html). Это включает:
- Функции (например, `date()`, `substr()`, `json_extract()`)
- Оконные функции
- Общие табличные выражения (CTE)
- И многое другое!

### Модификация данных
- Операции `INSERT`, `UPDATE` и `DELETE` влияют на базу данных в памяти
- **Исходные файлы остаются неизменными по умолчанию** — filesql не изменяет ваши исходные файлы, если только вы не используете автосохранение
- Вы можете использовать **автосохранение** для автоматического сохранения изменений в файлы при закрытии или коммите
- Это делает безопасными эксперименты с преобразованием данных, предоставляя при этом опциональную персистентность

### Расширенные возможности SQL

Поскольку filesql использует SQLite3, вы можете использовать всю его мощь:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "employees.csv", "departments.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Используем оконные функции, CTE и сложные запросы
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

rows, err := db.QueryContext(ctx, query)
```

### Функция автосохранения

filesql предоставляет функцию автосохранения для автоматического сохранения изменений базы данных в файлы. Вы можете выбрать из двух вариантов времени сохранения:

#### Автосохранение при закрытии базы данных

Автоматически сохраняет изменения при закрытии соединения с базой данных (рекомендуется для большинства случаев использования):

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Включить автосохранение при закрытии
builder := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSave("./backup") // Сохранить в директорию резервных копий

validatedBuilder, err := builder.Build(ctx)
if err != nil {
    log.Fatal(err)
}


db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close() // Автосохранение запускается здесь

// Вносим изменения — они будут автоматически сохранены при закрытии
_, err = db.ExecContext(ctx, "UPDATE data SET status = 'processed' WHERE status = 'pending'")
_, err = db.ExecContext(ctx, "INSERT INTO data (name, status) VALUES ('New Record', 'active')")
```

#### Автосохранение при коммите транзакции

Автоматически сохраняет изменения после каждого коммита транзакции (для частого сохранения):

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Включить автосохранение при коммите — пустая строка означает перезапись исходных файлов
builder := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSaveOnCommit("") // Перезаписать исходные файлы

validatedBuilder, err := builder.Build(ctx)
if err != nil {
    log.Fatal(err)
}


db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Каждый коммит будет автоматически сохраняться в файлы
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    log.Fatal(err)
}

_, err = tx.ExecContext(ctx, "UPDATE data SET status = 'processed' WHERE id = 1")
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

err = tx.Commit() // Автосохранение запускается здесь
if err != nil {
    log.Fatal(err)
}
```

### Ручной экспорт данных (альтернатива автосохранению)

Если вы предпочитаете ручное управление тем, когда сохранять изменения в файлы, вместо использования автосохранения:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "data.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Вносим изменения
_, err = db.ExecContext(ctx, "UPDATE data SET status = 'processed' WHERE status = 'pending'")
if err != nil {
    log.Fatal(err)
}

// Экспортируем изменённые данные в новую директорию
// Можно дополнительно указать формат вывода и сжатие
options := filesql.NewDumpOptions().
    WithFormat(filesql.OutputFormatTSV).
    WithCompression(filesql.CompressionGZ)
err = filesql.DumpDatabase(db, "/path/to/output/directory", options)
if err != nil {
    log.Fatal(err)
}
```

## 🤝 Вклад

Вклад приветствуется! Пожалуйста, ознакомьтесь с [Руководством по внесению вклада](CONTRIBUTING.md) для получения более подробной информации.

## 💖 Поддержка

Если вы считаете этот проект полезным, пожалуйста, рассмотрите:

- ⭐ Поставить звезду на GitHub — это помогает другим найти проект
- 💝 [Стать спонсором](https://github.com/sponsors/nao1215) — ваша поддержка поддерживает проект и мотивирует продолжать разработку

Ваша поддержка, будь то звёзды, спонсорство или вклад, является движущей силой этого проекта. Спасибо!

## 📄 Лицензия

Этот проект лицензирован под лицензией MIT — подробности см. в файле [LICENSE](../../LICENSE).