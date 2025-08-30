# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

[English](../../README.md) | [中文](../zh-cn/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Français](../fr/README.md) | [日本語](../ja/README.md)

**filesql** — это SQL-драйвер для Go, который позволяет запрашивать файлы CSV, TSV и LTSV, используя синтаксис SQL SQLite3. Запрашивайте ваши файлы данных напрямую без импорта или трансформации!

## 🎯 Зачем filesql?

Эта библиотека родилась из опыта поддержки двух отдельных CLI-инструментов - [sqly](https://github.com/nao1215/sqly) и [sqluv](https://github.com/nao1215/sqluv). Оба инструмента имели общую особенность: выполнение SQL-запросов к файлам CSV, TSV и другим форматам файлов.

Вместо поддержки дублирующегося кода в обоих проектах, мы извлекли основную функциональность в этот повторно используемый SQL-драйвер. Теперь любой Go-разработчик может использовать эту возможность в своих собственных приложениях!

## ✨ Особенности

- 🔍 **Интерфейс SQL SQLite3** - Используйте мощный SQL-диалект SQLite3 для запроса ваших файлов
- 📁 **Множественные форматы файлов** - Поддержка файлов CSV, TSV и LTSV
- 🗜️ **Поддержка сжатия** - Автоматически обрабатывает сжатые файлы .gz, .bz2, .xz и .zst
- 🌊 **Потоковая обработка** - Эффективно обрабатывает большие файлы через потоковую передачу с настраиваемыми размерами блоков
- 📖 **Гибкие источники ввода** - Поддержка путей к файлам, каталогов, io.Reader и embed.FS
- 🚀 **Нулевая настройка** - Сервер баз данных не требуется, всё работает в памяти
- 💾 **Автосохранение** - Автоматически сохраняет изменения в файлы
- 🌍 **Кроссплатформенность** - Безупречно работает на Linux, macOS и Windows
- ⚡ **На основе SQLite3** - Построен на надёжном движке SQLite3 для надёжной обработки SQL

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

### Простое использование

Рекомендуемый способ начать работу — с `OpenContext` для правильной обработки таймаутов:

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
    // Создать контекст с таймаутом для операций с большими файлами
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    // Открыть CSV-файл как базу данных
    db, err := filesql.OpenContext(ctx, "data.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Запросить данные (имя таблицы = имя файла без расширения)
    rows, err := db.QueryContext(ctx, "SELECT * FROM data WHERE age > 25")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    // Обработать результаты
    for rows.Next() {
        var name string
        var age int
        if err := rows.Scan(&name, &age); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("Имя: %s, Возраст: %d\n", name, age)
    }
}
```

### Множественные файлы и форматы

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Открыть несколько файлов одновременно
db, err := filesql.OpenContext(ctx, "users.csv", "orders.tsv", "logs.ltsv.gz")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Объединить данные из разных форматов файлов
rows, err := db.QueryContext(ctx, `
    SELECT u.name, o.order_date, l.event
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN logs l ON u.id = l.user_id
    WHERE o.order_date > '2024-01-01'
`)
```

### Работа с каталогами

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Загрузить все поддерживаемые файлы из каталога (рекурсивно)
db, err := filesql.OpenContext(ctx, "/path/to/data/directory")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Посмотреть, какие таблицы доступны
rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
```

## 🔧 Расширенное использование

### Паттерн Builder

Для продвинутых сценариев используйте паттерн builder:

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
    
    // Настроить источники данных с помощью builder
    validatedBuilder, err := filesql.NewBuilder().
        AddPath("local_file.csv").      // Локальный файл
        AddFS(embeddedFiles).           // Встроенные файлы
        SetDefaultChunkSize(50*1024*1024). // Блоки по 50МБ
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }
    
    db, err := validatedBuilder.Open(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Запросить все источники данных
    rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
}
```

### Функции автосохранения

#### Автосохранение при закрытии базы данных

```go
// Автоматически сохранять изменения при закрытии базы данных
validatedBuilder, err := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSave("./backup"). // Сохранить в каталог резервных копий
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close() // Изменения автоматически сохраняются здесь

// Внести изменения
db.Exec("UPDATE data SET status = 'processed' WHERE id = 1")
db.Exec("INSERT INTO data (name, age) VALUES ('Иван', 30)")
```

#### Автосохранение при коммите транзакции

```go
// Автоматически сохранять после каждой транзакции
validatedBuilder, err := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSaveOnCommit(""). // Пустая строка = перезаписать исходные файлы
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Изменения сохраняются после каждого коммита
tx, _ := db.Begin()
tx.Exec("UPDATE data SET status = 'processed' WHERE id = 1")
tx.Commit() // Автосохранение происходит здесь
```

### Работа с io.Reader и сетевыми данными

```go
import (
    "net/http"
    "github.com/nao1215/filesql"
)

// Загрузить данные из HTTP-ответа
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

// Запросить удалённые данные
rows, err := db.QueryContext(ctx, "SELECT * FROM remote_data LIMIT 10")
```

### Ручной экспорт данных

Если вы предпочитаете ручное управление сохранением:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "data.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Внести изменения
db.Exec("UPDATE data SET status = 'processed'")

// Вручную экспортировать изменения
err = filesql.DumpDatabase(db, "./output")
if err != nil {
    log.Fatal(err)
}

// Или с пользовательским форматом и сжатием
options := filesql.NewDumpOptions().
    WithFormat(filesql.OutputFormatTSV).
    WithCompression(filesql.CompressionGZ)
err = filesql.DumpDatabase(db, "./output", options)
```

## 📝 Правила именования таблиц

filesql автоматически выводит имена таблиц из путей к файлам:

- `users.csv` → таблица `users`
- `data.tsv.gz` → таблица `data`
- `/path/to/sales.csv` → таблица `sales`
- `products.ltsv.bz2` → таблица `products`

## ⚠️ Важные заметки

### SQL-синтаксис
Поскольку filesql использует SQLite3 в качестве базового движка, весь SQL-синтаксис следует [SQL-диалекту SQLite3](https://www.sqlite.org/lang.html). Это включает:
- Функции (например, `date()`, `substr()`, `json_extract()`)
- Оконные функции
- Общие табличные выражения (CTE)
- Триггеры и представления

### Изменения данных
- Операции `INSERT`, `UPDATE` и `DELETE` влияют на базу данных в памяти
- **Исходные файлы остаются неизменными по умолчанию**
- Используйте функции автосохранения или `DumpDatabase()` для сохранения изменений
- Это делает безопасным экспериментирование с трансформациями данных

### Советы по производительности
- Используйте `OpenContext()` с таймаутами для больших файлов
- Настройте размеры блоков с помощью `SetDefaultChunkSize()` для оптимизации памяти
- Одно соединение SQLite работает лучше всего для большинства сценариев
- Используйте потоковую передачу для файлов больше доступной памяти

## 🎨 Продвинутые примеры

### Сложные SQL-запросы

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "employees.csv", "departments.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Использовать продвинутые возможности SQLite
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

### Контекст и отмена

```go
import (
    "context"
    "time"
)

// Установить таймаут для операций с большими файлами
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
defer cancel()

db, err := filesql.OpenContext(ctx, "huge_dataset.csv.gz")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Запрос с контекстом для поддержки отмены
rows, err := db.QueryContext(ctx, "SELECT * FROM huge_dataset WHERE status = 'active'")
```

## 🤝 Вклад

Вклады приветствуются! Пожалуйста, ознакомьтесь с [Руководством по участию](../../CONTRIBUTING.md) для получения более подробной информации.

## 💖 Поддержка

Если вы находите этот проект полезным, пожалуйста, рассмотрите возможность:

- ⭐ Поставить звёзду на GitHub - это помогает другим найти проект
- 💝 [Стать спонсором](https://github.com/sponsors/nao1215) - ваша поддержка поддерживает проект живым и мотивирует непрерывную разработку

Ваша поддержка, будь то через звёзды, спонсорство или вклады, — это то, что движет этот проект вперёд. Спасибо!

## 📄 Лицензия

Этот проект лицензирован под лицензией MIT - см. файл [LICENSE](../../LICENSE) для подробностей.