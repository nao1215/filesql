# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

[English](../../README.md) | [中文](../zh-cn/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Français](../fr/README.md) | [日本語](../ja/README.md)

![logo](../image/filesql-logo.png)

**filesql** — это SQL-драйвер для Go, который позволяет запрашивать файлы CSV, TSV, LTSV, Parquet и Excel (XLSX), используя синтаксис SQL SQLite3. Запрашивайте ваши файлы данных напрямую без импорта или трансформации!

**Хотите попробовать возможности filesql?** Оцените **[sqly](https://github.com/nao1215/sqly)** — инструмент командной строки, который использует filesql для лёгкого выполнения SQL-запросов к файлам CSV, TSV, LTSV и Excel прямо из вашего shell. Это идеальный способ испытать мощь filesql в действии!

## Зачем filesql?

Эта библиотека родилась из опыта поддержки двух отдельных CLI-инструментов - [sqly](https://github.com/nao1215/sqly) и [sqluv](https://github.com/nao1215/sqluv). Оба инструмента имели общую особенность: выполнение SQL-запросов к файлам CSV, TSV и другим форматам файлов.

Вместо поддержки дублирующегося кода в обоих проектах, мы извлекли основную функциональность в этот повторно используемый SQL-драйвер. Теперь любой Go-разработчик может использовать эту возможность в своих собственных приложениях!

## Особенности

- Интерфейс SQL SQLite3 - Используйте мощный SQL-диалект SQLite3 для запроса ваших файлов
- Множественные форматы файлов - Поддержка файлов CSV, TSV, LTSV, Parquet и Excel (XLSX)
- Поддержка сжатия - Автоматически обрабатывает сжатые файлы .gz, .bz2, .xz и .zst
- Потоковая обработка - Эффективно обрабатывает большие файлы через потоковую передачу с настраиваемыми размерами блоков
- Гибкие источники ввода - Поддержка путей к файлам, каталогов, io.Reader и embed.FS
- Нулевая настройка - Сервер баз данных не требуется, всё работает в памяти
- Автосохранение - Автоматически сохраняет изменения в файлы
- Кроссплатформенность - Безупречно работает на Linux, macOS и Windows
- На основе SQLite3 - Построен на надёжном движке SQLite3 для надёжной обработки SQL

## Поддерживаемые форматы файлов

| Расширение | Формат | Описание |
|-----------|--------|-------------|
| `.csv` | CSV | Значения, разделённые запятыми |
| `.tsv` | TSV | Значения, разделённые табуляцией |
| `.ltsv` | LTSV | Помеченные значения, разделённые табуляцией |
| `.parquet` | Parquet | Колонночный формат Apache Parquet |
| `.xlsx` | Excel XLSX | Формат рабочей книги Microsoft Excel |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz`, `.parquet.gz`, `.xlsx.gz` | Сжатие Gzip | Файлы, сжатые Gzip |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2`, `.parquet.bz2`, `.xlsx.bz2` | Сжатие Bzip2 | Файлы, сжатые Bzip2 |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz`, `.parquet.xz`, `.xlsx.xz` | Сжатие XZ | Файлы, сжатые XZ |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst`, `.parquet.zst`, `.xlsx.zst` | Сжатие Zstandard | Файлы, сжатые Zstandard |

## Установка

```bash
go get github.com/nao1215/filesql
```

## Требования

- **Версия Go**: 1.24 или новее
- **Поддерживаемые ОС**:
  - Linux
  - macOS  
  - Windows

## Быстрый старт

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

// Открыть несколько файлов одновременно (включая Parquet)
db, err := filesql.OpenContext(ctx, "users.csv", "orders.tsv", "logs.ltsv.gz", "analytics.parquet")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Объединить данные из разных форматов файлов
rows, err := db.QueryContext(ctx, `
    SELECT u.name, o.order_date, l.event, a.metrics
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN logs l ON u.id = l.user_id
    JOIN analytics a ON u.id = a.user_id
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

## Расширенное использование

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
        SetDefaultChunkSize(5000). // 5000 строк на блок
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

// Экспорт в формат Parquet (когда доступен)
parquetOptions := filesql.NewDumpOptions().
    WithFormat(filesql.OutputFormatParquet)
// Примечание: Экспорт Parquet реализован (внешнее сжатие не поддерживается, используйте встроенное сжатие Parquet)
```

## Правила именования таблиц

filesql автоматически выводит имена таблиц из путей к файлам:

- `users.csv` → таблица `users`
- `data.tsv.gz` → таблица `data`
- `/path/to/sales.csv` → таблица `sales`
- `products.ltsv.bz2` → таблица `products`
- `analytics.parquet` → таблица `analytics`

## Важные заметки

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
- Настройте размеры блоков (количество строк) с помощью `SetDefaultChunkSize()` для оптимизации памяти
- Одно соединение SQLite работает лучше всего для большинства сценариев
- Используйте потоковую передачу для файлов больше доступной памяти

## Бенчмарк

Производительность с **CSV-файлом на 100 000 строк**:

| Метрика | Значение |
|---------|----------|
| Время выполнения | ~430 мс |
| Использование памяти | ~141 МБ |

Запустите бенчмарки самостоятельно:
```bash
make benchmark
```

### Ограничения многопоточности
⚠️ **ВАЖНО**: Эта библиотека **НЕ потокобезопасна** и имеет **ограничения многопоточности**:
- **НЕ** делитесь соединениями базы данных между горутинами
- **НЕ** выполняйте одновременные операции на одном экземпляре базы данных
- **НЕ** вызывайте `db.Close()` пока запросы активны в других горутинах
- При необходимости многопоточных операций используйте отдельные экземпляры базы данных
- Состояния гонки могут вызвать ошибки сегментации или повреждение данных

**Рекомендуемый паттерн для многопоточного доступа**:
```go
// ✅ ХОРОШО: Отдельные экземпляры базы данных для каждой горутины
func processFileConcurrently(filename string) error {
    db, err := filesql.Open(filename)  // Каждая горутина получает свой экземпляр
    if err != nil {
        return err
    }
    defer db.Close()
    
    // Безопасно использовать в пределах этой горутины
    return processData(db)
}

// ❌ ПЛОХО: Совместное использование экземпляра базы данных между горутинами
var sharedDB *sql.DB  // Это вызовет состояния гонки
```

### Поддержка Parquet
- **Чтение**: Полная поддержка файлов Apache Parquet со сложными типами данных
- **Запись**: Функциональность экспорта реализована (внешнее сжатие не поддерживается, используйте встроенное сжатие Parquet)
- **Отображение типов**: Типы Parquet отображаются в типы SQLite
- **Сжатие**: Используется встроенное сжатие Parquet вместо внешнего сжатия
- **Большие данные**: Файлы Parquet эффективно обрабатываются с помощью колонночного формата Arrow

### Поддержка Excel (XLSX)
- **Структура 1-Лист-1-Таблица**: Каждый лист в рабочей книге Excel становится отдельной SQL-таблицей
- **Именование таблиц**: Имена SQL-таблиц следуют формату `{имя_файла}_{имя_листа}` (например, "продажи_Q1", "продажи_Q2")
- **Обработка строки заголовков**: Первая строка каждого листа становится заголовками столбцов для этой таблицы
- **Стандартные SQL-операции**: Запрашивайте каждый лист независимо или используйте JOIN для объединения данных между листами
- **Требования к памяти**: XLSX-файлы требуют полной загрузки в память из-за ZIP-структуры формата, даже при потоковых операциях
- **Полная загрузка в память**: XLSX-файлы полностью загружаются в память из-за их ZIP-структуры, и обрабатываются все листы (не только первый). Потоковые парсеры CSV/TSV не применимы к XLSX-файлам
- **Функциональность экспорта**: При экспорте в формат XLSX имена таблиц автоматически становятся именами листов
- **Поддержка сжатия**: Полная поддержка сжатых XLSX-файлов (.xlsx.gz, .xlsx.bz2, .xlsx.xz, .xlsx.zst)

#### Пример структуры Excel-файла
```
Excel-файл с несколькими листами:

┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Лист1       │    │ Лист2       │    │ Лист3       │
│ Имя   Возр. │    │ Товар       │    │ Регион      │
│ Алиса   25  │    │ Ноутбук     │    │ Север       │
│ Борис   30  │    │ Мышь        │    │ Юг          │
└─────────────┘    └─────────────┘    └─────────────┘

Результат: 3 отдельные SQL-таблицы:

продажи_Лист1:          продажи_Лист2:          продажи_Лист3:
┌───────┬──────┐         ┌──────────┐             ┌────────┐
│ Имя   │ Возр.│         │ Товар    │             │ Регион │
├───────┼──────┤         ├──────────┤             ├────────┤
│ Алиса │  25  │         │ Ноутбук  │             │ Север  │
│ Борис │  30  │         │ Мышь     │             │ Юг     │
└───────┴──────┘         └──────────┘             └────────┘

Примеры SQL:
SELECT * FROM продажи_Лист1 WHERE Возраст > 27;
SELECT l1.Имя, l2.Товар FROM продажи_Лист1 l1 
  JOIN продажи_Лист2 l2 ON l1.rowid = l2.rowid;
```

## Продвинутые примеры

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

## Примеры

Директория [examples](../../examples) содержит примеры кода, демонстрирующие различные возможности filesql:

| Пример | Описание |
|--------|----------|
| [basic](../../examples/basic) | Базовые операции запросов CSV |
| [multi-format](../../examples/multi-format) | Работа с несколькими форматами файлов (CSV, TSV, LTSV, Parquet) |
| [sqlc](../../examples/sqlc) | Интеграция с [sqlc](https://sqlc.dev/) - типобезопасный генератор SQL кода |
| [gorm](../../examples/gorm) | Интеграция с [GORM](https://gorm.io/) - полнофункциональный ORM |
| [sqlx](../../examples/sqlx) | Интеграция с [sqlx](https://github.com/jmoiron/sqlx) - расширения database/sql |
| [bun](../../examples/bun) | Интеграция с [Bun](https://bun.uptrace.dev/) - SQL-first ORM |
| [squirrel](../../examples/squirrel) | Интеграция с [Squirrel](https://github.com/Masterminds/squirrel) - построитель SQL запросов |
| [ent](../../examples/ent) | Интеграция с [Ent](https://entgo.io/) - entity framework от Facebook |

## Предобработка данных с fileprep

Для валидации и предобработки данных перед запросами с filesql, мы рекомендуем использовать **[nao1215/fileprep](https://github.com/nao1215/fileprep)**.

fileprep — это библиотека-компаньон, которая предоставляет:
- **Предобработку на основе struct-тегов** (тег `prep`): обрезка, нижний регистр, верхний регистр, значения по умолчанию и многое другое
- **Валидацию на основе struct-тегов** (тег `validate`): обязательные поля, валидация формата, кросс-валидация полей
- **Бесшовную интеграцию с filesql**: Возвращает `io.Reader` для прямого использования с паттерном Builder filesql

```go
// Определение struct с тегами предобработки и валидации
type User struct {
    // Name: удалить пробелы, требуется непустое значение
    Name  string `prep:"trim" validate:"required"`
    // Email: удалить пробелы, преобразовать в нижний регистр, проверить формат email
    Email string `prep:"trim,lowercase" validate:"required,email"`
    // Age: установить значение по умолчанию если пусто, проверить диапазон 0-150
    Age   string `prep:"default=0" validate:"numeric,gte=0,lte=150"`
    // Role: удалить пробелы, верхний регистр, должно быть одним из разрешённых значений
    Role  string `prep:"trim,uppercase" validate:"oneof=ADMIN USER GUEST"`
}

func main() {
    // CSV данные с неаккуратным вводом
    csvData := `name,email,age,role
  John Doe  ,JOHN@EXAMPLE.COM,25,admin
Alice,alice@example.com,,user`

    // Создать процессор и обработать CSV
    processor := fileprep.NewProcessor(fileprep.FileTypeCSV)
    var users []User

    reader, result, err := processor.Process(strings.NewReader(csvData), &users)
    if err != nil {
        log.Fatal(err)
    }

    // Проверить результаты валидации
    fmt.Printf("Обработано: %d строк, Валидных: %d строк\n", result.RowCount, result.ValidRowCount)
    if result.HasErrors() {
        for _, e := range result.ValidationErrors() {
            log.Printf("Строка %d, Столбец %s: %s", e.Row, e.Column, e.Message)
        }
    }

    // Передать предобработанные данные в filesql
    // Данные теперь очищены: пробелы удалены, email в нижнем регистре, значения по умолчанию применены
    ctx := context.Background()
    db, err := filesql.NewBuilder().
        AddReader(reader, "users", filesql.FileTypeCSV).
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Запрос к очищенным данным
    rows, _ := db.QueryContext(ctx, "SELECT * FROM users WHERE role = 'ADMIN'")
    // ...
}
```

Полный список опций предобработки и валидации см. в [документации fileprep](https://github.com/nao1215/fileprep).

## Связанные проекты

Используете filesql в своём проекте? Мы хотим об этом узнать! Пожалуйста, [откройте issue](https://github.com/nao1215/filesql/issues), чтобы сообщить нам, и мы добавим ваш проект в список ниже.

### Связанные библиотеки

| Проект | Описание |
|--------|----------|
| [nao1215/fileprep](https://github.com/nao1215/fileprep) | Библиотека предобработки данных с валидацией через struct теги |

### CLI инструменты использующие filesql

| Проект | Описание |
|--------|----------|
| [nao1215/sqly](https://github.com/nao1215/sqly) | Интерактивная оболочка для выполнения SQL запросов к CSV, TSV, LTSV, JSON и Excel файлам |
| [kanmu/gocon2025-ctf](https://github.com/kanmu/gocon2025-ctf) | Репозиторий CTF Go Conference 2025 (на японском) |

## Вклад

Вклады приветствуются! Пожалуйста, ознакомьтесь с [Руководством по участию](../../CONTRIBUTING.md) для получения более подробной информации.

## Поддержка

Если вы находите этот проект полезным, пожалуйста, рассмотрите возможность:

- Поставить звёзду на GitHub - это помогает другим найти проект
- [Стать спонсором](https://github.com/sponsors/nao1215) - ваша поддержка поддерживает проект живым и мотивирует непрерывную разработку

Ваша поддержка, будь то через звёзды, спонсорство или вклады, — это то, что движет этот проект вперёд. Спасибо!

### История звёзд

[![Star History Chart](https://api.star-history.com/svg?repos=nao1215/filesql&type=date&legend=top-left)](https://www.star-history.com/#nao1215/filesql&Date)

## Лицензия

Этот проект лицензирован под лицензией MIT - см. файл [LICENSE](../../LICENSE) для подробностей.
