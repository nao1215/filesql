# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

[English](../../README.md) | [Русский](../ru/README.md) | [日本語](../ja/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Français](../fr/README.md)

![logo](../image/filesql-logo.png)

**filesql** 是一个 Go SQL 驱动，让您可以使用 SQLite3 SQL 语法直接查询 CSV、TSV、LTSV、Parquet 和 Excel (XLSX) 文件。无需导入或转换即可直接查询数据文件！

**想要体验 filesql 的功能？** 试试 **[sqly](https://github.com/nao1215/sqly)** - 一个使用 filesql 直接从 shell 轻松对 CSV、TSV、LTSV 和 Excel 文件执行 SQL 查询的命令行工具！这是体验 filesql 强大功能的完美方式！

## 🎯 为什么选择 filesql？

这个库诞生于维护两个独立 CLI 工具 - [sqly](https://github.com/nao1215/sqly) 和 [sqluv](https://github.com/nao1215/sqluv) 的经验。两个工具都有一个共同特性：对 CSV、TSV 和其他文件格式执行 SQL 查询。

我们将核心功能提取为可重用的 SQL 驱动，而不是在两个项目中维护重复代码。现在，任何 Go 开发者都可以在自己的应用中使用这项功能！

## ✨ 功能特性

- 🔍 **SQLite3 SQL 接口** - 使用 SQLite3 强大的 SQL 方言查询文件
- 📁 **多种文件格式** - 支持 CSV、TSV、LTSV、Parquet 和 Excel (XLSX) 文件
- 🗜️ **压缩支持** - 自动处理 .gz、.bz2、.xz 和 .zst 压缩文件
- 🌊 **流式处理** - 通过可配置的块大小高效处理大文件
- 📖 **灵活的输入源** - 支持文件路径、目录、io.Reader 和 embed.FS
- 🚀 **零配置** - 无需数据库服务器，全部在内存中运行
- 💾 **自动保存** - 自动将更改持久化到文件
- 🌍 **跨平台** - 在 Linux、macOS 和 Windows 上无缝运行
- ⚡ **SQLite3 驱动** - 基于强大的 SQLite3 引擎，确保可靠的 SQL 处理

## 📋 支持的文件格式

| 扩展名 | 格式 | 描述 |
|--------|------|------|
| `.csv` | CSV | 逗号分隔值 |
| `.tsv` | TSV | 制表符分隔值 |
| `.ltsv` | LTSV | 标签制表符分隔值 |
| `.parquet` | Parquet | Apache Parquet 列式格式 |
| `.xlsx` | Excel XLSX | Microsoft Excel 工作簿格式 |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz`, `.parquet.gz`, `.xlsx.gz` | Gzip 压缩 | Gzip 压缩文件 |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2`, `.parquet.bz2`, `.xlsx.bz2` | Bzip2 压缩 | Bzip2 压缩文件 |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz`, `.parquet.xz`, `.xlsx.xz` | XZ 压缩 | XZ 压缩文件 |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst`, `.parquet.zst`, `.xlsx.zst` | Zstandard 压缩 | Zstandard 压缩文件 |

## 📦 安装

```bash
go get github.com/nao1215/filesql
```

## 🔧 系统要求

- **Go 版本**: 1.24 或更高版本
- **支持操作系统**:
  - Linux
  - macOS  
  - Windows

## 🚀 快速开始

### 简单用法

推荐使用 `OpenContext` 来正确处理超时：

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
    // 为大文件操作创建带超时的上下文
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    // 将 CSV 文件作为数据库打开
    db, err := filesql.OpenContext(ctx, "data.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // 查询数据（表名 = 去掉扩展名的文件名）
    rows, err := db.QueryContext(ctx, "SELECT * FROM data WHERE age > 25")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    // 处理结果
    for rows.Next() {
        var name string
        var age int
        if err := rows.Scan(&name, &age); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("姓名: %s, 年龄: %d\n", name, age)
    }
}
```

### 多文件和格式

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// 一次打开多个文件（包括 Parquet 和 XLSX）
db, err := filesql.OpenContext(ctx, "users.csv", "orders.tsv", "logs.ltsv.gz", "analytics.parquet", "sales.xlsx")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 跨不同文件格式连接数据
rows, err := db.QueryContext(ctx, `
    SELECT u.name, o.order_date, l.event, a.metrics, s.total_amount
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN logs l ON u.id = l.user_id
    JOIN analytics a ON u.id = a.user_id
    JOIN sales_Sheet1 s ON u.id = s.user_id
    WHERE o.order_date > '2024-01-01'
`)
```

### 处理目录

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// 从目录加载所有支持的文件（递归）
db, err := filesql.OpenContext(ctx, "/path/to/data/directory")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 查看可用的表
rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
```

## 🔧 高级用法

### 构建器模式

对于高级场景，使用构建器模式：

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
    
    // 使用构建器配置数据源
    validatedBuilder, err := filesql.NewBuilder().
        AddPath("local_file.csv").      // 本地文件
        AddFS(embeddedFiles).           // 嵌入文件
        SetDefaultChunkSize(5000). // 5000行块
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }
    
    db, err := validatedBuilder.Open(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // 查询所有数据源
    rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
}
```

### 自动保存功能

#### 数据库关闭时自动保存

```go
// 数据库关闭时自动保存更改
validatedBuilder, err := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSave("./backup"). // 保存到备份目录
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close() // 在此处自动保存更改

// 进行更改
db.Exec("UPDATE data SET status = 'processed' WHERE id = 1")
db.Exec("INSERT INTO data (name, age) VALUES ('张三', 30)")
```

#### 事务提交时自动保存

```go
// 每次事务后自动保存
validatedBuilder, err := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSaveOnCommit(""). // 空字符串 = 覆盖原始文件
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 每次提交后保存更改
tx, _ := db.Begin()
tx.Exec("UPDATE data SET status = 'processed' WHERE id = 1")
tx.Commit() // 在此处执行自动保存
```

### 处理 io.Reader 和网络数据

```go
import (
    "net/http"
    "github.com/nao1215/filesql"
)

// 从 HTTP 响应加载数据
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

// 查询远程数据
rows, err := db.QueryContext(ctx, "SELECT * FROM remote_data LIMIT 10")
```

### 手动数据导出

如果您希望手动控制保存：

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "data.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 进行修改
db.Exec("UPDATE data SET status = 'processed'")

// 手动导出更改
err = filesql.DumpDatabase(db, "./output")
if err != nil {
    log.Fatal(err)
}

// 或使用自定义格式和压缩
options := filesql.NewDumpOptions().
    WithFormat(filesql.OutputFormatTSV).
    WithCompression(filesql.CompressionGZ)
err = filesql.DumpDatabase(db, "./output", options)

// 导出到 Parquet 格式（计划中）
parquetOptions := filesql.NewDumpOptions().
    WithFormat(filesql.OutputFormatParquet)
// 注意：Parquet 导出功能已实现（不支持外部压缩，请使用 Parquet 的内置压缩）
```

## 📝 表命名规则

filesql 自动从文件路径推导表名：

- `users.csv` → 表 `users`
- `data.tsv.gz` → 表 `data`
- `/path/to/sales.csv` → 表 `sales`
- `products.ltsv.bz2` → 表 `products`
- `analytics.parquet` → 表 `analytics`
- `sales.xlsx`（包含工作表 "Q1"、"Q2"）→ 表 `sales_Q1`、`sales_Q2`

## ⚠️ 重要说明

### SQL 语法
由于 filesql 使用 SQLite3 作为底层引擎，所有 SQL 语法都遵循 [SQLite3 的 SQL 方言](https://www.sqlite.org/lang.html)。包括：
- 函数（如 `date()`、`substr()`、`json_extract()`）
- 窗口函数
- 公用表表达式 (CTE)
- 触发器和视图

### 数据修改
- `INSERT`、`UPDATE` 和 `DELETE` 操作影响内存数据库
- **默认情况下原始文件保持不变**
- 使用自动保存功能或 `DumpDatabase()` 来持久化更改
- 这使得安全地尝试数据转换成为可能

### 性能提示
- 对大文件使用带超时的 `OpenContext()`
- 使用 `SetDefaultChunkSize()` 配置块大小（行数）以优化内存
- 单个 SQLite 连接对大多数场景效果最佳
- 对于大于可用内存的文件使用流式处理

### 并发限制
⚠️ **重要**: 此库**不是线程安全的**，并且有**并发限制**：
- **不要**在 goroutine 之间共享数据库连接
- **不要**在同一数据库实例上执行并发操作
- **不要**在其他 goroutine 中有活动查询时调用 `db.Close()`
- 如需并发操作，请为每个 goroutine 使用单独的数据库实例
- 竞态条件可能导致段错误或数据损坏

**并发访问的推荐模式**：
```go
// ✅ 好的做法：每个 goroutine 使用单独的数据库实例
func processFileConcurrently(filename string) error {
    db, err := filesql.Open(filename)  // 每个 goroutine 获取自己的实例
    if err != nil {
        return err
    }
    defer db.Close()
    
    // 在此 goroutine 内安全使用
    return processData(db)
}

// ❌ 不好的做法：在 goroutine 间共享数据库实例
var sharedDB *sql.DB  // 这会导致竞态条件
```

## 🎨 高级示例

### 复杂的 SQL 查询

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "employees.csv", "departments.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 使用 SQLite 高级功能
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

### 上下文和取消

```go
import (
    "context"
    "time"
)

// 为大文件操作设置超时
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
defer cancel()

db, err := filesql.OpenContext(ctx, "huge_dataset.csv.gz")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 使用上下文支持取消的查询
rows, err := db.QueryContext(ctx, "SELECT * FROM huge_dataset WHERE status = 'active'")
```

### Parquet 支持
- **读取**：完全支持 Apache Parquet 文件和复杂数据类型
- **写入**：导出功能已实现（不支持外部压缩，请使用 Parquet 的内置压缩）
- **类型映射**：Parquet 类型映射到 SQLite 类型
- **压缩**：使用 Parquet 的内置压缩而不是外部压缩
- **大数据**：使用 Arrow 列式格式高效处理 Parquet 文件

### Excel (XLSX) 支持
- **一张工作表一张表结构**：Excel 工作簿中的每张工作表都会成为单独的 SQL 表
- **表命名规则**：SQL 表名遵循 `{文件名}_{工作表名}` 格式（例如："sales_Q1"、"sales_Q2"）
- **标题行处理**：每张工作表的第一行成为该表的列标题
- **标准 SQL 操作**：可以独立查询每张工作表，或使用 JOIN 合并不同工作表的数据
- **内存要求**：由于基于 ZIP 的格式结构，XLSX 文件即使在流式操作期间也需要完全加载到内存中
- **完全内存加载**：XLSX 文件由于其 ZIP 结构需要完全加载到内存中，并处理所有工作表（不仅仅是第一张工作表）。CSV/TSV 流式解析器不适用于 XLSX 文件
- **导出功能**：导出到 XLSX 格式时，表名会自动成为工作表名
- **压缩支持**：完全支持压缩的 XLSX 文件（.xlsx.gz、.xlsx.bz2、.xlsx.xz、.xlsx.zst）

#### Excel 文件结构示例
```
Excel 文件包含多张工作表：

┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ 员工信息    │    │ 产品列表    │    │ 销售区域    │
│ 姓名   年龄  │    │ 产品名称    │    │ 区域名称    │
│ 小明    25  │    │ 笔记本电脑  │    │ 华北地区    │
│ 小红    30  │    │ 鼠标       │    │ 华南地区    │
└─────────────┘    └─────────────┘    └─────────────┘

生成 3 个独立的 SQL 表：

sales_员工信息:         sales_产品列表:         sales_销售区域:
┌──────┬─────┐          ┌────────────┐        ┌────────────┐
│ 姓名 │ 年龄│          │ 产品名称   │        │ 区域名称   │
├──────┼─────┤          ├────────────┤        ├────────────┤
│ 小明 │  25 │          │ 笔记本电脑 │        │ 华北地区   │
│ 小红 │  30 │          │ 鼠标       │        │ 华南地区   │
└──────┴─────┘          └────────────┘        └────────────┘

SQL 示例：
SELECT * FROM sales_员工信息 WHERE 年龄 > 27;
SELECT e.姓名, p.产品名称 FROM sales_员工信息 e 
  JOIN sales_产品列表 p ON e.rowid = p.rowid;
```

## 🤝 贡献

欢迎贡献！更多详情请参见[贡献指南](../../CONTRIBUTING.md)。

## 💖 支持

如果您觉得这个项目有用，请考虑：

- ⭐ 在 GitHub 上给它一个星标 - 这有助于其他人发现这个项目
- 💝 [成为赞助者](https://github.com/sponsors/nao1215) - 您的支持让项目保持活力并激励持续开发

您的支持，无论是通过星标、赞助还是贡献，都是推动这个项目前进的动力。谢谢！

## 📄 许可证

本项目基于 MIT 许可证授权 - 详情请参见 [LICENSE](../../LICENSE) 文件。