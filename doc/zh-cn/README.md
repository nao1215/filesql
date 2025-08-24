# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

**filesql** 是一个 Go 语言 SQL 驱动程序，可以让您使用 SQLite3 SQL 语法查询 CSV、TSV 和 LTSV 文件。无需导入或转换，直接查询您的数据文件！

## 🎯 为什么选择 filesql？

这个库源于维护两个独立 CLI 工具 - [sqly](https://github.com/nao1215/sqly) 和 [sqluv](https://github.com/nao1215/sqluv) 的经验。两个工具都有一个共同功能：对 CSV、TSV 和其他文件格式执行 SQL 查询。

与其在两个项目中维护重复代码，我们将核心功能提取为这个可复用的 SQL 驱动程序。现在，任何 Go 开发者都可以在自己的应用程序中使用此功能！

## ✨ 特性

- 🔍 **SQLite3 SQL 接口** - 使用 SQLite3 强大的 SQL 方言查询您的文件
- 📁 **多种文件格式** - 支持 CSV、TSV 和 LTSV 文件
- 🗜️ **压缩支持** - 自动处理 .gz、.bz2、.xz 和 .zst 压缩文件
- 🚀 **零配置** - 无需数据库服务器，全部在内存中运行
- 🌍 **跨平台** - 在 Linux、macOS 和 Windows 上无缝运行
- 💾 **SQLite3 驱动** - 基于强大的 SQLite3 引擎构建，提供可靠的 SQL 处理

## 📋 支持的文件格式

| 扩展名 | 格式 | 描述 |
|-----------|--------|-------------|
| `.csv` | CSV | 逗号分隔值 |
| `.tsv` | TSV | 制表符分隔值 |
| `.ltsv` | LTSV | 标签制表符分隔值 |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz` | Gzip 压缩 | Gzip 压缩文件 |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2` | Bzip2 压缩 | Bzip2 压缩文件 |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz` | XZ 压缩 | XZ 压缩文件 |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst` | Zstandard 压缩 | Zstandard 压缩文件 |


## 📦 安装

```bash
go get github.com/nao1215/filesql
```

## 🚀 快速开始

[示例代码在这里](../../example_test.go)。

### 基本用法

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
    // 将 CSV 文件作为数据库打开
    db, err := filesql.Open("data.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // 执行 SQL 查询（表名从不带扩展名的文件名派生）
    rows, err := db.QueryContext(context.Background(), "SELECT * FROM data WHERE age > 25 ORDER BY name")
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
        fmt.Printf("Name: %s, Age: %d\n", name, age)
    }
}
```

### 支持上下文的打开方式

```go
// 使用超时控制打开文件
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "large_dataset.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 使用上下文查询以支持取消操作
rows, err := db.QueryContext(ctx, "SELECT * FROM large_dataset WHERE status = 'active'")
```

### 打开多个文件

```go
// 在单个数据库中打开多个文件
db, err := filesql.Open("users.csv", "orders.tsv", "products.ltsv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 跨不同文件格式连接数据！
rows, err := db.QueryContext(context.Background(), `
    SELECT u.name, o.order_date, p.product_name
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN products p ON o.product_id = p.id
    WHERE o.order_date > '2024-01-01'
`)
```

### 处理目录

```go
// 打开目录中的所有支持文件（递归）
db, err := filesql.Open("/path/to/data/directory")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 查询所有加载的表
rows, err := db.QueryContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table'")
```

### 压缩文件支持

```go
// 自动处理压缩文件
db, err := filesql.Open("large_dataset.csv.gz", "archive.tsv.bz2")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 无缝查询压缩数据
rows, err := db.QueryContext(context.Background(), "SELECT COUNT(*) FROM large_dataset")
```

### 表命名规则

filesql 自动从文件路径派生表名：

```go
// 表命名示例：
// "users.csv"           -> 表名："users"
// "data.tsv"            -> 表名："data"
// "logs.ltsv"           -> 表名："logs"
// "archive.csv.gz"      -> 表名："archive"
// "backup.tsv.bz2"      -> 表名："backup"
// "/path/to/sales.csv"  -> 表名："sales"

db, err := filesql.Open("employees.csv", "departments.tsv.gz")
if err != nil {
    log.Fatal(err)
}

// 在查询中使用派生的表名
rows, err := db.QueryContext(context.Background(), `
    SELECT * FROM employees 
    JOIN departments ON employees.dept_id = departments.id
`)
```

## ⚠️ 重要说明

### SQL 语法
由于 filesql 使用 SQLite3 作为底层引擎，所有 SQL 语法都遵循 [SQLite3 的 SQL 方言](https://www.sqlite.org/lang.html)。这包括：
- 函数（例如 `date()`、`substr()`、`json_extract()`）
- 窗口函数
- 公共表表达式（CTE）
- 还有更多！

### 数据修改
- `INSERT`、`UPDATE` 和 `DELETE` 操作只影响内存数据库
- **原文件保持不变** - filesql 永远不会修改您的源文件
- 这使得安全地实验数据转换成为可能

### 高级 SQL 功能

由于 filesql 使用 SQLite3，您可以发挥其全部威力：

```go
db, err := filesql.Open("employees.csv", "departments.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 使用窗口函数、CTE 和复杂查询
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

### 导出修改的数据

如果您需要持久化对内存数据库所做的更改：

```go
db, err := filesql.Open("data.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 进行修改
_, err = db.ExecContext(context.Background(), "UPDATE data SET status = 'processed' WHERE status = 'pending'")
if err != nil {
    log.Fatal(err)
}

// 将修改的数据导出到新目录
err = filesql.DumpDatabase(db, "/path/to/output/directory")
if err != nil {
    log.Fatal(err)
}
```

## 💖 支持

如果您觉得这个项目有用，请考虑：

- ⭐ 在 GitHub 上给它一个星标 - 这有助于其他人发现这个项目
- 💝 [成为赞助者](https://github.com/sponsors/nao1215) - 您的支持让项目保持活力并激励持续开发

无论是星标、赞助还是贡献，您的支持都是推动这个项目前进的动力。谢谢！

## 📄 许可证

本项目在 MIT 许可证下授权 - 详情请参见 [LICENSE](../../LICENSE) 文件。