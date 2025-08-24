# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

**filesql**は、SQLite3のSQL構文を使用してCSV、TSV、LTSVファイルにクエリを実行できるGo言語のSQLドライバーです。インポートや変換なしで、データファイルに直接クエリを実行できます！

## 🎯 なぜfilesqlなのか？

このライブラリは、2つの別々のCLIツール - [sqly](https://github.com/nao1215/sqly)と[sqluv](https://github.com/nao1215/sqluv)のメンテナンス経験から生まれました。両方のツールには、CSV、TSV、その他のファイル形式に対してSQLクエリを実行するという共通の機能がありました。

両プロジェクトで重複するコードをメンテナンスする代わりに、コア機能を再利用可能なSQLドライバーとして抽出しました。これで、Go開発者なら誰でも自分のアプリケーションでこの機能を活用できます！

## ✨ 機能

- 🔍 **SQLite3 SQLインターフェース** - SQLite3の強力なSQL方言を使用してファイルにクエリを実行します
- 📁 **複数のファイル形式** - CSV、TSV、LTSVファイルをサポートします
- 🗜️ **圧縮サポート** - .gz、.bz2、.xz、.zst圧縮ファイルを自動的に処理します
- 🚀 **ゼロセットアップ** - データベースサーバーは不要で、すべてインメモリで実行されます
- 🌍 **クロスプラットフォーム** - Linux、macOS、Windowsでシームレスに動作します
- 💾 **SQLite3搭載** - 信頼性の高いSQL処理のために堅牢なSQLite3エンジン上に構築されています

## 📋 サポートされているファイル形式

| 拡張子 | フォーマット | 説明 |
|-----------|--------|-------------|
| `.csv` | CSV | カンマ区切り値 |
| `.tsv` | TSV | タブ区切り値 |
| `.ltsv` | LTSV | ラベル付きタブ区切り値 |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz` | Gzip圧縮 | Gzip圧縮ファイル |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2` | Bzip2圧縮 | Bzip2圧縮ファイル |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz` | XZ圧縮 | XZ圧縮ファイル |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst` | Zstandard圧縮 | Zstandard圧縮ファイル |


## 📦 インストール

```bash
go get github.com/nao1215/filesql
```

## 🚀 クイックスタート

[サンプルコードはこちら](../../example_test.go)です。

### 基本的な使い方

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
    // CSVファイルをデータベースとして開きます
    db, err := filesql.Open("data.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // SQLクエリを実行します（テーブル名は拡張子なしのファイル名から派生します）
    rows, err := db.QueryContext(context.Background(), "SELECT * FROM data WHERE age > 25 ORDER BY name")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    // 結果を処理します
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

### Contextサポート付きで開く

```go
// タイムアウト制御付きでファイルを開きます
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "large_dataset.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// キャンセルサポート付きでコンテキストを使用してクエリを実行
rows, err := db.QueryContext(ctx, "SELECT * FROM large_dataset WHERE status = 'active'")
```

### 複数ファイルを開く

```go
// 複数のファイルを単一のデータベースで開きます
db, err := filesql.Open("users.csv", "orders.tsv", "products.ltsv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 異なるファイル形式間でデータを結合します！
rows, err := db.QueryContext(context.Background(), `
    SELECT u.name, o.order_date, p.product_name
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN products p ON o.product_id = p.id
    WHERE o.order_date > '2024-01-01'
`)
```

### ディレクトリの操作

```go
// ディレクトリ内のすべてのサポートされたファイルを開きます（再帰的）
db, err := filesql.Open("/path/to/data/directory")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 読み込まれたすべてのテーブルをクエリします
rows, err := db.QueryContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table'")
```

### 圧縮ファイルのサポート

```go
// 圧縮ファイルを自動的に処理します
db, err := filesql.Open("large_dataset.csv.gz", "archive.tsv.bz2")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 圧縮データをシームレスにクエリします
rows, err := db.QueryContext(context.Background(), "SELECT COUNT(*) FROM large_dataset")
```

### テーブル命名規則

filesqlはファイルパスから自動的にテーブル名を導出します：

```go
// テーブル命名の例：
// "users.csv"           -> テーブル名: "users"
// "data.tsv"            -> テーブル名: "data"
// "logs.ltsv"           -> テーブル名: "logs"
// "archive.csv.gz"      -> テーブル名: "archive"
// "backup.tsv.bz2"      -> テーブル名: "backup"
// "/path/to/sales.csv"  -> テーブル名: "sales"

db, err := filesql.Open("employees.csv", "departments.tsv.gz")
if err != nil {
    log.Fatal(err)
}

// クエリで導出されたテーブル名を使用します
rows, err := db.QueryContext(context.Background(), `
    SELECT * FROM employees 
    JOIN departments ON employees.dept_id = departments.id
`)
```

## ⚠️ 重要な注意事項

### SQL構文
filesqlは基盤となるエンジンとしてSQLite3を使用しているため、すべてのSQL構文は[SQLite3のSQL方言](https://www.sqlite.org/lang.html)に従います。これには以下が含まれます：
- 関数（例：`date()`、`substr()`、`json_extract()`）
- ウィンドウ関数
- 共通テーブル式（CTE）
- その他多数！

### データの変更
- `INSERT`、`UPDATE`、`DELETE`操作はインメモリデータベースにのみ影響します
- **元のファイルは変更されません** - filesqlはソースファイルを決して変更しません
- これにより、データ変換を安全に実験できます

### 高度なSQL機能

filesqlはSQLite3を使用しているため、その全機能を活用できます：

```go
db, err := filesql.Open("employees.csv", "departments.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// ウィンドウ関数、CTE、複雑なクエリを使用します
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

### 変更されたデータのエクスポート

インメモリデータベースに加えた変更を永続化する必要がある場合：

```go
db, err := filesql.Open("data.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 変更を加えます
_, err = db.ExecContext(context.Background(), "UPDATE data SET status = 'processed' WHERE status = 'pending'")
if err != nil {
    log.Fatal(err)
}

// 変更されたデータを新しいディレクトリにエクスポートします
err = filesql.DumpDatabase(db, "/path/to/output/directory")
if err != nil {
    log.Fatal(err)
}
```

## 💖 サポート

このプロジェクトが役に立つと思われた場合は、以下をご検討ください：

- ⭐ GitHubでスターを付ける - 他の人がプロジェクトを発見するのに役立ちます
- 💝 [スポンサーになる](https://github.com/sponsors/nao1215) - あなたのサポートがプロジェクトを維持し、継続的な開発のモチベーションになります

スター、スポンサーシップ、貢献など、あなたのサポートがこのプロジェクトを前進させる原動力です。ありがとうございます！

## 📄 ライセンス

このプロジェクトはMITライセンスの下でライセンスされています - 詳細は[LICENSE](../../LICENSE)ファイルをご覧ください。