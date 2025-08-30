# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

[English](../../README.md) | [Русский](../ru/README.md) | [中文](../zh-cn/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Français](../fr/README.md)

![logo](../image/filesql-logo.png)

**filesql** は、SQLite3のSQL構文を使用してCSV、TSV、LTSV、Parquet、Excel (XLSX)ファイルを直接クエリできるGo SQLドライバーです。インポートや変換なしでデータファイルを直接クエリできます！

## 🎯 なぜfilesqlなのか？

このライブラリは、2つの独立したCLIツール - [sqly](https://github.com/nao1215/sqly) と [sqluv](https://github.com/nao1215/sqluv) のメンテナンス経験から生まれました。どちらのツールも共通の機能を持っていました：CSV、TSV、その他のファイル形式に対するSQLクエリの実行です。

両プロジェクトで重複するコードを維持するのではなく、核となる機能を再利用可能なSQLドライバーとして抽出しました。これで、どのGo開発者でも自分のアプリケーションでこの機能を活用できます！

## ✨ 機能

- 🔍 **SQLite3 SQLインターフェース** - SQLite3の強力なSQL方言を使用してファイルをクエリ
- 📁 **複数のファイル形式** - CSV、TSV、LTSV、Parquet、Excel (XLSX)ファイルをサポート
- 🗜️ **圧縮サポート** - .gz、.bz2、.xz、.zst圧縮ファイルを自動処理
- 🌊 **ストリーム処理** - 設定可能なチャンクサイズでストリーミングにより大容量ファイルを効率的に処理
- 📖 **柔軟な入力ソース** - ファイルパス、ディレクトリ、io.Reader、embed.FSをサポート
- 🚀 **ゼロセットアップ** - データベースサーバー不要、すべてインメモリで動作
- 💾 **自動保存** - ファイルへの変更を自動的に永続化
- 🌍 **クロスプラットフォーム** - Linux、macOS、Windowsでシームレスに動作
- ⚡ **SQLite3駆動** - 信頼性の高いSQL処理のための堅牢なSQLite3エンジンを基盤

## 📋 サポートされているファイル形式

| 拡張子 | 形式 | 説明 |
|--------|------|------|
| `.csv` | CSV | カンマ区切り値 |
| `.tsv` | TSV | タブ区切り値 |
| `.ltsv` | LTSV | ラベル付きタブ区切り値 |
| `.parquet` | Parquet | Apache Parquet 列指向形式 |
| `.xlsx` | Excel XLSX | Microsoft Excel ワークブック形式 |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz`, `.parquet.gz`, `.xlsx.gz` | Gzip圧縮 | Gzip圧縮ファイル |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2`, `.parquet.bz2`, `.xlsx.bz2` | Bzip2圧縮 | Bzip2圧縮ファイル |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz`, `.parquet.xz`, `.xlsx.xz` | XZ圧縮 | XZ圧縮ファイル |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst`, `.parquet.zst`, `.xlsx.zst` | Zstandard圧縮 | Zstandard圧縮ファイル |

## 📦 インストール

```bash
go get github.com/nao1215/filesql
```

## 🚀 クイックスタート

### シンプルな使用方法

適切なタイムアウト処理のため、`OpenContext`の使用を推奨します：

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
    // 大容量ファイル操作用のタイムアウト付きコンテキストを作成
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    // CSVファイルをデータベースとして開く
    db, err := filesql.OpenContext(ctx, "data.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // データをクエリ（テーブル名 = 拡張子なしのファイル名）
    rows, err := db.QueryContext(ctx, "SELECT * FROM data WHERE age > 25")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    // 結果を処理
    for rows.Next() {
        var name string
        var age int
        if err := rows.Scan(&name, &age); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("名前: %s, 年齢: %d\n", name, age)
    }
}
```

### 複数ファイルと形式

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// 複数ファイルを一度に開く（Parquetも含む）
db, err := filesql.OpenContext(ctx, "users.csv", "orders.tsv", "logs.ltsv.gz", "analytics.parquet")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 異なるファイル形式間でデータを結合
rows, err := db.QueryContext(ctx, `
    SELECT u.name, o.order_date, l.event, a.metrics
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN logs l ON u.id = l.user_id
    JOIN analytics a ON u.id = a.user_id
    WHERE o.order_date > '2024-01-01'
`)
```

### ディレクトリの操作

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// ディレクトリからサポートされているすべてのファイルを読み込み（再帰的）
db, err := filesql.OpenContext(ctx, "/path/to/data/directory")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 利用可能なテーブルを確認
rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
```

## 🔧 高度な使用方法

### ビルダーパターン

高度なシナリオではビルダーパターンを使用します：

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
    
    // ビルダーでデータソースを設定
    validatedBuilder, err := filesql.NewBuilder().
        AddPath("local_file.csv").      // ローカルファイル
        AddFS(embeddedFiles).           // 埋め込みファイル
        SetDefaultChunkSize(50*1024*1024). // 50MBチャンク
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }
    
    db, err := validatedBuilder.Open(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // すべてのデータソースに対してクエリ
    rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
}
```

### 自動保存機能

#### データベースクローズ時の自動保存

```go
// データベースクローズ時に変更を自動保存
validatedBuilder, err := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSave("./backup"). // バックアップディレクトリに保存
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close() // ここで変更が自動的に保存される

// 変更を実行
db.Exec("UPDATE data SET status = 'processed' WHERE id = 1")
db.Exec("INSERT INTO data (name, age) VALUES ('田中', 30)")
```

#### トランザクションコミット時の自動保存

```go
// トランザクション後に自動保存
validatedBuilder, err := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSaveOnCommit(""). // 空 = 元ファイルを上書き
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// コミット後に変更が保存される
tx, _ := db.Begin()
tx.Exec("UPDATE data SET status = 'processed' WHERE id = 1")
tx.Commit() // ここで自動保存が実行される
```

### io.Readerとネットワークデータの操作

```go
import (
    "net/http"
    "github.com/nao1215/filesql"
)

// HTTP レスポンスからデータを読み込み
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

// リモートデータをクエリ
rows, err := db.QueryContext(ctx, "SELECT * FROM remote_data LIMIT 10")
```

### 手動データエクスポート

手動で保存を制御したい場合：

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "data.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 変更を実行
db.Exec("UPDATE data SET status = 'processed'")

// 変更を手動でエクスポート
err = filesql.DumpDatabase(db, "./output")
if err != nil {
    log.Fatal(err)
}

// カスタム形式と圧縮を使用
options := filesql.NewDumpOptions().
    WithFormat(filesql.OutputFormatTSV).
    WithCompression(filesql.CompressionGZ)
err = filesql.DumpDatabase(db, "./output", options)

// Parquet形式でエクスポート
parquetOptions := filesql.NewDumpOptions().
    WithFormat(filesql.OutputFormatParquet)
// 注意: Parquetエクスポートは実装済みですが、外部圧縮は非対応です（Parquetの内蔵圧縮を使用してください）
```

## 📝 テーブル命名規則

filesqlはファイルパスから自動的にテーブル名を導出します：

- `users.csv` → テーブル `users`
- `data.tsv.gz` → テーブル `data`
- `/path/to/sales.csv` → テーブル `sales`
- `products.ltsv.bz2` → テーブル `products`
- `analytics.parquet` → テーブル `analytics`

## ⚠️ 重要な注意事項

### SQL構文
filesqlはSQLite3を基盤エンジンとして使用するため、すべてのSQL構文は[SQLite3のSQL方言](https://www.sqlite.org/lang.html)に従います。これには以下が含まれます：
- 関数（例：`date()`、`substr()`、`json_extract()`）
- ウィンドウ関数
- 共通テーブル式（CTE）
- トリガーとビュー

### データ変更
- `INSERT`、`UPDATE`、`DELETE`操作はインメモリデータベースに影響します
- **元ファイルはデフォルトで変更されません**
- 変更を永続化するには自動保存機能または`DumpDatabase()`を使用してください
- これによりデータ変換を安全に実験できます

### パフォーマンスのヒント
- 大容量ファイルには`OpenContext()`とタイムアウトを使用
- メモリ最適化のため`SetDefaultChunkSize()`でチャンクサイズを設定
- ほとんどのシナリオでは単一のSQLite接続が最適
- 利用可能メモリより大きなファイルにはストリーミングを使用

### Parquetサポート
- **読み取り**: 複雑なデータ型を含むApache Parquetファイルを完全サポート
- **書き込み**: エクスポートをサポート（外部圧縮は非対応。Parquetの内蔵圧縮を使用）
- **型マッピング**: ParquetタイプはSQLiteタイプにマッピングされます（[PARQUET_TYPE_MAPPING.md](PARQUET_TYPE_MAPPING.md)を参照）
- **圧縮**: 外部圧縮の代わりにParquetの内蔵圧縮を使用
- **大容量データ**: Parquetファイルは、Arrowの列指向フォーマットで効率的に処理されます

### Excel (XLSX)サポート
- **1シート1テーブル構造**: ExcelワークブックのシートはそれぞれSQLテーブルになります
- **テーブル命名**: SQLテーブル名は`{ファイル名}_{シート名}`の形式に従います（例：「sales_Q1」、「sales_Q2」）
- **ヘッダー行処理**: 各シートの最初の行がそのテーブルの列ヘッダーになります
- **標準SQL操作**: 各シートを独立してクエリするか、JOINを使用してシート間でデータを結合できます
- **メモリ要件**: XLSXファイルはZIPベースの形式構造のため、ストリーミング操作中でもメモリに完全読み込みが必要です
- **実装メモ**: XLSX はZIP構造のため全体をメモリ展開し、全シートを処理します（CSV/TSV向けのストリーミングパーサーは適用されません）
- **エクスポート機能**: XLSX形式にエクスポートする際は、テーブル名が自動的にシート名になります
- **圧縮サポート**: 圧縮XLSXファイル（.xlsx.gz、.xlsx.bz2、.xlsx.xz、.xlsx.zst）を完全サポート

#### Excelファイル構造の例
```
複数シートを持つExcelファイル:

┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Sheet1      │    │ Sheet2      │    │ Sheet3      │
│ Name   Age  │    │ Product     │    │ Region      │
│ Alice   25  │    │ Laptop      │    │ North       │
│ Bob     30  │    │ Mouse       │    │ South       │
└─────────────┘    └─────────────┘    └─────────────┘

3つの独立したSQLテーブルに変換:

sales_Sheet1:           sales_Sheet2:           sales_Sheet3:
┌──────┬─────┐          ┌─────────┐             ┌────────┐
│ Name │ Age │          │ Product │             │ Region │
├──────┼─────┤          ├─────────┤             ├────────┤
│ Alice│  25 │          │ Laptop  │             │ North  │
│ Bob  │  30 │          │ Mouse   │             │ South  │
└──────┴─────┘          └─────────┘             └────────┘

SQL例:
SELECT * FROM sales_Sheet1 WHERE Age > 27;
SELECT s1.Name, s2.Product FROM sales_Sheet1 s1 
  JOIN sales_Sheet2 s2 ON s1.rowid = s2.rowid;
```

## 🎨 高度な例

### 複雑なSQLクエリ

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "employees.csv", "departments.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// SQLiteの高度な機能を使用
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

### コンテキストとキャンセレーション

```go
import (
    "context"
    "time"
)

// 大容量ファイル操作にタイムアウトを設定
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
defer cancel()

db, err := filesql.OpenContext(ctx, "huge_dataset.csv.gz")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// キャンセレーションサポート付きでクエリ
rows, err := db.QueryContext(ctx, "SELECT * FROM huge_dataset WHERE status = 'active'")
```

## 🤝 貢献

貢献を歓迎します！詳細は[貢献ガイド](../../CONTRIBUTING.md)をご覧ください。

## 💖 サポート

このプロジェクトが有用だと感じましたら、以下をご検討ください：

- ⭐ GitHubでスターを付ける - プロジェクトの発見に役立ちます
- 💝 [スポンサーになる](https://github.com/sponsors/nao1215) - あなたのサポートがプロジェクトを維持し、継続的な開発の動機となります

スター、スポンサーシップ、貢献を通じたあなたのサポートが、このプロジェクトを前進させる力となります。ありがとうございます！

## 📄 ライセンス

このプロジェクトはMITライセンスの下でライセンスされています - 詳細は[LICENSE](../../LICENSE)ファイルをご覧ください。