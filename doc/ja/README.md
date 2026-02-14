# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

[English](../../README.md) | [Русский](../ru/README.md) | [中文](../zh-cn/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Français](../fr/README.md)

![logo](../image/filesql-logo.png)

**filesql** は、SQLite3のSQL構文を使用してCSV、TSV、LTSV、Parquet、Excel (XLSX)ファイルを直接クエリできるGo SQLドライバーです。インポートや変換なしでデータファイルを直接クエリできます！

**filesqlの機能を試してみたいですか？** **[sqly](https://github.com/nao1215/sqly)** をチェックしてください - filesqlを使用してシェルから直接CSV、TSV、LTSV、ExcelファイルにSQLクエリを簡単に実行できるコマンドラインツールです。filesqlの力を実際に体験する最適な方法です！

## なぜfilesqlなのか？

このライブラリは、2つの独立したCLIツール - [sqly](https://github.com/nao1215/sqly) と [sqluv](https://github.com/nao1215/sqluv) のメンテナンス経験から生まれました。どちらのツールも共通の機能を持っていました：CSV、TSV、その他のファイル形式に対するSQLクエリの実行です。

両プロジェクトで重複するコードを維持するのではなく、核となる機能を再利用可能なSQLドライバーとして抽出しました。これで、どのGo開発者でも自分のアプリケーションでこの機能を活用できます！

## 機能

- SQLite3 SQLインターフェース - SQLite3の強力なSQL方言を使用してファイルをクエリ
- 複数のファイル形式 - CSV、TSV、LTSV、Parquet、Excel (XLSX)ファイルをサポート
- 圧縮サポート - .gz、.bz2、.xz、.zst、.z、.snappy、.s2、.lz4圧縮ファイルを自動処理
- ストリーム処理 - 設定可能なチャンクサイズでストリーミングにより大容量ファイルを効率的に処理
- 柔軟な入力ソース - ファイルパス、ディレクトリ、io.Reader、embed.FSをサポート
- ゼロセットアップ - データベースサーバー不要、すべてインメモリで動作
- 自動保存 - ファイルへの変更を自動的に永続化
- クロスプラットフォーム - Linux、macOS、Windowsでシームレスに動作
- SQLite3駆動 - 信頼性の高いSQL処理のための堅牢なSQLite3エンジンを基盤

## サポートされているファイル形式

| 拡張子 | 形式 | 説明 |
|--------|------|------|
| `.csv` | CSV | カンマ区切り値 |
| `.tsv` | TSV | タブ区切り値 |
| `.ltsv` | LTSV | ラベル付きタブ区切り値 |
| `.parquet` | Parquet | Apache Parquet 列指向形式 |
| `.xlsx` | Excel XLSX | Microsoft Excel ワークブック形式 |
| `.json` | JSON | JSON形式（フィールドアクセスには `json_extract()` を使用） |
| `.jsonl` | JSONL | JSON Lines形式（1行に1つのJSONオブジェクト） |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz`, `.parquet.gz`, `.xlsx.gz`, `.json.gz`, `.jsonl.gz` | Gzip圧縮 | Gzip圧縮ファイル |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2`, `.parquet.bz2`, `.xlsx.bz2`, `.json.bz2`, `.jsonl.bz2` | Bzip2圧縮 | Bzip2圧縮ファイル |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz`, `.parquet.xz`, `.xlsx.xz`, `.json.xz`, `.jsonl.xz` | XZ圧縮 | XZ圧縮ファイル |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst`, `.parquet.zst`, `.xlsx.zst`, `.json.zst`, `.jsonl.zst` | Zstandard圧縮 | Zstandard圧縮ファイル |
| `.csv.z`, `.tsv.z`, `.ltsv.z`, `.parquet.z`, `.xlsx.z`, `.json.z`, `.jsonl.z` | Zlib圧縮 | Zlib圧縮ファイル |
| `.csv.snappy`, `.tsv.snappy`, `.ltsv.snappy`, `.parquet.snappy`, `.xlsx.snappy`, `.json.snappy`, `.jsonl.snappy` | Snappy圧縮 | Snappy圧縮ファイル |
| `.csv.s2`, `.tsv.s2`, `.ltsv.s2`, `.parquet.s2`, `.xlsx.s2`, `.json.s2`, `.jsonl.s2` | S2圧縮 | S2圧縮ファイル（Snappy互換） |
| `.csv.lz4`, `.tsv.lz4`, `.ltsv.lz4`, `.parquet.lz4`, `.xlsx.lz4`, `.json.lz4`, `.jsonl.lz4` | LZ4圧縮 | LZ4圧縮ファイル |
| `.fed` | Fedwire | レガシーFedwireメッセージファイル（**実験的**） |

## インストール

```bash
go get github.com/nao1215/filesql
```

## 要件

- **Goバージョン**: 1.24以降
- **対応OS**:
  - Linux
  - macOS  
  - Windows

## クイックスタート

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

### JSON / JSONL サポート

JSON・JSONLファイルは `data` TEXT カラムに生JSONとして格納されます。SQLite の `json_extract()` 関数でフィールドにアクセスできます:

```go
// JSONファイルを開く
db, err := filesql.OpenContext(ctx, "users.json")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// json_extract() を使ったクエリ
rows, err := db.QueryContext(ctx, `
    SELECT json_extract(data, '$.name') AS name,
           json_extract(data, '$.age') AS age
    FROM users
    WHERE json_extract(data, '$.age') > 25
`)

// ネストされたフィールドも対応
rows, err = db.QueryContext(ctx, `
    SELECT json_extract(data, '$.address.city') AS city
    FROM users
    WHERE json_extract(data, '$.address.country') = 'Japan'
`)
```

## 高度な使用方法

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
        SetDefaultChunkSize(5000). // 5000行チャンク
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

### カスタムロガー

filesqlは`Logger`インターフェースを介してプラガブルなロギングをサポートしています。デフォルトではパフォーマンスオーバーヘッドがゼロのno-opロガーが使用されます。デバッグやモニタリングのために独自のロガー（例：`slog`）を注入できます。

```go
import (
    "log/slog"
    "os"
    "github.com/nao1215/filesql"
)

// slogロガーを作成
slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug,
}))

// SlogAdapterでラップしてビルダーに渡す
logger := filesql.NewSlogAdapter(slogLogger)

validatedBuilder, err := filesql.NewBuilder().
    WithLogger(logger).
    AddPath("data.csv").
    Build(ctx)
```

#### Loggerインターフェース

```go
type Logger interface {
    Debug(msg string, args ...any)
    Info(msg string, args ...any)
    Warn(msg string, args ...any)
    Error(msg string, args ...any)
    With(args ...any) Logger
}
```

#### コンテキスト対応ロガー

コンテキスト対応のロギングには`ContextLogger`を使用します：

```go
type ContextLogger interface {
    Logger
    DebugContext(ctx context.Context, msg string, args ...any)
    InfoContext(ctx context.Context, msg string, args ...any)
    WarnContext(ctx context.Context, msg string, args ...any)
    ErrorContext(ctx context.Context, msg string, args ...any)
}

// コンテキスト対応ロギングにはSlogContextAdapterを使用
logger := filesql.NewSlogContextAdapter(slogLogger)
```

#### パフォーマンス

| ロガータイプ | パフォーマンス | メモリ |
|-------------|---------------|--------|
| nopLogger（デフォルト） | 約0.2 ns/op | 0 B/op |
| SlogAdapter | 約1000 ns/op | 約630 B/op |

デフォルトのno-opロガーは実質ゼロのオーバーヘッドを持つため、本番コードにロギング呼び出しを残しておいても安全です。

## テーブル命名規則

filesqlはファイルパスから自動的にテーブル名を導出します：

- `users.csv` → テーブル `users`
- `data.tsv.gz` → テーブル `data`
- `/path/to/sales.csv` → テーブル `sales`
- `products.ltsv.bz2` → テーブル `products`
- `analytics.parquet` → テーブル `analytics`

## 重要な注意事項

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
- メモリ最適化のため`SetDefaultChunkSize()`でチャンクサイズ（行数）を設定
- ほとんどのシナリオでは単一のSQLite接続が最適
- 利用可能メモリより大きなファイルにはストリーミングを使用

## ベンチマーク

**10万行のCSVファイル**でのパフォーマンス:

| 指標 | 値 |
|------|-----|
| 実行時間 | 約430 ms |
| メモリ使用量 | 約141 MB |

ベンチマークの実行方法:
```bash
make benchmark
```

### 並行処理の制限事項
⚠️ **重要**: このライブラリは**スレッドセーフではなく**、**並行処理に制限があります**：
- **ゴルーチン間でデータベース接続を共有しないでください**
- **同一データベースインスタンスで並行操作を行わないでください**
- **他のゴルーチンでクエリがアクティブな状態で`db.Close()`を呼び出さないでください**
- 並行操作が必要な場合は、個別のデータベースインスタンスを使用してください
- 競合状態により、セグメンテーション違反やデータ破損が発生する可能性があります

**並行アクセスの推奨パターン**：
```go
// ✅ 良い例: ゴルーチンごとに個別のデータベースインスタンス
func processFileConcurrently(filename string) error {
    db, err := filesql.Open(filename)  // 各ゴルーチンが専用インスタンスを取得
    if err != nil {
        return err
    }
    defer db.Close()
    
    // このゴルーチン内では安全に使用可能
    return processData(db)
}

// ❌ 悪い例: ゴルーチン間でのデータベースインスタンス共有
var sharedDB *sql.DB  // これは競合状態を引き起こします
```

### Parquetサポート
- **読み取り**: 複雑なデータ型を含むApache Parquetファイルを完全サポート
- **書き込み**: エクスポートをサポート（外部圧縮は非対応。Parquetの内蔵圧縮を使用）
- **型マッピング**: ParquetタイプはSQLiteタイプにマッピングされます
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
- **圧縮サポート**: 圧縮XLSXファイル（.xlsx.gz、.xlsx.bz2、.xlsx.xz、.xlsx.zst、.xlsx.z、.xlsx.snappy、.xlsx.s2、.xlsx.lz4）を完全サポート

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

### Fedwireサポート - 実験的

> **警告**: Fedwireファイルサポートは**実験的**です。APIは将来のバージョンで変更される可能性があります。

レガシーFedwireメッセージファイル（`.fed`）を読み込み、クエリ、変更、およびFedwire形式でエクスポートできます。各Fedwireファイルは単一のFEDWireMessageを含み、約326列のフラットテーブルに変換されます。

| テーブル名 | 説明 |
|-----------|------|
| `{ファイル名}_message` | FEDWireMessageの全フィールドを持つフラットテーブル（約326列、1行） |

Wire形式はすべての値を固定長文字列として格納するため、すべてのカラムはTEXT型です。

#### 制限事項

**UPDATEのみ**: ラウンドトリップ編集では、既存行へのUPDATE操作のみサポートされます。SQLでのINSERT/DELETE操作は出力Wireファイルに反映されません。

**新規セクション不可**: 元のファイルに存在しなかったオプションのメッセージセクションは、SQL変更で追加できません。

**圧縮**: Fedwireファイルは圧縮ラッパー（`.fed.gz`等）をサポートしません。

**セキュリティ**: Fedwireデータにはルーティング番号、口座番号、名前、取引金額を含む機密銀行情報が含まれています。本番環境ではWireテーブルデータをそのままログ出力やエクスポートしないでください。

#### 例

```go
ctx := context.Background()
db, err := filesql.OpenContext(ctx, "payment.fed")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 送信者と受信者の情報をクエリ
rows, err := db.QueryContext(ctx, `
    SELECT sender_di_routing_number, receiver_di_routing_number, amount
    FROM payment_message
`)

// 変更してFedwire形式にエクスポート
db.ExecContext(ctx, "UPDATE payment_message SET amount = '000005000000'")
filesql.DumpFedWire(ctx, db, "payment", "modified.fed")
```

## 高度な例

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

## サンプル

[examples](../../examples)ディレクトリには、filesqlの様々な機能を示すサンプルコードが含まれています：

| サンプル | 説明 |
|---------|------|
| [basic](../../examples/basic) | 基本的なCSVクエリ操作 |
| [multi-format](../../examples/multi-format) | 複数ファイル形式（CSV、TSV、LTSV、Parquet）の操作 |
| [sqlc](../../examples/sqlc) | [sqlc](https://sqlc.dev/)との連携 - 型安全なSQLコードジェネレーター |
| [gorm](../../examples/gorm) | [GORM](https://gorm.io/)との連携 - フル機能ORM |
| [sqlx](../../examples/sqlx) | [sqlx](https://github.com/jmoiron/sqlx)との連携 - database/sqlの拡張 |
| [bun](../../examples/bun) | [Bun](https://bun.uptrace.dev/)との連携 - SQL-first ORM |
| [squirrel](../../examples/squirrel) | [Squirrel](https://github.com/Masterminds/squirrel)との連携 - SQLクエリビルダー |
| [ent](../../examples/ent) | [Ent](https://entgo.io/)との連携 - Facebook製エンティティフレームワーク |

## fileprepによるデータ前処理

filesqlでクエリを実行する前のデータ検証と前処理には、**[nao1215/fileprep](https://github.com/nao1215/fileprep)**の使用をお勧めします。

fileprepは以下の機能を提供するコンパニオンライブラリです：
- **構造体タグによる前処理**（`prep`タグ）: トリム、小文字化、大文字化、デフォルト値など
- **構造体タグによるバリデーション**（`validate`タグ）: 必須フィールド、フォーマット検証、クロスフィールド検証
- **filesqlとのシームレスな統合**: filesqlのBuilderパターンで直接使用できる`io.Reader`を返却

```go
// 前処理とバリデーション用のタグを持つ構造体を定義
type User struct {
    // Name: 空白をトリム、空でないことを要求
    Name  string `prep:"trim" validate:"required"`
    // Email: トリム、小文字に変換、メール形式を検証
    Email string `prep:"trim,lowercase" validate:"required,email"`
    // Age: 空の場合デフォルト値を設定、0-150の範囲を検証
    Age   string `prep:"default=0" validate:"numeric,gte=0,lte=150"`
    // Role: トリム、大文字化、許可された値のいずれかであること
    Role  string `prep:"trim,uppercase" validate:"oneof=ADMIN USER GUEST"`
}

func main() {
    // 乱雑な入力を含むCSVデータ
    csvData := `name,email,age,role
  John Doe  ,JOHN@EXAMPLE.COM,25,admin
Alice,alice@example.com,,user`

    // プロセッサを作成してCSVを処理
    processor := fileprep.NewProcessor(fileprep.FileTypeCSV)
    var users []User

    reader, result, err := processor.Process(strings.NewReader(csvData), &users)
    if err != nil {
        log.Fatal(err)
    }

    // バリデーション結果を確認
    fmt.Printf("処理完了: %d 行, 有効: %d 行\n", result.RowCount, result.ValidRowCount)
    if result.HasErrors() {
        for _, e := range result.ValidationErrors() {
            log.Printf("行 %d, 列 %s: %s", e.Row, e.Column, e.Message)
        }
    }

    // 前処理済みデータをfilesqlに渡す
    // データはクリーン済み: トリム、メールの小文字化、デフォルト値適用
    ctx := context.Background()
    db, err := filesql.NewBuilder().
        AddReader(reader, "users", filesql.FileTypeCSV).
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // クリーンなデータに対してクエリを実行
    rows, _ := db.QueryContext(ctx, "SELECT * FROM users WHERE role = 'ADMIN'")
    // ...
}
```

前処理とバリデーションオプションの完全なリストは、[fileprepドキュメント](https://github.com/nao1215/fileprep)を参照してください。

## 関連プロジェクト

filesqlをあなたのプロジェクトで使用していますか？ぜひお知らせください！[Issueを作成](https://github.com/nao1215/filesql/issues)していただければ、以下のリストに追加します。

### 関連ライブラリ

| プロジェクト | 説明 |
|-------------|------|
| [nao1215/fileprep](https://github.com/nao1215/fileprep) | 構造体タグによるバリデーション機能を持つデータ前処理ライブラリ |

### filesqlを使用したCLIツール

| プロジェクト | 説明 |
|-------------|------|
| [nao1215/sqly](https://github.com/nao1215/sqly) | CSV、TSV、LTSV、JSON、ExcelファイルにSQLクエリを実行するインタラクティブシェル |
| [kanmu/gocon2025-ctf](https://github.com/kanmu/gocon2025-ctf) | Go Conference 2025 CTFリポジトリ（日本語） |

## 貢献

貢献を歓迎します！詳細は[貢献ガイド](../../CONTRIBUTING.md)をご覧ください。

## サポート

このプロジェクトが有用だと感じましたら、以下をご検討ください：

- GitHubでスターを付ける - プロジェクトの発見に役立ちます
- [スポンサーになる](https://github.com/sponsors/nao1215) - あなたのサポートがプロジェクトを維持し、継続的な開発の動機となります

スター、スポンサーシップ、貢献を通じたあなたのサポートが、このプロジェクトを前進させる力となります。ありがとうございます！

### スター履歴

[![Star History Chart](https://api.star-history.com/svg?repos=nao1215/filesql&type=date&legend=top-left)](https://www.star-history.com/#nao1215/filesql&Date)

## ライセンス

このプロジェクトはMITライセンスの下でライセンスされています - 詳細は[LICENSE](../../LICENSE)ファイルをご覧ください。
