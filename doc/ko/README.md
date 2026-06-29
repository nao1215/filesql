# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

[English](../../README.md) | [Русский](../ru/README.md) | [中文](../zh-cn/README.md) | [Español](../es/README.md) | [Français](../fr/README.md) | [日本語](../ja/README.md)

![logo](../image/filesql-logo.png)

**filesql**은 SQLite3 SQL 구문을 사용하여 CSV, TSV, LTSV, JSON, JSONL, Parquet, Excel (XLSX) 파일을 쿼리할 수 있게 해주는 Go SQL 드라이버입니다. filesql이 파일을 인메모리 SQLite 데이터베이스로 로드해주므로, 수동 가져오기 단계나 스키마 정의, 실행할 데이터베이스 서버 없이 파일에 대해 SQL을 작성할 수 있습니다.

**filesql의 기능을 체험해보고 싶으신가요?** **[sqly](https://github.com/nao1215/sqly)**를 확인해보세요 - filesql을 사용하여 셸에서 직접 CSV, TSV, LTSV, Excel 파일에 대해 SQL 쿼리를 쉽게 실행할 수 있는 명령줄 도구입니다. filesql의 성능을 실제로 경험할 수 있는 완벽한 방법입니다!

## 왜 filesql인가요?

이 라이브러리는 두 개의 별도 CLI 도구인 [sqly](https://github.com/nao1215/sqly)와 [sqluv](https://github.com/nao1215/sqluv)를 유지 관리한 경험에서 탄생했습니다. 두 도구 모두 공통 기능을 공유했습니다: CSV, TSV 및 기타 파일 형식에 대한 SQL 쿼리 실행.

두 프로젝트에서 중복 코드를 유지하는 대신, 핵심 기능을 재사용 가능한 SQL 드라이버로 추출했습니다. 이제 모든 Go 개발자가 자신의 애플리케이션에서 이 기능을 활용할 수 있습니다!

## 기능

- SQLite3 SQL 인터페이스 - SQLite3의 강력한 SQL 방언을 사용하여 파일 쿼리
- 다중 파일 형식 - CSV, TSV, LTSV, Parquet, Excel (XLSX) 파일 지원
- 압축 지원 - .gz, .bz2, .xz, .zst, .z, .snappy, .s2, .lz4 압축 파일 자동 처리
- 스트림 처리 - 설정 가능한 청크 크기로 스트리밍을 통해 대용량 파일 효율적 처리
- 유연한 입력 소스 - 파일 경로, 디렉터리, io.Reader, embed.FS 지원
- 제로 설정 - 데이터베이스 서버 불필요, 모든 것이 메모리에서 실행
- 자동 저장 - 변경사항을 파일에 자동으로 저장
- 크로스 플랫폼 - Linux, macOS, Windows에서 원활하게 작동
- SQLite3 기반 - 안정적인 SQL 처리를 위한 견고한 SQLite3 엔진 기반

## 지원되는 파일 형식

| 확장자 | 형식 | 설명 |
|--------|------|------|
| `.csv` | CSV | 쉼표로 구분된 값 |
| `.tsv` | TSV | 탭으로 구분된 값 |
| `.ltsv` | LTSV | 레이블이 있는 탭으로 구분된 값 |
| `.parquet` | Parquet | Apache Parquet 칼럼형 형식 |
| `.xlsx` | Excel XLSX | Microsoft Excel 워크북 형식 |
| `.json` | JSON | JSON 형식 (필드 접근에 `json_extract()` 사용) |
| `.jsonl` | JSONL | JSON Lines 형식 (한 줄에 하나의 JSON 객체) |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz`, `.parquet.gz`, `.xlsx.gz`, `.json.gz`, `.jsonl.gz` | Gzip 압축 | Gzip 압축 파일 |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2`, `.parquet.bz2`, `.xlsx.bz2`, `.json.bz2`, `.jsonl.bz2` | Bzip2 압축 | Bzip2 압축 파일 |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz`, `.parquet.xz`, `.xlsx.xz`, `.json.xz`, `.jsonl.xz` | XZ 압축 | XZ 압축 파일 |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst`, `.parquet.zst`, `.xlsx.zst`, `.json.zst`, `.jsonl.zst` | Zstandard 압축 | Zstandard 압축 파일 |
| `.csv.z`, `.tsv.z`, `.ltsv.z`, `.parquet.z`, `.xlsx.z`, `.json.z`, `.jsonl.z` | Zlib 압축 | Zlib 압축 파일 |
| `.csv.snappy`, `.tsv.snappy`, `.ltsv.snappy`, `.parquet.snappy`, `.xlsx.snappy`, `.json.snappy`, `.jsonl.snappy` | Snappy 압축 | Snappy 압축 파일 |
| `.csv.s2`, `.tsv.s2`, `.ltsv.s2`, `.parquet.s2`, `.xlsx.s2`, `.json.s2`, `.jsonl.s2` | S2 압축 | S2 압축 파일 (Snappy 호환) |
| `.csv.lz4`, `.tsv.lz4`, `.ltsv.lz4`, `.parquet.lz4`, `.xlsx.lz4`, `.json.lz4`, `.jsonl.lz4` | LZ4 압축 | LZ4 압축 파일 |
| `.fed` | Fedwire | 레거시 Fedwire 메시지 파일 (**실험적**) |

## 설치

```bash
go get github.com/nao1215/filesql
```

## 요구사항

- **Go 버전**: 1.25 이상
- **지원 OS**:
  - Linux
  - macOS  
  - Windows

## 빠른 시작

### 간단한 사용법

권장되는 시작 방법은 적절한 타임아웃 처리를 위해 `OpenContext`를 사용하는 것입니다:

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
    // 대용량 파일 작업을 위한 타임아웃이 있는 컨텍스트 생성
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    // CSV 파일을 데이터베이스로 열기
    db, err := filesql.OpenContext(ctx, "data.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // 데이터 쿼리 (테이블 이름 = 확장자가 없는 파일 이름)
    rows, err := db.QueryContext(ctx, "SELECT * FROM data WHERE age > 25")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    // 결과 처리
    for rows.Next() {
        var name string
        var age int
        if err := rows.Scan(&name, &age); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("이름: %s, 나이: %d\n", name, age)
    }
}
```

### 다중 파일과 형식

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// 여러 파일을 한 번에 열기 (Parquet 포함)
db, err := filesql.OpenContext(ctx, "users.csv", "orders.tsv", "logs.ltsv.gz", "analytics.parquet")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 다양한 파일 형식에서 데이터 조인
rows, err := db.QueryContext(ctx, `
    SELECT u.name, o.order_date, l.event, a.metrics
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN logs l ON u.id = l.user_id
    JOIN analytics a ON u.id = a.user_id
    WHERE o.order_date > '2024-01-01'
`)
```

### 디렉터리 작업

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// 디렉터리에서 지원되는 모든 파일 로드 (재귀적)
db, err := filesql.OpenContext(ctx, "/path/to/data/directory")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 사용 가능한 테이블 확인
rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
```

### JSON / JSONL 지원

JSON 및 JSONL 파일은 `data` TEXT 열에 원시 JSON으로 저장됩니다. SQLite의 `json_extract()` 함수를 사용하여 필드를 쿼리할 수 있습니다:

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

## 고급 사용법

### 빌더 패턴

고급 시나리오에서는 빌더 패턴을 사용하세요:

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
    
    // 빌더로 데이터 소스 구성
    validatedBuilder, err := filesql.NewBuilder().
        AddPath("local_file.csv").      // 로컬 파일
        AddFS(embeddedFiles).           // 임베디드 파일
        SetDefaultChunkSize(5000). // 5000행 청크
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }
    
    db, err := validatedBuilder.Open(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // 모든 데이터 소스 쿼리
    rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
}
```

### 자동 저장 기능

#### 데이터베이스 닫기 시 자동 저장

```go
// 데이터베이스가 닫힐 때 변경사항 자동 저장
validatedBuilder, err := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSave("./backup"). // 백업 디렉터리에 저장
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close() // 여기서 변경사항이 자동으로 저장됨

// 변경 수행
db.Exec("UPDATE data SET status = 'processed' WHERE id = 1")
db.Exec("INSERT INTO data (name, age) VALUES ('김철수', 30)")
```

#### 트랜잭션 커밋 시 자동 저장

```go
// 각 트랜잭션 후 자동 저장
validatedBuilder, err := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSaveOnCommit(""). // 빈 문자열 = 원본 파일 덮어쓰기
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 각 커밋 후 변경사항 저장
tx, _ := db.Begin()
tx.Exec("UPDATE data SET status = 'processed' WHERE id = 1")
tx.Commit() // 여기서 자동 저장 발생
```

### io.Reader와 네트워크 데이터 작업

```go
import (
    "net/http"
    "github.com/nao1215/filesql"
)

// HTTP 응답에서 데이터 로드
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

// 원격 데이터 쿼리
rows, err := db.QueryContext(ctx, "SELECT * FROM remote_data LIMIT 10")
```

### 수동 데이터 내보내기

저장을 수동으로 제어하려면:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "data.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 수정 수행
db.Exec("UPDATE data SET status = 'processed'")

// 변경사항 수동 내보내기
err = filesql.DumpDatabase(db, "./output")
if err != nil {
    log.Fatal(err)
}

// 또는 사용자 정의 형식과 압축으로
options := filesql.NewDumpOptions().
    WithFormat(filesql.OutputFormatTSV).
    WithCompression(filesql.CompressionGZ)
err = filesql.DumpDatabase(db, "./output", options)

// Parquet 형식으로 내보내기 (사용 가능할 때)
parquetOptions := filesql.NewDumpOptions().
    WithFormat(filesql.OutputFormatParquet)
// 참고: Parquet 내보내기 기능이 구현되었습니다 (외부 압축은 지원하지 않으므로 Parquet의 내장 압축을 사용하세요)
```

### 커스텀 로거

filesql은 `Logger` 인터페이스를 통해 플러그형 로깅을 지원합니다. 기본적으로 성능 오버헤드가 없는 no-op 로거가 사용됩니다. 디버깅 및 모니터링을 위해 자체 로거(예: `slog`)를 주입할 수 있습니다.

```go
import (
    "log/slog"
    "os"
    "github.com/nao1215/filesql"
)

// slog 로거 생성
slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug,
}))

// SlogAdapter로 래핑하고 빌더에 전달
logger := filesql.NewSlogAdapter(slogLogger)

validatedBuilder, err := filesql.NewBuilder().
    WithLogger(logger).
    AddPath("data.csv").
    Build(ctx)
```

#### Logger 인터페이스

```go
type Logger interface {
    Debug(msg string, args ...any)
    Info(msg string, args ...any)
    Warn(msg string, args ...any)
    Error(msg string, args ...any)
    With(args ...any) Logger
}
```

#### 컨텍스트 인식 로거

컨텍스트 인식 로깅에는 `ContextLogger`를 사용합니다:

```go
type ContextLogger interface {
    Logger
    DebugContext(ctx context.Context, msg string, args ...any)
    InfoContext(ctx context.Context, msg string, args ...any)
    WarnContext(ctx context.Context, msg string, args ...any)
    ErrorContext(ctx context.Context, msg string, args ...any)
}

// 컨텍스트 인식 로깅에는 SlogContextAdapter 사용
logger := filesql.NewSlogContextAdapter(slogLogger)
```

#### 성능

| 로거 타입 | 성능 | 메모리 |
|----------|------|--------|
| nopLogger (기본값) | ~0.2 ns/op | 0 B/op |
| SlogAdapter | ~1000 ns/op | ~630 B/op |

기본 no-op 로거는 실질적으로 제로 오버헤드를 가지므로, 프로덕션 코드에 로깅 호출을 남겨두어도 안전합니다.

## 테이블 명명 규칙

filesql은 파일 경로에서 자동으로 테이블 이름을 도출합니다:

- `users.csv` → 테이블 `users`
- `data.tsv.gz` → 테이블 `data`
- `/path/to/sales.csv` → 테이블 `sales`
- `products.ltsv.bz2` → 테이블 `products`
- `analytics.parquet` → 테이블 `analytics`
- `sales.xlsx` (시트 "Q1", "Q2" 포함) → 테이블 `sales_Q1`, `sales_Q2`

## 중요한 주의사항

### SQL 구문
filesql은 SQLite3를 기본 엔진으로 사용하므로 모든 SQL 구문은 [SQLite3의 SQL 방언](https://www.sqlite.org/lang.html)을 따릅니다. 여기에는 다음이 포함됩니다:
- 함수 (예: `date()`, `substr()`, `json_extract()`)
- 윈도우 함수
- 공통 테이블 표현식 (CTE)
- 트리거와 뷰

### 데이터 수정
- `INSERT`, `UPDATE`, `DELETE` 작업은 메모리 내 데이터베이스에 영향을 줍니다
- **기본적으로 원본 파일은 변경되지 않습니다**
- 변경사항을 지속하려면 자동 저장 기능이나 `DumpDatabase()`를 사용하세요
- 이는 데이터 변환을 안전하게 실험할 수 있게 합니다

### 성능 팁
- 대용량 파일에는 타임아웃이 있는 `OpenContext()` 사용
- 메모리 최적화를 위해 `SetDefaultChunkSize()`로 청크 크기 (행 수) 설정
- 모든 데이터는 인메모리 SQLite 데이터베이스로 로드되므로, 데이터셋 크기에 대략 비례하는 메모리를 계획하세요

#### 메모리와 스트리밍
filesql은 로딩 중에 CSV, TSV, JSON 배열을 청크 단위로 스트리밍하므로, 파서 자체가 전체 파일을 한 번에 보유하지 않습니다. 다른 형식은 그 구조상 로딩 중에 메모리로 완전히 읽어 들입니다:

- LTSV, 배열이 아닌 JSON/JSONL 값, Parquet (랜덤 액세스가 필요함), Excel (XLSX, ZIP 기반)은 로딩 전에 전체가 읽혀집니다.

어느 쪽이든 파싱된 행은 결국 인메모리 SQLite 데이터베이스에 저장되므로, 전체 메모리 사용량은 청크 크기만이 아니라 데이터셋 크기에 따라 결정됩니다. 사용 가능한 메모리보다 큰 데이터의 경우, 스트리밍에만 의존하기보다는 파일을 미리 분할하거나 일부만 로드하세요.

## 벤치마크

**100,000행 CSV 파일** 성능:

| 지표 | 값 |
|------|-----|
| 실행 시간 | ~430 ms |
| 메모리 사용량 | ~141 MB |

직접 벤치마크 실행:
```bash
make benchmark
```

### 동시성
`Open`/`OpenContext`가 반환하는 `*sql.DB`는 여러 고루틴에서 공유해도 안전합니다. 이는 공유 캐시(shared-cache) 인메모리 SQLite 데이터베이스를 기반으로 하므로, 풀에 있는 각 연결은 동일한 데이터에 대해 자체 연결을 열고 `database/sql`이 이를 대신 관리해 줍니다. 따라서 `SetMaxOpenConns(1)`을 직접 호출할 필요가 없습니다:

```go
// Safe: share one *sql.DB across goroutines.
db, err := filesql.Open("data.csv")
if err != nil {
    return err
}
defer db.Close()

var wg sync.WaitGroup
for range 8 {
    wg.Go(func() {
        rows, err := db.Query("SELECT * FROM data")
        // ... use rows ...
    })
}
wg.Wait()
```

SQLite는 공유 인메모리 데이터베이스에 대한 쓰기를 직렬화하므로, 동시 쓰기가 많으면 서로를 기다리게 됩니다. 반면 읽기는 함께 진행될 수 있습니다. 완전히 독립적인 데이터베이스가 필요하다면 고루틴마다 별도의 `*sql.DB`를 여세요.

> `LoadInto`는 다릅니다. 그 경우에는 사용자가 자신의 `*sql.DB`를 직접 가져오므로 풀 설정에 대한 책임도 사용자에게 있습니다. 일반 인메모리 데이터베이스(`sql.Open("sqlite", ":memory:")`)의 경우, 해당 데이터베이스는 단일 연결에 종속되므로 `db.SetMaxOpenConns(1)`을 직접 호출하세요.

### Parquet 지원
- **읽기**: 복잡한 데이터 타입을 가진 Apache Parquet 파일에 대한 완전 지원
- **쓰기**: 내보내기 기능이 구현되었습니다 (외부 압축은 지원하지 않으므로 Parquet의 내장 압축을 사용하세요)
- **타입 매핑**: Parquet 타입은 SQLite 타입으로 매핑됨
- **압축**: 외부 압축 대신 Parquet의 내장 압축이 사용됨
- **대용량 데이터**: Parquet 파일은 Arrow의 칼럼형 형식으로 효율적으로 처리됨

### Excel (XLSX) 지원
- **1-시트-1-테이블 구조**: Excel 워크북의 각 시트는 별도의 SQL 테이블이 됨
- **테이블 명명**: SQL 테이블 이름은 `{파일명}_{시트명}` 형식을 따름 (예: "sales_Q1", "sales_Q2")
- **헤더 행 처리**: 각 시트의 첫 번째 행이 해당 테이블의 칼럼 헤더가 됨
- **표준 SQL 작업**: 각 시트를 독립적으로 쿼리하거나 JOIN을 사용하여 시트 간 데이터 결합
- **메모리 요구사항**: ZIP 기반 형식 구조로 인해 XLSX 파일은 스트리밍 작업 중에도 전체를 메모리에 로드해야 함
- **메모리 완전 로딩**: XLSX 파일은 ZIP 구조로 인해 메모리에 완전히 로드되며, 모든 시트가 처리됩니다(첫 번째 시트만이 아닙니다). CSV/TSV 스트리밍 파서는 XLSX 파일에 적용되지 않습니다
- **내보내기 기능**: XLSX 형식으로 내보낼 때 테이블 이름이 자동으로 시트 이름이 됨
- **압축 지원**: 압축된 XLSX 파일에 대한 완전 지원 (.xlsx.gz, .xlsx.bz2, .xlsx.xz, .xlsx.zst, .xlsx.z, .xlsx.snappy, .xlsx.s2, .xlsx.lz4)

#### Excel 파일 구조 예제
```
여러 시트가 있는 Excel 파일:

┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Sheet1      │    │ Sheet2      │    │ Sheet3      │
│ 이름   나이  │    │ 상품        │    │ 지역        │
│ 민수   25   │    │ 노트북      │    │ 북부        │
│ 영희   30   │    │ 마우스      │    │ 남부        │
└─────────────┘    └─────────────┘    └─────────────┘

3개의 별도 SQL 테이블 생성:

sales_Sheet1:           sales_Sheet2:           sales_Sheet3:
┌──────┬─────┐          ┌─────────┐             ┌────────┐
│ 이름 │ 나이│          │ 상품    │             │ 지역   │
├──────┼─────┤          ├─────────┤             ├────────┤
│ 민수 │  25 │          │ 노트북  │             │ 북부   │
│ 영희 │  30 │          │ 마우스  │             │ 남부   │
└──────┴─────┘          └─────────┘             └────────┘

SQL 예제:
SELECT * FROM sales_Sheet1 WHERE 나이 > 27;
SELECT s1.이름, s2.상품 FROM sales_Sheet1 s1 
  JOIN sales_Sheet2 s2 ON s1.rowid = s2.rowid;
```

### Fedwire 지원 - 실험적

> **경고**: Fedwire 파일 지원은 **실험적**입니다. API는 향후 버전에서 변경될 수 있습니다.

레거시 Fedwire 메시지 파일(`.fed`)을 로드, 쿼리, 수정하고 Fedwire 형식으로 다시 내보낼 수 있습니다. 각 Fedwire 파일은 단일 FEDWireMessage를 포함하며 약 326개의 열을 가진 단일 플랫 테이블로 변환됩니다.

| 테이블 이름 | 설명 |
|-----------|------|
| `{파일명}_message` | 모든 FEDWireMessage 필드를 가진 플랫 테이블 (~326열, 1행) |

Wire 형식은 모든 값을 고정 너비 문자열로 저장하므로 모든 열은 TEXT 타입입니다.

#### 제한사항

**UPDATE만 지원**: 왕복 편집에는 기존 행에 대한 UPDATE 작업만 지원됩니다. SQL에서의 INSERT/DELETE 작업은 출력 Wire 파일에 반영되지 않습니다.

**새 섹션 불가**: 원본 파일에 없었던 선택적 메시지 섹션은 SQL 수정을 통해 추가할 수 없습니다.

**압축**: Fedwire 파일은 압축 래퍼(`.fed.gz` 등)를 지원하지 않습니다.

**보안**: Fedwire 데이터에는 라우팅 번호, 계좌 번호, 이름, 거래 금액을 포함한 민감한 은행 정보가 포함되어 있습니다. 프로덕션 환경에서 Wire 테이블 데이터를 그대로 로깅하거나 내보내지 마세요.

#### 예제

```go
ctx := context.Background()
db, err := filesql.OpenContext(ctx, "payment.fed")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 발신자와 수신자 정보 쿼리
rows, err := db.QueryContext(ctx, `
    SELECT sender_di_routing_number, receiver_di_routing_number, amount
    FROM payment_message
`)

// 수정하고 Fedwire 형식으로 다시 내보내기
db.ExecContext(ctx, "UPDATE payment_message SET amount = '000005000000'")
filesql.DumpFedWire(ctx, db, "payment", "modified.fed")
```

## 고급 예제

### 복잡한 SQL 쿼리

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "employees.csv", "departments.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// SQLite의 고급 기능 사용
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

### 컨텍스트와 취소

```go
import (
    "context"
    "time"
)

// 대용량 파일 작업을 위한 타임아웃 설정
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
defer cancel()

db, err := filesql.OpenContext(ctx, "huge_dataset.csv.gz")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 취소 지원을 위한 컨텍스트와 함께 쿼리
rows, err := db.QueryContext(ctx, "SELECT * FROM huge_dataset WHERE status = 'active'")
```

## 예제

[examples](../../examples) 디렉토리에는 filesql의 다양한 기능을 보여주는 샘플 코드가 포함되어 있습니다:

| 예제 | 설명 |
|------|------|
| [basic](../../examples/basic) | 기본 CSV 쿼리 작업 |
| [multi-format](../../examples/multi-format) | 여러 파일 형식 작업 (CSV, TSV, LTSV, Parquet) |
| [sqlc](../../examples/sqlc) | [sqlc](https://sqlc.dev/)와의 통합 - 타입 안전 SQL 코드 생성기 |
| [gorm](../../examples/gorm) | [GORM](https://gorm.io/)과의 통합 - 완전한 기능의 ORM |
| [sqlx](../../examples/sqlx) | [sqlx](https://github.com/jmoiron/sqlx)와의 통합 - database/sql 확장 |
| [bun](../../examples/bun) | [Bun](https://bun.uptrace.dev/)과의 통합 - SQL-first ORM |
| [squirrel](../../examples/squirrel) | [Squirrel](https://github.com/Masterminds/squirrel)과의 통합 - SQL 쿼리 빌더 |
| [ent](../../examples/ent) | [Ent](https://entgo.io/)와의 통합 - Facebook의 엔티티 프레임워크 |

## fileprep을 통한 데이터 전처리

filesql로 쿼리하기 전 데이터 검증 및 전처리에는 **[nao1215/fileprep](https://github.com/nao1215/fileprep)**을 사용하는 것을 권장합니다.

fileprep은 다음 기능을 제공하는 컴패니언 라이브러리입니다:
- **struct 태그 기반 전처리** (`prep` 태그): 트림, 소문자화, 대문자화, 기본값 등
- **struct 태그 기반 검증** (`validate` 태그): 필수 필드, 형식 검증, 필드 간 검증
- **filesql과의 원활한 통합**: filesql의 Builder 패턴에서 직접 사용할 수 있는 `io.Reader` 반환

```go
// 전처리 및 검증 태그가 있는 struct 정의
type User struct {
    // Name: 공백 제거, 비어있지 않아야 함
    Name  string `prep:"trim" validate:"required"`
    // Email: 공백 제거, 소문자로 변환, 이메일 형식 검증
    Email string `prep:"trim,lowercase" validate:"required,email"`
    // Age: 비어있으면 기본값 설정, 0-150 범위 검증
    Age   string `prep:"default=0" validate:"numeric,gte=0,lte=150"`
    // Role: 공백 제거, 대문자화, 허용된 값 중 하나여야 함
    Role  string `prep:"trim,uppercase" validate:"oneof=ADMIN USER GUEST"`
}

func main() {
    // 지저분한 입력이 있는 CSV 데이터
    csvData := `name,email,age,role
  John Doe  ,JOHN@EXAMPLE.COM,25,admin
Alice,alice@example.com,,user`

    // 프로세서 생성 및 CSV 처리
    processor := fileprep.NewProcessor(fileprep.FileTypeCSV)
    var users []User

    reader, result, err := processor.Process(strings.NewReader(csvData), &users)
    if err != nil {
        log.Fatal(err)
    }

    // 검증 결과 확인
    fmt.Printf("처리됨: %d 행, 유효함: %d 행\n", result.RowCount, result.ValidRowCount)
    if result.HasErrors() {
        for _, e := range result.ValidationErrors() {
            log.Printf("행 %d, 열 %s: %s", e.Row, e.Column, e.Message)
        }
    }

    // 전처리된 데이터를 filesql에 전달
    // 데이터가 정리됨: 공백 제거, 이메일 소문자화, 기본값 적용
    ctx := context.Background()
    db, err := filesql.NewBuilder().
        AddReader(reader, "users", filesql.FileTypeCSV).
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 정리된 데이터 쿼리
    rows, _ := db.QueryContext(ctx, "SELECT * FROM users WHERE role = 'ADMIN'")
    // ...
}
```

전처리 및 검증 옵션의 전체 목록은 [fileprep 문서](https://github.com/nao1215/fileprep)를 참조하세요.

## 관련 프로젝트

filesql을 프로젝트에서 사용하고 계신가요? 알려주세요! [이슈를 열어](https://github.com/nao1215/filesql/issues) 알려주시면 아래 목록에 프로젝트를 추가하겠습니다.

### 관련 라이브러리

| 프로젝트 | 설명 |
|---------|------|
| [nao1215/fileprep](https://github.com/nao1215/fileprep) | struct 태그 유효성 검사가 있는 데이터 전처리 라이브러리 |

### filesql을 사용하는 CLI 도구

| 프로젝트 | 설명 |
|---------|------|
| [nao1215/sqly](https://github.com/nao1215/sqly) | CSV, TSV, LTSV, JSON, Excel 파일에 SQL 쿼리를 실행하는 대화형 셸 |
| [kanmu/gocon2025-ctf](https://github.com/kanmu/gocon2025-ctf) | Go Conference 2025 CTF 저장소 (일본어) |

## 기여

기여를 환영합니다! 자세한 내용은 [기여 가이드](../../CONTRIBUTING.md)를 참조하세요.

## 지원

이 프로젝트가 유용하다고 생각하신다면 다음을 고려해 주세요:

- GitHub에서 스타를 눌러주세요 - 다른 사람들이 프로젝트를 발견하는 데 도움이 됩니다
- [스폰서가 되어주세요](https://github.com/sponsors/nao1215) - 여러분의 지원이 프로젝트를 유지하고 지속적인 개발에 동기를 부여합니다

스타, 스폰서십, 기여를 통한 여러분의 지원이 이 프로젝트를 앞으로 나아가게 하는 원동력입니다. 감사합니다!

### 스타 히스토리

[![Star History Chart](https://api.star-history.com/svg?repos=nao1215/filesql&type=date&legend=top-left)](https://www.star-history.com/#nao1215/filesql&Date)

## 라이센스

이 프로젝트는 MIT 라이센스 하에 라이센스가 부여됩니다. 자세한 내용은 [LICENSE](../../LICENSE) 파일을 참조하세요.
