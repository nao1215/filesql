# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

[English](../../README.md) | [Русский](../ru/README.md) | [中文](../zh-cn/README.md) | [Español](../es/README.md) | [Français](../fr/README.md) | [日本語](../ja/README.md)

**filesql**은 SQLite3 SQL 구문을 사용하여 CSV, TSV, LTSV 파일을 쿼리할 수 있게 해주는 Go SQL 드라이버입니다. 가져오기나 변환 없이 데이터 파일을 직접 쿼리하세요!

## 🎯 왜 filesql인가요?

이 라이브러리는 두 개의 별도 CLI 도구인 [sqly](https://github.com/nao1215/sqly)와 [sqluv](https://github.com/nao1215/sqluv)를 유지 관리한 경험에서 탄생했습니다. 두 도구 모두 공통 기능을 공유했습니다: CSV, TSV 및 기타 파일 형식에 대한 SQL 쿼리 실행.

두 프로젝트에서 중복 코드를 유지하는 대신, 핵심 기능을 재사용 가능한 SQL 드라이버로 추출했습니다. 이제 모든 Go 개발자가 자신의 애플리케이션에서 이 기능을 활용할 수 있습니다!

## ✨ 기능

- 🔍 **SQLite3 SQL 인터페이스** - SQLite3의 강력한 SQL 방언을 사용하여 파일 쿼리
- 📁 **다중 파일 형식** - CSV, TSV, LTSV 파일 지원
- 🗜️ **압축 지원** - .gz, .bz2, .xz, .zst 압축 파일 자동 처리
- 🌊 **스트림 처리** - 설정 가능한 청크 크기로 스트리밍을 통해 대용량 파일 효율적 처리
- 📖 **유연한 입력 소스** - 파일 경로, 디렉터리, io.Reader, embed.FS 지원
- 🚀 **제로 설정** - 데이터베이스 서버 불필요, 모든 것이 메모리에서 실행
- 💾 **자동 저장** - 변경사항을 파일에 자동으로 저장
- 🌍 **크로스 플랫폼** - Linux, macOS, Windows에서 원활하게 작동
- ⚡ **SQLite3 기반** - 안정적인 SQL 처리를 위한 견고한 SQLite3 엔진 기반

## 📋 지원되는 파일 형식

| 확장자 | 형식 | 설명 |
|--------|------|------|
| `.csv` | CSV | 쉼표로 구분된 값 |
| `.tsv` | TSV | 탭으로 구분된 값 |
| `.ltsv` | LTSV | 레이블이 있는 탭으로 구분된 값 |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz` | Gzip 압축 | Gzip 압축 파일 |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2` | Bzip2 압축 | Bzip2 압축 파일 |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz` | XZ 압축 | XZ 압축 파일 |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst` | Zstandard 압축 | Zstandard 압축 파일 |

## 📦 설치

```bash
go get github.com/nao1215/filesql
```

## 🚀 빠른 시작

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

// 여러 파일을 한 번에 열기
db, err := filesql.OpenContext(ctx, "users.csv", "orders.tsv", "logs.ltsv.gz")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 다양한 파일 형식에서 데이터 조인
rows, err := db.QueryContext(ctx, `
    SELECT u.name, o.order_date, l.event
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN logs l ON u.id = l.user_id
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

## 🔧 고급 사용법

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
        SetDefaultChunkSize(50*1024*1024). // 50MB 청크
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
```

## 📝 테이블 명명 규칙

filesql은 파일 경로에서 자동으로 테이블 이름을 도출합니다:

- `users.csv` → 테이블 `users`
- `data.tsv.gz` → 테이블 `data`
- `/path/to/sales.csv` → 테이블 `sales`
- `products.ltsv.bz2` → 테이블 `products`

## ⚠️ 중요한 주의사항

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
- 메모리 최적화를 위해 `SetDefaultChunkSize()`로 청크 크기 설정
- 대부분의 시나리오에서 단일 SQLite 연결이 가장 잘 작동
- 사용 가능한 메모리보다 큰 파일에는 스트리밍 사용

## 🎨 고급 예제

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

## 🤝 기여

기여를 환영합니다! 자세한 내용은 [기여 가이드](../../CONTRIBUTING.md)를 참조하세요.

## 💖 지원

이 프로젝트가 유용하다고 생각하신다면 다음을 고려해 주세요:

- ⭐ GitHub에서 스타를 눌러주세요 - 다른 사람들이 프로젝트를 발견하는 데 도움이 됩니다
- 💝 [스폰서가 되어주세요](https://github.com/sponsors/nao1215) - 여러분의 지원이 프로젝트를 유지하고 지속적인 개발에 동기를 부여합니다

스타, 스폰서십, 기여를 통한 여러분의 지원이 이 프로젝트를 앞으로 나아가게 하는 원동력입니다. 감사합니다!

## 📄 라이센스

이 프로젝트는 MIT 라이센스 하에 라이센스가 부여됩니다. 자세한 내용은 [LICENSE](../../LICENSE) 파일을 참조하세요.