# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

**filesql**은 SQLite3 SQL 구문을 사용하여 CSV, TSV, LTSV 파일을 쿼리할 수 있게 해주는 Go SQL 드라이버입니다. 가져오기나 변환 없이 데이터 파일을 직접 쿼리하세요!

## 🎯 왜 filesql인가요?

이 라이브러리는 두 개의 별도 CLI 도구 - [sqly](https://github.com/nao1215/sqly)와 [sqluv](https://github.com/nao1215/sqluv)를 유지보수한 경험에서 탄생했습니다. 두 도구 모두 CSV, TSV 및 기타 파일 형식에 대해 SQL 쿼리를 실행하는 공통 기능을 가지고 있었습니다.

두 프로젝트에서 중복 코드를 유지보수하는 대신, 핵심 기능을 이 재사용 가능한 SQL 드라이버로 추출했습니다. 이제 모든 Go 개발자가 자신의 애플리케이션에서 이 기능을 활용할 수 있습니다!

## ✨ 기능

- 🔍 **SQLite3 SQL 인터페이스** - SQLite3의 강력한 SQL 방언을 사용하여 파일을 쿼리
- 📁 **다중 파일 형식** - CSV, TSV, LTSV 파일 지원
- 🗜️ **압축 지원** - .gz, .bz2, .xz, .zst 압축 파일 자동 처리
- 🚀 **제로 설정** - 데이터베이스 서버 불필요, 모든 것이 메모리에서 실행
- 🌍 **크로스 플랫폼** - Linux, macOS, Windows에서 원활하게 작동
- 💾 **SQLite3 기반** - 안정적인 SQL 처리를 위해 견고한 SQLite3 엔진 위에 구축

## 📋 지원되는 파일 형식

| 확장자 | 형식 | 설명 |
|-----------|--------|-------------|
| `.csv` | CSV | 쉼표로 구분된 값 |
| `.tsv` | TSV | 탭으로 구분된 값 |
| `.ltsv` | LTSV | 레이블 탭 구분 값 |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz` | Gzip 압축 | Gzip 압축 파일 |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2` | Bzip2 압축 | Bzip2 압축 파일 |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz` | XZ 압축 | XZ 압축 파일 |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst` | Zstandard 압축 | Zstandard 압축 파일 |


## 📦 설치

```bash
go get github.com/nao1215/filesql
```

## 🚀 빠른 시작

[예제 코드는 여기에 있습니다](../../example_test.go).

### 기본 사용법

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
    // 컨텍스트와 함께 CSV 파일을 데이터베이스로 열기
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    db, err := filesql.OpenContext(ctx, "data.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // SQL 쿼리 실행 (테이블 이름은 확장자 없는 파일명에서 파생됨)
    rows, err := db.QueryContext(ctx, "SELECT * FROM data WHERE age > 25 ORDER BY name")
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
        fmt.Printf("Name: %s, Age: %d\n", name, age)
    }
}
```

### Context 지원으로 열기

```go
// 타임아웃 제어로 파일 열기
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "large_dataset.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 취소 지원을 위한 컨텍스트로 쿼리
rows, err := db.QueryContext(ctx, "SELECT * FROM large_dataset WHERE status = 'active'")
```

### 여러 파일 열기

```go
// 단일 데이터베이스에서 여러 파일 열기
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "users.csv", "orders.tsv", "products.ltsv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 다른 파일 형식 간 데이터 조인!
rows, err := db.QueryContext(ctx, `
    SELECT u.name, o.order_date, p.product_name
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN products p ON o.product_id = p.id
    WHERE o.order_date > '2024-01-01'
`)
```

### 디렉토리 작업

```go
// 디렉토리의 모든 지원 파일 열기 (재귀적)
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "/path/to/data/directory")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 로드된 모든 테이블 쿼리
rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
```

### 압축 파일 지원

```go
// 압축 파일 자동 처리
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "large_dataset.csv.gz", "archive.tsv.bz2")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 압축된 데이터를 원활하게 쿼리
rows, err := db.QueryContext(ctx, "SELECT COUNT(*) FROM large_dataset")
```

### 테이블 명명 규칙

filesql은 파일 경로에서 자동으로 테이블 이름을 도출합니다:

```go
// 테이블 명명 예제:
// "users.csv"           -> 테이블 이름: "users"
// "data.tsv"            -> 테이블 이름: "data"
// "logs.ltsv"           -> 테이블 이름: "logs"
// "archive.csv.gz"      -> 테이블 이름: "archive"
// "backup.tsv.bz2"      -> 테이블 이름: "backup"
// "/path/to/sales.csv"  -> 테이블 이름: "sales"

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "employees.csv", "departments.tsv.gz")
if err != nil {
    log.Fatal(err)
}

// 쿼리에서 도출된 테이블 이름 사용
rows, err := db.QueryContext(ctx, `
    SELECT * FROM employees 
    JOIN departments ON employees.dept_id = departments.id
`)
```

## ⚠️ 중요한 참고사항

### SQL 구문
filesql은 SQLite3를 기본 엔진으로 사용하므로 모든 SQL 구문은 [SQLite3의 SQL 방언](https://www.sqlite.org/lang.html)을 따릅니다. 여기에는 다음이 포함됩니다:
- 함수 (예: `date()`, `substr()`, `json_extract()`)
- 윈도우 함수
- 공통 테이블 표현식(CTE)
- 그리고 더 많은 것들!

### 데이터 수정
- `INSERT`, `UPDATE`, `DELETE` 작업은 메모리 내 데이터베이스에만 영향을 미칩니다
- **원본 파일은 변경되지 않습니다** - filesql은 소스 파일을 절대 수정하지 않습니다
- 이로 인해 데이터 변환을 안전하게 실험할 수 있습니다

### 고급 SQL 기능

filesql은 SQLite3를 사용하므로 그 전체 기능을 활용할 수 있습니다:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "employees.csv", "departments.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 윈도우 함수, CTE, 복잡한 쿼리 사용
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

### 수정된 데이터 내보내기

메모리 내 데이터베이스에 가한 변경사항을 유지해야 하는 경우:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "data.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 수정하기
_, err = db.ExecContext(ctx, "UPDATE data SET status = 'processed' WHERE status = 'pending'")
if err != nil {
    log.Fatal(err)
}

// 수정된 데이터를 새 디렉토리로 내보내기
err = filesql.DumpDatabase(db, "/path/to/output/directory")
if err != nil {
    log.Fatal(err)
}
```

## 🤝 기여

기여를 환영합니다! 자세한 내용은 [기여 가이드](CONTRIBUTING.md)를 참조하세요.

## 💖 지원

이 프로젝트가 유용하다고 생각하시면 다음을 고려해 주세요:

- ⭐ GitHub에서 스타 주기 - 다른 사람들이 프로젝트를 발견하는 데 도움이 됩니다
- 💝 [후원자 되기](https://github.com/sponsors/nao1215) - 여러분의 지원이 프로젝트를 유지하고 지속적인 개발에 동기를 부여합니다

스타, 후원, 기여 등 여러분의 지원이 이 프로젝트를 앞으로 나아가게 하는 원동력입니다. 감사합니다!

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 라이선스가 부여됩니다 - 자세한 내용은 [LICENSE](../../LICENSE) 파일을 참조하세요.