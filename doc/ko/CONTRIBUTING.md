# 기여 가이드

## 소개

filesql 프로젝트에 기여를 고려해 주셔서 감사합니다! 이 문서는 프로젝트에 기여하는 방법을 설명합니다. 코드 기여, 문서 개선, 버그 보고, 기능 제안 등 모든 형태의 기여를 환영합니다.

## 개발 환경 설정

### 사전 요구사항

#### Go 설치

filesql 개발에는 Go 1.24 이상이 필요합니다.

**macOS (Homebrew 사용)**
```bash
brew install go
```

**Linux (Ubuntu의 경우)**
```bash
# snap 사용
sudo snap install go --classic

# 또는 공식 사이트에서 다운로드
wget https://go.dev/dl/go1.24.0.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.24.0.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile
source ~/.profile
```

**Windows**
[Go 공식 웹사이트](https://go.dev/dl/)에서 설치 프로그램을 다운로드하여 실행하세요.

설치 확인:
```bash
go version
```

### 프로젝트 복제

```bash
git clone https://github.com/nao1215/filesql.git
cd filesql
```

### 개발 도구 설치

```bash
# 필요한 개발 도구 설치
make tools
```

### 확인

개발 환경이 올바르게 설정되었는지 확인하려면 다음 명령을 실행하세요:

```bash
# 테스트 실행
make test

# 린터 실행
make lint
```

## 개발 워크플로우

### 브랜치 전략

- `main` 브랜치가 최신 안정 버전입니다
- 새 기능이나 버그 수정을 위해 `main`에서 새 브랜치를 생성하세요
- 브랜치 이름 예시:
  - `feature/add-json-support` - 새 기능
  - `fix/issue-123` - 버그 수정
  - `docs/update-readme` - 문서 업데이트

### 코딩 표준

이 프로젝트는 다음 표준을 따릅니다:

1. **[Effective Go](https://go.dev/doc/effective_go) 준수**
2. **전역 변수 사용 방지** (config 패키지 제외)
3. **공개 함수, 변수, 구조체에는 항상 주석 추가**
4. **함수를 가능한 한 작게 유지**
5. **테스트 작성 권장**

### 테스트 작성

테스트는 중요합니다. 다음 지침을 따르세요:

1. **단위 테스트**: 80% 이상의 커버리지 목표
2. **테스트 가독성**: 명확한 테스트 케이스 작성
3. **병렬 실행**: 가능한 한 `t.Parallel()` 사용

테스트 예제:
```go
func TestFile_Parse(t *testing.T) {
    t.Parallel()
    
    t.Run("should parse CSV file correctly", func(t *testing.T) {
        // 테스트 케이스의 명확한 입력과 예상 값
        input := "name,age\nAlice,30"
        expected := &Table{...}
        
        result, err := ParseCSV(input)
        assert.NoError(t, err)
        assert.Equal(t, expected, result)
    })
}
```

## AI 어시스턴트(LLM) 활용

생산성과 코드 품질 향상을 위해 AI 코딩 어시스턴트 사용을 적극 권장합니다. Claude Code, GitHub Copilot, Cursor 같은 도구들을 다음 용도로 활용할 수 있습니다:

- 보일러플레이트 코드 작성
- 포괄적인 테스트 케이스 생성
- 문서 개선
- 기존 코드 리팩토링
- 잠재적 버그 발견
- 성능 최적화 제안
- 문서 번역

### AI 지원 개발 가이드라인

1. **생성된 코드 검토**: AI가 생성한 코드는 항상 검토하고 이해한 후 커밋하세요
2. **일관성 유지**: AI 생성 코드가 CLAUDE.md의 코딩 표준을 따르는지 확인하세요
3. **철저한 테스트**: AI 생성 코드는 모든 테스트와 린팅(`make test`와 `make lint`)을 통과해야 합니다
4. **프로젝트 설정 사용**: AI 어시스턴트가 프로젝트 표준을 이해할 수 있도록 `CLAUDE.md`, `.cursorrules`와 `.github/copilot-instructions.md`를 제공합니다

## Pull Request 생성

### 준비

1. **이슈 확인 또는 생성**
   - 기존 이슈가 있는지 확인하세요
   - 주요 변경사항의 경우, 먼저 이슈에서 접근 방법을 논의하는 것을 권장합니다

2. **테스트 작성**
   - 새 기능에는 항상 테스트를 추가하세요
   - 버그 수정의 경우, 버그를 재현하는 테스트를 생성하세요
   - AI 도구를 사용하여 포괄적인 테스트 케이스를 생성할 수 있습니다

3. **품질 확인**
   ```bash
   # 모든 테스트가 통과하는지 확인
   make test
   
   # 린터 확인
   make lint
   
   # 커버리지 확인 (80% 이상)
   go test -cover ./...
   ```

### Pull Request 제출

1. 포크한 저장소에서 메인 저장소로 Pull Request 생성
2. PR 제목은 변경사항을 간략하게 설명
3. PR 설명에 다음 내용 포함:
   - 변경사항의 목적과 내용
   - 관련 이슈 번호 (있는 경우)
   - 테스트 방법
   - 버그 수정의 경우 재현 단계

### CI/CD 정보

GitHub Actions가 다음 항목을 자동으로 확인합니다:

- **크로스 플랫폼 테스트**: Linux, macOS, Windows에서 테스트 실행
- **린터 확인**: golangci-lint를 사용한 정적 분석
- **테스트 커버리지**: 80% 이상의 커버리지 유지
- **빌드 확인**: 각 플랫폼에서 성공적인 빌드

모든 확인이 통과하지 않으면 병합할 수 없습니다.

## 버그 보고

버그를 발견하면 다음 정보를 포함하여 이슈를 생성하세요:

1. **환경 정보**
   - OS (Linux/macOS/Windows) 및 버전
   - Go 버전
   - filesql 버전

2. **재현 단계**
   - 버그를 재현하기 위한 최소 코드 예제
   - 사용된 데이터 파일 (가능한 경우)

3. **예상 동작과 실제 동작**

4. **오류 메시지 또는 스택 추적** (있는 경우)

## 코드 외 기여

다음 활동도 매우 환영합니다:

### 동기부여를 높이는 활동

- **GitHub Star 주기**: 프로젝트에 대한 관심 표시
- **프로젝트 홍보**: 블로그, 소셜 미디어, 스터디 그룹 등에서 소개
- **GitHub 스폰서 되기**: [https://github.com/sponsors/nao1215](https://github.com/sponsors/nao1215)에서 지원 가능

### 기타 기여 방법

- **문서 개선**: 오타 수정, 설명의 명확성 개선
- **번역**: 새로운 언어로 문서 번역
- **예제 추가**: 실용적인 샘플 코드 제공
- **기능 제안**: 이슈에서 새로운 기능 아이디어 공유

## 커뮤니티

### 행동 강령

[CODE_OF_CONDUCT.md](../../CODE_OF_CONDUCT.md)를 참조하세요. 모든 기여자가 서로를 존중하기를 기대합니다.

### 질문 및 보고

- **GitHub Issues**: 버그 보고 및 기능 제안

## 라이선스

이 프로젝트에 대한 기여는 프로젝트 라이선스(MIT 라이선스) 하에 공개되는 것으로 간주됩니다.

---

기여를 고려해 주셔서 다시 한 번 감사드립니다! 여러분의 참여를 진심으로 기다리고 있습니다.