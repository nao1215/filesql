# 贡献指南

## 简介

感谢您考虑为 filesql 项目做出贡献！本文档说明了如何为项目做出贡献。我们欢迎各种形式的贡献，包括代码贡献、文档改进、错误报告和功能建议。

## 设置开发环境

### 先决条件

#### 安装 Go

filesql 开发需要 Go 1.24 或更高版本。

**macOS（使用 Homebrew）**
```bash
brew install go
```

**Linux（以 Ubuntu 为例）**
```bash
# 使用 snap
sudo snap install go --classic

# 或从官方网站下载
wget https://go.dev/dl/go1.24.0.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.24.0.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile
source ~/.profile
```

**Windows**
从 [Go 官方网站](https://go.dev/dl/) 下载并运行安装程序。

验证安装：
```bash
go version
```

### 克隆项目

```bash
git clone https://github.com/nao1215/filesql.git
cd filesql
```

### 安装开发工具

```bash
# 安装必要的开发工具
make tools
```

### 验证

要验证您的开发环境是否正确设置，请运行以下命令：

```bash
# 运行测试
make test

# 运行 linter
make lint
```

## 开发工作流程

### 分支策略

- `main` 分支是最新的稳定版本
- 为新功能或错误修复从 `main` 创建新分支
- 分支命名示例：
  - `feature/add-json-support` - 新功能
  - `fix/issue-123` - 错误修复
  - `docs/update-readme` - 文档更新

### 编码标准

本项目遵循以下标准：

1. **遵循 [Effective Go](https://go.dev/doc/effective_go)**
2. **避免使用全局变量**（config 包除外）
3. **始终为公共函数、变量和结构体添加注释**
4. **尽可能保持函数小巧**
5. **鼓励编写测试**

### 编写测试

测试很重要。请遵循以下准则：

1. **单元测试**：目标覆盖率 80% 或更高
2. **测试可读性**：编写清晰的测试用例
3. **并行执行**：尽可能使用 `t.Parallel()`

测试示例：
```go
func TestFile_Parse(t *testing.T) {
    t.Parallel()
    
    t.Run("should parse CSV file correctly", func(t *testing.T) {
        // 清晰的测试用例输入和预期值
        input := "name,age\nAlice,30"
        expected := &Table{...}
        
        result, err := ParseCSV(input)
        assert.NoError(t, err)
        assert.Equal(t, expected, result)
    })
}
```

## 使用 AI 助手（LLM）

我们积极鼓励使用 AI 编码助手来提高生产力和代码质量。Claude Code、GitHub Copilot 和 Cursor 等工具可用于：

- 编写样板代码
- 生成全面的测试用例
- 改进文档
- 重构现有代码
- 发现潜在的错误
- 建议性能优化
- 翻译文档

### AI 辅助开发指南

1. **审查所有生成的代码**：始终审查并理解 AI 生成的代码后再提交
2. **保持一致性**：确保 AI 生成的代码遵循 CLAUDE.md 中的编码标准
3. **彻底测试**：AI 生成的代码必须通过所有测试和代码检查（`make test` 和 `make lint`）
4. **使用项目配置**：我们提供 `CLAUDE.md`, `.cursorrules` 和 `.github/copilot-instructions.md` 来帮助 AI 助手理解我们的项目标准

## 创建拉取请求

### 准备工作

1. **检查或创建问题**
   - 检查是否有现有问题
   - 对于重大更改，建议先在问题中讨论方法

2. **编写测试**
   - 始终为新功能添加测试
   - 对于错误修复，创建重现错误的测试
   - AI 工具可以帮助生成全面的测试用例

3. **质量检查**
   ```bash
   # 确保所有测试通过
   make test
   
   # Linter 检查
   make lint
   
   # 检查覆盖率（80% 或更高）
   go test -cover ./...
   ```

### 提交拉取请求

1. 从您的分叉仓库向主仓库创建拉取请求
2. PR 标题应简要描述更改
3. 在 PR 描述中包括以下内容：
   - 更改的目的和内容
   - 相关问题编号（如果有）
   - 测试方法
   - 错误修复的重现步骤

### 关于 CI/CD

GitHub Actions 自动检查以下项目：

- **跨平台测试**：在 Linux、macOS 和 Windows 上执行测试
- **Linter 检查**：使用 golangci-lint 进行静态分析
- **测试覆盖率**：保持 80% 或更高的覆盖率
- **构建验证**：在每个平台上成功构建

除非所有检查都通过，否则无法合并。

## 错误报告

当您发现错误时，请创建包含以下信息的问题：

1. **环境信息**
   - 操作系统（Linux/macOS/Windows）和版本
   - Go 版本
   - filesql 版本

2. **重现步骤**
   - 重现错误的最小代码示例
   - 使用的数据文件（如果可能）

3. **预期和实际行为**

4. **错误消息或堆栈跟踪**（如果有）

## 代码之外的贡献

以下活动也非常受欢迎：

### 提高积极性的活动

- **给予 GitHub Star**：表达您对项目的兴趣
- **推广项目**：在博客、社交媒体、学习小组等介绍
- **成为 GitHub 赞助者**：可通过 [https://github.com/sponsors/nao1215](https://github.com/sponsors/nao1215) 支持

### 其他贡献方式

- **文档改进**：修正错别字，改进说明的清晰度
- **翻译**：将文档翻译成新语言
- **添加示例**：提供实用的示例代码
- **功能建议**：在问题中分享新功能想法

## 社区

### 行为准则

请参考 [CODE_OF_CONDUCT.md](../../CODE_OF_CONDUCT.md)。我们期望所有贡献者相互尊重。

### 问题和报告

- **GitHub Issues**：错误报告和功能建议

## 许可证

对本项目的贡献被视为在项目许可证（MIT 许可证）下发布。

---

再次感谢您考虑做出贡献！我们真诚期待您的参与。