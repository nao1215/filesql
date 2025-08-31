# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.1] - 2025-08-31

### Added
- **🔧 CI/CD Automation**: Enhanced development workflow with automated processes
  - **GitHub Actions integration**: Added Claude-powered PR assistant and code review workflows
  - **Automated release process**: Auto-release workflow triggered by tag creation
  - **Comprehensive development tooling**: Streamlined development experience with AI assistance
- **📚 LLM Development Support**: Comprehensive AI assistant integration for development
  - **Multi-platform LLM support**: Added configuration files for Claude Code, Cursor, and GitHub Copilot
  - **Development guidelines**: Created detailed contributing guides in 7 languages (EN, JA, ES, FR, RU, KO, ZH-CN)
  - **Coding standards documentation**: Comprehensive guidelines for AI-assisted development
  - **International contributor support**: Multi-language documentation for global development team
- **🔍 Enhanced Edge Case Testing**: Expanded test coverage for robustness
  - **Error handling validation**: Additional tests for edge cases and error conditions
  - **Stream processing edge cases**: Enhanced testing for unusual input scenarios
  - **Builder pattern validation**: More comprehensive validation of configuration edge cases

### Changed
- **🧪 Testing Framework Modernization**: Migrated to testify for improved test maintainability
  - **Reduced test code complexity**: Replaced verbose manual assertions with concise testify assertions
  - **Improved test readability**: Cleaner test structure using `assert` and `require` functions
  - **Enhanced test reliability**: Better error messages and assertion failures with testify
  - **Code reduction**: Significantly reduced test code lines (over 600 lines removed) while maintaining coverage
- **🎯 Test Stability Improvements**: Enhanced test reliability and performance
  - **Fixed flaky tests**: Resolved intermittent test failures in concurrent scenarios
  - **Local development optimization**: Added conditions to skip heavy tests in local environments
  - **Better test isolation**: Improved test independence and parallel execution safety

### Technical Improvements
- **Test framework migration**: Complete transition from manual assertions to testify
- **CI/CD pipeline enhancement**: Automated release and review processes
- **Development documentation**: Comprehensive guides for contributors
- **Code quality**: Maintained high test coverage while reducing code complexity
- **International support**: Multi-language documentation for global development

### Dependencies
- **Added**: `github.com/stretchr/testify v1.11.1` for enhanced testing capabilities

## [0.4.0] - 2025-08-30

### Added
- **🎉 Excel (XLSX) Support**: Complete Microsoft Excel XLSX file support with 1-sheet-1-table architecture
  - **Multi-sheet processing**: Each Excel sheet becomes a separate SQL table with naming format `{filename}_{sheetname}`
  - **Full-featured XLSX integration**: 
    - Header row processing from first row of each sheet
    - Support for compressed XLSX files (`.xlsx.gz`, `.xlsx.bz2`, `.xlsx.xz`, `.xlsx.zst`)
    - Multi-sheet JOIN operations across different sheets in the same workbook
    - Export functionality to XLSX format with table names automatically becoming sheet names
  - **XLSX streaming parser**: Memory-efficient processing using `excelize.Rows()` iterator
    - Eliminated double memory allocation for better performance
    - Added duplicate header validation for parity with CSV/TSV parsers
    - Streaming parser processes first sheet only (use `Open`/`OpenContext` for multi-sheet support)
- **Enhanced Security**: Safe SQL identifier handling
  - `quoteIdent()` function for proper SQLite identifier escaping
  - Sanitized table name generation with `sanitizeTableName()` for all file types
  - Protection against SQL injection through identifier names

### Fixed
- **🐛 Critical Windows Compatibility**: Fixed Windows test failures in `TestIntegrationWithEmbedFS`
  - Replaced `filepath.Join()` with forward slashes for embed.FS paths to prevent Windows path separator issues
  - Fixed similar issues in `example_test.go` for consistent cross-platform behavior
- **📊 Excel Column Limit Bug**: Fixed 26+ column support in Excel export operations
  - Replaced arithmetic-based column naming (`'A'+i`) with `excelize.CoordinatesToCellName()`
  - Now supports unlimited columns: 27th column becomes `AA`, 28th becomes `AB`, etc.
  - Proper error handling for coordinate generation failures
- **🔍 Case-Insensitive File Detection**: Enhanced compression file detection
  - Made `isCompressedFile()` case-insensitive to match other file type detection functions
  - Files like `.CSV.GZ`, `.TSV.BZ2` now properly detected alongside `.csv.gz`, `.tsv.bz2`
- **📁 Compressed File Path Handling**: Fixed table name derivation for compressed XLSX files
  - Files like `data.xlsx.gz` now correctly produce table name `data` instead of `data.xlsx`
  - Improved logic: first strips compression extension, then strips file extension
- **⚡ XLSX Streaming Performance**: Major optimization in XLSX streaming parser
  - **Eliminated double memory allocation**: Removed `io.ReadAll()` + `GetRows()` pattern
  - **True streaming implementation**: Direct use of `excelize.OpenReader()` + `Rows()` iterator
  - **Memory usage reduction**: 50-70% less memory usage for large XLSX files
  - **Improved error handling**: Better error messages with row/column context

### Changed
- **📚 Comprehensive Documentation Updates**: Updated all README files across 7 languages (EN, JA, ES, FR, RU, KO, ZH-CN)
  - **Corrected Parquet status**: Updated "planned but not implemented" to "implemented with caveats"
  - **Added Excel (XLSX) documentation**: Comprehensive sections with examples, architecture diagrams, and usage patterns
  - **Fixed XLSX streaming descriptions**: Clarified that XLSX files are fully loaded and all sheets are processed
  - **Enhanced export examples**: Added Parquet and XLSX export examples with proper annotations
  - **Multi-language consistency**: Ensured technical accuracy across all language versions
- **🏗️ Enhanced Builder Pattern**: Improved table name sanitization and validation
  - Base table names for XLSX files are now sanitized before sheet name concatenation
  - Better handling of special characters and invalid identifiers in file paths

### Internal Improvements
- **📈 Test Coverage**: Maintained 83.2% test coverage with enhanced reliability
- **🧹 Code Quality**: Removed unused imports and improved code consistency
- **🔧 Architecture**: Enhanced streaming architecture for better memory efficiency
- **🛡️ Error Handling**: Improved error messages with more context and actionable information

### Breaking Changes
**⚠️ XLSX File Behavior Change**: 
- XLSX files now create **multiple tables** (one per sheet) instead of a single table
- Table names follow the `{filename}_{sheetname}` pattern (e.g., `sales_Q1`, `sales_Q2`)
- This enables full utilization of multi-sheet Excel workbooks but changes the table structure

### Migration Notes
For users upgrading from v0.3.x:
1. **XLSX files**: Expect multiple tables instead of one. Update queries to reference specific sheet tables.
2. **Streaming parsers**: XLSX streaming parsers now process only the first sheet. Use `Open`/`OpenContext` for multi-sheet support.
3. **Table names**: XLSX-derived table names now include sheet names. Update any hardcoded table references.

## [0.3.0] - 2025-08-30

### Added
- **🎉 Parquet file format support (v0.3.0)**: Complete Apache Parquet integration with streaming capabilities
  - **Full Parquet read/write functionality**: Complete implementation using Apache Arrow Go library (v18)
    - `writeParquetData()` function with schema inference and data conversion
    - `parseParquet()` and `parseCompressedParquet()` for reading Parquet files
    - Support for both uncompressed and externally compressed Parquet files (.parquet.gz, .parquet.bz2, .parquet.xz, .parquet.zst)
  - **Parquet streaming support**: Memory-efficient processing for large Parquet files
    - `parseParquetStream()` method for streaming Parquet data from io.Reader
    - `processParquetInChunks()` for chunked processing with configurable batch sizes
    - `bytesReaderAt` helper for random access requirements
  - **Export functionality**: Parquet output format in database dump operations
    - `OutputFormatParquet` enum value for export configuration
    - Integration with existing `DumpDatabase()` function and `DumpOptions`
    - Maintains schema and data type information during export
- **Comprehensive Parquet testing**: Extensive test coverage for all Parquet functionality
  - Integration tests for Parquet read/write operations with real data
  - Streaming functionality tests with chunked processing
  - Compressed Parquet file handling tests
  - Cross-format compatibility tests (CSV → Parquet → SQLite)

### Changed
- **Unified streaming architecture**: All file formats now use consistent streaming approach
  - Consolidated file processing pipeline through `streamReaderToSQLite()`
  - Removed format-specific processing functions in favor of unified stream handling
  - Enhanced memory efficiency across all supported formats (CSV, TSV, LTSV, Parquet)
- **Enhanced test coverage**: Improved from 73.5% to 80.7% coverage (exceeding 80% target)
  - Added comprehensive tests for dump options functionality
  - Enhanced column inference testing with mixed data types
  - Added LTSV chunk processing tests for better coverage
  - Expanded Parquet-specific test scenarios

### Fixed
- **Code quality improvements**: Resolved all linting issues (13 total issues fixed)
  - **errcheck**: Fixed unchecked error returns with proper error handling
  - **gofmt**: Applied consistent code formatting across all files
  - **gosec**: Addressed security issues with appropriate nolint annotations for test files
  - **noctx**: Updated database operations to use context-aware methods (`BeginTx`, `ExecContext`)
- **Concurrent access simplification**: Removed complex goroutine usage in favor of simpler, more reliable patterns
  - Simplified database connection management per user feedback
  - Enhanced test reliability and reduced race condition potential
- **Memory management**: Improved resource cleanup in Parquet processing
  - Proper memory allocator usage with Apache Arrow
  - Better error handling for Parquet file operations
  - Enhanced cleanup of temporary resources during streaming

### Technical Details
- **Apache Arrow integration**: Leverages Apache Arrow Go library for efficient Parquet processing
- **Schema preservation**: Maintains data types and column information across format conversions
- **Cross-platform compatibility**: Verified Parquet functionality on Linux, macOS, and Windows
- **Performance optimization**: Streaming approach reduces memory footprint for large files
- **Documentation updates**: All 7 language README files updated with Parquet support examples
- **Lint compliance**: Achieved zero linting issues with proper error handling and context usage

## [0.2.0] - 2025-08-27

### Added
- **🎉 Major architecture enhancement (v0.2.0)**: Stream processing support and domain model restructuring
- **Stream processing capabilities**: Complete stream-based file loading for improved memory efficiency
  - `AddReader()` method in Builder pattern for stream input support
  - Chunked reading for local files to handle large datasets efficiently
  - Memory-optimized processing for both local files and streaming data
  - Stream-friendly auto-save functionality with proper resource management
- **Integration testing framework**: Comprehensive BDD-style integration tests using Ginkgo/Gomega
  - Full end-to-end behavior validation for library functionality
  - Stream processing integration tests with various data sources
  - Auto-save functionality testing across different scenarios
  - Cross-platform compatibility verification

### Changed
- **Breaking change**: Domain model architecture restructuring for improved maintainability
  - Moved all model types from `domain/model` package to main `filesql` package
  - Simplified import structure and reduced package complexity
  - Enhanced type organization and accessibility for library users
  - Streamlined API with consolidated model definitions
- **Enhanced file loading system**: Improved file processing with stream support
  - Unified file loading approach supporting both file paths and streams
  - Better memory management for large file processing
  - Enhanced chunked reading implementation for local files
  - Improved error handling and resource cleanup

### Fixed
- **Auto-save functionality**: Resolved limitations and edge cases in auto-save operations
  - Fixed auto-save behavior with stream inputs and temporary files
  - Improved handling of auto-save with various input sources
  - Enhanced error recovery and cleanup during auto-save operations
  - Better validation for auto-save configuration consistency
- **Stream processing stability**: Enhanced reliability of stream-based operations
  - Proper resource management for stream readers
  - Improved error handling in chunked reading scenarios
  - Fixed memory leaks in stream processing pipeline

### Technical Details
- **Architecture simplification**: Reduced package complexity while maintaining functionality
- **Memory optimization**: Improved memory usage patterns for large dataset processing
- **Test coverage enhancement**: Added comprehensive integration tests with Ginkgo/Gomega
- **Code organization**: Better separation of concerns with unified model location
- **Performance improvements**: Enhanced processing efficiency for both small and large files

## [0.1.0] - 2025-08-26

### Added
- **🎉 Initial major feature release (v0.1.0)**: Library with comprehensive Builder pattern and auto-save functionality
- **Builder pattern architecture**: Complete implementation of extensible Builder pattern for flexible configuration
  - `NewBuilder()` provides fluent API for database construction
  - `AddPath()` method for adding individual files and directories
  - `AddFS()` method for embedded filesystem support (go:embed compatibility)
  - `EnableAutoSave()` and `EnableAutoSaveOnCommit()` for automatic data persistence
  - `Build()` method with comprehensive validation and error checking
  - Chainable method design for clean, readable configuration code
- **go:embed and fs.FS support**: Full integration with Go's embedded filesystem capabilities
  - Works seamlessly with `//go:embed` directive for embedded data files
  - Custom `fs.FS` implementation support for advanced use cases
  - Automatic temporary file management for embedded content
  - Cross-platform embedded file handling
- **Advanced auto-save functionality**: Comprehensive automatic data persistence system
  - **Two timing modes**: Save on database close (`OnClose`) or transaction commit (`OnCommit`)
  - **Overwrite mode**: Automatically saves back to original file locations when output directory is empty
  - **Directory mode**: Saves to specified backup directory with original file names
  - **Format preservation**: Maintains original file formats (CSV, TSV, LTSV) and compression
  - **Configurable compression**: Support for gzip, bzip2, xz, and zstd compression options
  - **Transaction integration**: Seamless integration with database transaction lifecycle

### Changed
- **Breaking change**: Enhanced driver interface with auto-save configuration support
  - Extended `Connection` struct with auto-save capabilities and original path tracking
  - Updated `Connector` interface to support Builder-generated configurations
  - DSN format extended to include JSON-encoded auto-save configuration via base64 encoding
- **Enhanced export system**: Improved table export with comprehensive format support
  - Extended `DumpOptions` with detailed format and compression configuration
  - Enhanced compression detection and writer creation pipeline
  - Improved error handling with proper resource cleanup and partial file removal
  - Better cross-platform file path handling and sanitization

### Fixed
- **Auto-save overwrite mode**: Fixed critical issue where overwrite mode incorrectly used current working directory
  - Now properly uses original input file locations for file overwrites
  - Maintains correct directory structure and file naming conventions
  - Preserves original file formats and compression settings automatically
- **Builder validation**: Enhanced configuration validation with detailed error reporting
- **Memory management**: Improved cleanup of temporary files created from embedded filesystems

### Technical Details
- **Feature completeness**: v0.1.0 introduces major Builder pattern and auto-save functionality
- **Comprehensive testing**: Extensive test coverage including Builder pattern, auto-save functionality, and embedded filesystem support
- **Documentation updates**: All 7 language README files updated with auto-save examples and Builder pattern usage
- **Code quality**: All linting issues resolved, comprehensive error handling with `errors.Join()` (Go 1.20+)
- **Cross-platform compatibility**: Verified functionality across Linux, macOS, and Windows with embedded filesystems

## [0.0.4] - 2025-08-24

### Added
- **Version 0.0.4 release**: Minor version update with maintenance improvements

### Changed
- Project maintenance and version management updates

### Technical Details
- **Version management**: Updated version tracking for release v0.0.4

## [0.0.3] - 2025-08-24

### Added
- **Enhanced security compliance**: Added gosec security linter to the build process
  - Comprehensive security analysis for potential vulnerabilities
  - File permission restrictions (0600 for files, 0750 for directories)
  - Protection against SQL injection and file inclusion vulnerabilities
- **Duplicate validation system**: Implemented robust duplicate detection mechanisms
  - **Table name validation**: Prevents multiple files from creating tables with identical names
  - **Column name validation**: Detects and rejects files with duplicate column headers
  - **Cross-directory validation**: Ensures uniqueness across multiple input paths
  - **Compression preference logic**: Automatically prefers uncompressed files over compressed versions
- **Comprehensive test coverage expansion**: Significantly increased driver package coverage
  - Driver package coverage increased from 73.5% to 83.9%
  - Added extensive transaction testing, connection management, and error handling tests
  - Enhanced export functionality testing and helper method validation
  - Overall project coverage maintained at 80.4%

### Changed
- **Major driver.go refactoring**: Complete architectural reorganization for improved maintainability
  - **Method decomposition**: Split complex methods into focused, single-responsibility functions
    - `loadFileDirectly` → `loadSinglePath`, `validatePath`
    - `loadSingleFile` → `parseFileToTable`, `loadTableIntoDatabase`
    - `collectDirectoryFiles` → `readDirectoryEntries`, `shouldSkipFile`, `handleTableNameConflict`
    - `loadMultiplePaths` → `collectAllFiles`, `collectFilesFromPath`, `collectSingleFile`
  - **Database operations unification**: Centralized query execution and statement handling
    - `executeQuery`: Unified interface for all database queries
    - `executeStatement`: Consistent statement execution with proper context support
    - `scanStringValues`: Standardized database response processing
  - **CSV export enhancement**: Modular CSV generation pipeline
    - `writeCSVFile`, `writeDataRows`, `convertRowToCSVRecord`: Clean separation of concerns
    - Improved error handling and resource management
  - **Enhanced documentation**: Comprehensive package and method documentation
    - Detailed usage examples and feature descriptions
    - Clear API documentation for all public interfaces
- **Improved error handling consistency**: Standardized error formatting and path validation
- **Cross-platform compatibility improvements**: Enhanced Windows/Unix path handling compatibility

### Fixed
- **Security vulnerabilities**: Addressed all gosec security findings
  - **G104 (Unhandled Errors)**: Proper error handling in all file and database operations
  - **G201/G202 (SQL Injection)**: Secure SQL query construction with parameterization
  - **G301/G302/G306 (File Permissions)**: Restricted file and directory permissions for security
  - **G304 (File Inclusion)**: Safe file path handling with proper validation
- **Cross-platform path issues**: Fixed Windows filepath separator compatibility
  - Normalized path comparisons using `filepath.Clean()` for consistent behavior
  - Unified output path formatting in examples and tests
  - Resolved GitHub Actions Windows test failures
- **Code quality improvements**: 
  - All linting issues resolved with stricter gosec configuration
  - Proper code formatting with gofmt
  - Performance optimizations (replaced `fmt.Sprintf` with `strconv.Itoa` where appropriate)

### Technical Details
- **Security hardening**: Comprehensive security audit and remediation
- **Architecture improvement**: Clean code principles applied throughout driver implementation
- **Testing enhancement**: Robust test suite covering edge cases and error scenarios
- **Documentation quality**: Improved code documentation and usage examples
- **Platform compatibility**: Verified compatibility across Linux, macOS, and Windows environments

## [0.0.2] - 2025-08-24

### Added
- **OpenContext function**: Added `OpenContext(ctx context.Context, paths ...string)` function for context-aware database opening
  - Enables timeout control and cancellation support
  - Provides better resource management and operation control
  - Maintains backward compatibility by making `Open()` call `OpenContext()` internally
- **Comprehensive test coverage**: Added extensive tests for OpenContext functionality
  - Context timeout scenarios
  - Context cancellation handling
  - Concurrent access testing
  - Error handling validation
- **Example documentation**: Added `ExampleOpenContext` demonstrating proper usage with timeouts

### Changed
- **Updated all README files**: Modified all 7 language versions to use OpenContext in examples
  - English (README.md)
  - Japanese (doc/ja/README.md)
  - Russian (doc/ru/README.md)
  - Chinese Simplified (doc/zh-cn/README.md)
  - Korean (doc/ko/README.md)
  - Spanish (doc/es/README.md)
  - French (doc/fr/README.md)
- **Improved database operations**: All examples now demonstrate proper context usage
  - Added timeout configuration in examples
  - Replaced `context.Background()` with reusable context variables
  - Enhanced error handling patterns

### Fixed
- **Linting issues**: Resolved all golangci-lint warnings
  - Fixed context usage in tests to use `t.Context()` where appropriate
  - Adopted Go 1.22+ integer range loops syntax (`for i := range numGoroutines`)
  - Improved error wrapping with `%w` format verb instead of `%v`
  - Ensured proper code formatting with gofmt

### Technical Details
- **Go version compatibility**: Leverages Go 1.24 features as specified in go.mod
- **Test improvements**: Enhanced test reliability and coverage
- **Code quality**: Maintained 79.3% test coverage
- **Documentation consistency**: Ensured all language versions provide equivalent information

## [0.0.1] - 2025-08-23

### Added
- Initial release of filesql library
- Support for CSV, TSV, and LTSV file formats
- Compression support for .gz, .bz2, .xz, .zst files
- SQLite3-based in-memory database engine
- Multi-file and directory loading capabilities
- Cross-platform compatibility (Linux, macOS, Windows)
- Database export functionality via `DumpDatabase`
- Comprehensive test suite
- Multi-language documentation (7 languages)
- Standard database/sql interface implementation

[Unreleased]: https://github.com/nao1215/filesql/compare/v0.4.1...HEAD
[0.4.1]: https://github.com/nao1215/filesql/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/nao1215/filesql/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/nao1215/filesql/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/nao1215/filesql/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/nao1215/filesql/compare/v0.0.4...v0.1.0
[0.0.4]: https://github.com/nao1215/filesql/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/nao1215/filesql/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/nao1215/filesql/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/nao1215/filesql/releases/tag/v0.0.1