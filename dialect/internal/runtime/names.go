package runtime

// The names the helpers share: type names a cast may target, the date parts the
// datetime helpers take, and the function names more than one helper spells.
// They are constants so a typo is a compile error rather than a value that
// silently matches nothing.
const (
	typeDate      = "DATE"
	typeDatetime  = "DATETIME"
	typeTime      = "TIME"
	typeTimestamp = "TIMESTAMP"

	typeNameString = "STRING"
	typeNameBinary = "BINARY"

	unitMillisecond        = "millisecond"
	unitMicrosecond        = "microsecond"
	unitMicrosecondsPlural = "microseconds"

	patTZH = "TZH"
	patTZM = "TZM"
)

// The keywords a helper compares against, spelled once.
const (
	kwInterval      = "INTERVAL"
	fnNameFormat    = "FORMAT"
	fnNameMod       = "MOD"
	fnNameTrunc     = "TRUNC"
	fnNameRound     = "ROUND"
	fnNameReplace   = "REPLACE"
	fnNameLpad      = "LPAD"
	fnNameRpad      = "RPAD"
	fnNameUpper     = "UPPER"
	fnNameLower     = "LOWER"
	fnNameCast      = "CAST"
	fnNameExtract   = "EXTRACT"
	fnNameTrim      = "TRIM"
	fnNameSubstring = "SUBSTRING"
	fnNameSubstr    = "SUBSTR"
	fnNameStringAgg = "STRING_AGG"
	fnNameCharLen   = "CHAR_LENGTH"
	fnNameRepeat    = "REPEAT"
	fnNameSpace     = "SPACE"
	kwAll           = "ALL"
	kwUnion         = "UNION"
	kwIntersect     = "INTERSECT"
	kwExcept        = "EXCEPT"
	kwOffset        = "OFFSET"
	kwHaving        = "HAVING"
	kwLimit         = "LIMIT"
	kwWhere         = "WHERE"
	sqliteJSONArray = "json_group_array"

	// fnNamePostgresDateAdd is the helper that adds a number of days to a date.
	fnNamePostgresDateAdd = "postgresql_date_add"
)
