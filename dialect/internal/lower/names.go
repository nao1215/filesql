package lower

// The SQL names more than one dialect's rules mention. They are constants so a
// name spelled two ways in two places is a compile error rather than a rule
// that quietly never matches.
const (
	fnNameReplace   = "REPLACE"
	fnNameLpad      = "LPAD"
	fnNameLower     = "LOWER"
	fnNameUpper     = "UPPER"
	fnNameFormat    = "FORMAT"
	fnNameDatePart  = "DATE_PART"
	fnNameDateAdd   = "DATE_ADD"
	fnNameDateSub   = "DATE_SUB"
	fnNameCharLen   = "CHARACTER_LENGTH"
	fnNameRound     = "ROUND"
	fnNamePosition  = "POSITION"
	fnNameMod       = "MOD"
	fnNameExtract   = "EXTRACT"
	fnNameSubstring = "SUBSTRING"
	fnNameSubstr    = "SUBSTR"
	fnNameTrim      = "TRIM"
	fnNameConcat    = "CONCAT"
	fnNameStringAgg = "STRING_AGG"

	typeNameBinary   = "BINARY"
	typeNameDate     = "DATE"
	typeNameDatetime = "DATETIME"
	typeNameNumeric  = "NUMERIC"
	typeNameInteger  = "INTEGER"
	typeNameChar     = "CHAR"
	typeNameReal     = "REAL"
	typeNameBlob     = "BLOB"
	typeNameBoolean  = "BOOLEAN"
	typeNameText     = "TEXT"
	typeNameInterval = "INTERVAL"

	keywordCurrentTime = "CURRENT_TIME"
)

// The names only one dialect's rules mention, spelled once so the linter's
// repeated-string rule and a typo both fail loudly.
const (
	fnNameCharLength    = "CHAR_LENGTH"
	fnNameRpad          = "RPAD"
	typeNameString      = "STRING"
	typeNameTime        = "TIME"
	typeNameTimestamp   = "TIMESTAMP"
	typeNameBigserial   = "BIGSERIAL"
	typeNameSerial      = "SERIAL"
	typeNameSigned      = "SIGNED"
	typeNameUnsigned    = "UNSIGNED"
	typeNameTimestampTZ = "TIMESTAMPTZ"

	keywordCurrentTimestamp = "CURRENT_TIMESTAMP"
	unitYear                = "YEAR"
	keywordLocalTime        = "LOCALTIME"
	keywordLocalTimestamp   = "LOCALTIMESTAMP"
)
