package cloudutil

import (
	"strings"
	"unicode"
)

// ConvertToGlueColumnFormat takes a string and runs through various regex
// and string replacement statements to convert it to a format compatible
// with AWS Glue and Athena column names.
// Following guidance from AWS provided here ('Column Names' section):
// https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/run-athena-sql.html
// It returns a string containing the column name in proper column name format and length.
func ConvertToGlueColumnFormat(columnName string) string {
	var sb strings.Builder
	var prev rune
	for i, r := range columnName {
		if unicode.IsUpper(r) && prev != '_' && i != 0 {
			sb.WriteRune('_')
		}
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) {
			if prev != '_' && i != 0 && i != (len(columnName)-1) {
				sb.WriteRune('_')
			}
			prev = '_'
			continue
		}
		if r == '_' {
			if prev == '_' || i == 0 || i == len(columnName)-1 {
				prev = '_'
				continue
			}
		}
		sb.WriteRune(unicode.ToLower(r))
		prev = r
	}

	final := sb.String()
	if prev == '_' { // string any trailing '_'
		final = final[:len(final)-1]
	}
	// Longer column name than expected - remove _ left to right
	allowedColLen := 128
	underscoreToRemove := len(final) - allowedColLen
	if underscoreToRemove > 0 {
		final = strings.Replace(final, "_", "", underscoreToRemove)
	}

	// If removing all of the underscores still didn't
	// make the column name < 128 characters, trim it!
	if len(final) > allowedColLen {
		final = final[:allowedColLen]
	}

	return final
}
