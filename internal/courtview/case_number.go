package courtview

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var reNonAlnum = regexp.MustCompile(`[^A-Z0-9]+`)

// NormalizeCaseNumber standardizes malformed Alaska case numbers into:
// <regionDigit><courtLetters>-<2digitYear>-<5digitSequence><caseType>
//
// Examples:
// - 3ANS11123CR   -> 3AN-11-00123CR
// - 3KE-25-184CR  -> 3KE-25-00184CR
// - 3AN1100123    -> 3AN-11-00123CR
func NormalizeCaseNumber(raw string) (string, error) {
	cleaned := strings.ToUpper(strings.TrimSpace(raw))
	cleaned = reNonAlnum.ReplaceAllString(cleaned, "")
	if cleaned == "" {
		return "", errors.New("empty case number")
	}
	if len(cleaned) < 3 {
		return "", errors.New("case number too short")
	}

	regionDigit := cleaned[0]
	if regionDigit < '0' || regionDigit > '9' {
		return "", errors.New("missing or invalid region digit")
	}

	courtLetters := cleaned[1:3]
	if !isLetters(courtLetters) {
		return "", errors.New("court abbreviation must be letters")
	}

	remaining := cleaned[3:]
	if len(remaining) > 0 && isLetters(remaining[:1]) {
		remaining = remaining[1:]
	}

	caseType := ""
	if len(remaining) >= 2 && isLetters(remaining[len(remaining)-2:]) {
		caseType = remaining[len(remaining)-2:]
		remaining = remaining[:len(remaining)-2]
	}
	if caseType == "" {
		caseType = "CR"
	}

	if len(remaining) < 2 {
		return "", errors.New("need at least two digits for year")
	}
	if !isDigits(remaining) {
		return "", errors.New("year/sequence segment must be numeric")
	}

	year := remaining[:2]
	sequence := remaining[2:]
	if len(sequence) > 5 {
		return "", errors.New("case sequence must be at most 5 digits")
	}
	sequence = strings.Repeat("0", 5-len(sequence)) + sequence

	return fmt.Sprintf("%c%s-%s-%s%s", regionDigit, courtLetters, year, sequence, caseType), nil
}

func isLetters(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 'A' || s[i] > 'Z' {
			return false
		}
	}
	return true
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
