package courtview

import (
	"strings"
)

type PartyRecord struct {
	FullName       string `json:"full_name"`
	Role           string `json:"role,omitempty"`
	FirstName      string `json:"first_name,omitempty"`
	LastName       string `json:"last_name,omitempty"`
	DOB            string `json:"dob,omitempty"`
	NormalizedName string `json:"normalized_name,omitempty"`
}

func ExtractParties(rows []SearchResultRow) []PartyRecord {
	seen := make(map[string]struct{})
	parties := make([]PartyRecord, 0)

	for _, row := range rows {
		fullName := firstNonEmpty(row.Values,
			"Party/Company",
			"Party",
			"Defendant",
		)
		fullName = cleanText(fullName)
		if fullName == "" {
			continue
		}

		role := cleanText(firstNonEmpty(row.Values,
			"Party Type",
			"Role",
		))
		dob := cleanText(firstNonEmpty(row.Values,
			"Date of Birth",
			"DOB",
		))

		firstName, lastName := splitPartyName(fullName)
		norm := normalizePersonName(firstName, lastName, fullName)

		party := PartyRecord{
			FullName:       fullName,
			Role:           role,
			FirstName:      firstName,
			LastName:       lastName,
			DOB:            dob,
			NormalizedName: norm,
		}

		key := strings.ToLower(strings.Join([]string{party.FullName, party.Role, party.DOB, party.NormalizedName}, "|"))
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		parties = append(parties, party)
	}

	return parties
}

func ExtractDefendantParties(rows []SearchResultRow) []PartyRecord {
	all := ExtractParties(rows)
	defendants := make([]PartyRecord, 0)
	for _, party := range all {
		role := strings.ToLower(strings.TrimSpace(party.Role))
		if strings.Contains(role, "defendant") {
			defendants = append(defendants, party)
		}
	}
	return defendants
}

func splitPartyName(fullName string) (firstName, lastName string) {
	n := cleanText(fullName)
	if n == "" {
		return "", ""
	}

	if strings.Contains(n, ",") {
		parts := strings.SplitN(n, ",", 2)
		lastName = cleanText(parts[0])
		rest := cleanText(parts[1])
		fields := strings.Fields(rest)
		if len(fields) > 0 {
			firstName = fields[0]
		}
		return firstName, lastName
	}

	fields := strings.Fields(n)
	if len(fields) == 1 {
		return fields[0], ""
	}
	firstName = fields[0]
	lastName = fields[len(fields)-1]
	return firstName, lastName
}

func normalizePersonName(firstName, lastName, fullName string) string {
	if cleanText(firstName) != "" && cleanText(lastName) != "" {
		return strings.ToLower(cleanText(lastName + "," + firstName))
	}
	return strings.ToLower(cleanText(fullName))
}

func firstNonEmpty(values map[string]string, keys ...string) string {
	for _, k := range keys {
		if v, ok := values[k]; ok {
			if cleanText(v) != "" {
				return v
			}
		}
	}
	return ""
}
