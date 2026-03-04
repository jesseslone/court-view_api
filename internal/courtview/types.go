package courtview

type NameSearchRequest struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	DOBFrom   string `json:"dob_from,omitempty"`
	DOBTo     string `json:"dob_to,omitempty"`
}

type SearchResultRow struct {
	RowIndex   int               `json:"row_index"`
	Values     map[string]string `json:"values"`
	CaseNumber string            `json:"case_number,omitempty"`
	CaseURL    string            `json:"case_url,omitempty"`
}

type SearchResults struct {
	PageIndex      int               `json:"page_index,omitempty"`
	Criteria       map[string]string `json:"criteria"`
	Headers        []string          `json:"headers"`
	Rows           []SearchResultRow `json:"rows"`
	NoRecordsFound bool              `json:"no_records_found"`
	PagerSummary   string            `json:"pager_summary,omitempty"`
	SourceURL      string            `json:"source_url"`
}

type TableSnapshot struct {
	Index   int        `json:"index"`
	Headers []string   `json:"headers"`
	Rows    [][]string `json:"rows"`
}

type PageSnapshot struct {
	URL             string            `json:"url"`
	Title           string            `json:"title"`
	H1              string            `json:"h1,omitempty"`
	Tabs            []string          `json:"tabs"`
	LabelValues     map[string]string `json:"label_values"`
	Tables          []TableSnapshot   `json:"tables"`
	MainTextExcerpt string            `json:"main_text_excerpt"`
}

type CaseDetails struct {
	CaseNumber string                  `json:"case_number,omitempty"`
	CaseURL    string                  `json:"case_url"`
	Current    PageSnapshot            `json:"current"`
	Tabs       map[string]PageSnapshot `json:"tabs"`
	Error      string                  `json:"error,omitempty"`
}

type NameSearchResponse struct {
	Request      NameSearchRequest `json:"request"`
	Results      SearchResults     `json:"results"`
	ResultsPages []SearchResults   `json:"results_pages,omitempty"`
	Cases        []CaseDetails     `json:"cases,omitempty"`
}

type CaseSearchRequest struct {
	CaseNumber           string `json:"case_number"`
	NormalizedCaseNumber string `json:"normalized_case_number"`
}

type CaseSearchResponse struct {
	Request        CaseSearchRequest `json:"request"`
	Results        SearchResults     `json:"results"`
	ResultsPages   []SearchResults   `json:"results_pages,omitempty"`
	Cases          []CaseDetails     `json:"cases,omitempty"`
	RelatedParties []PartyRecord     `json:"related_parties,omitempty"`
}
