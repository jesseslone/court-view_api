package courtview

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const defaultBaseURL = "https://records.courts.alaska.gov/eaccess/home.page.2"

type Client struct {
	baseURL    string
	httpClient *http.Client
	userAgent  string
}

func NewClient(baseURL string) (*Client, error) {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultBaseURL
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("create cookie jar: %w", err)
	}
	transport := &http.Transport{
		Proxy:              http.ProxyFromEnvironment,
		ForceAttemptHTTP2:  false,
		DisableCompression: true,
		DisableKeepAlives:  true,
	}
	return &Client{
		baseURL: strings.TrimSpace(baseURL),
		httpClient: &http.Client{
			Jar:       jar,
			Transport: transport,
			Timeout:   60 * time.Second,
		},
		userAgent: "courtview-lookup-go/0.1",
	}, nil
}

func (c *Client) SearchByName(
	ctx context.Context,
	req NameSearchRequest,
	includeCases bool,
	maxCases int,
	allPages bool,
	maxPages int,
) (NameSearchResponse, error) {
	if strings.TrimSpace(req.LastName) == "" || strings.TrimSpace(req.FirstName) == "" {
		return NameSearchResponse{}, errors.New("first_name and last_name are required")
	}

	searchPageDoc, searchPageURL, err := c.enterSearchPage(ctx)
	if err != nil {
		return NameSearchResponse{}, err
	}

	nameFormDoc, baseForForm, err := c.ensureNameTab(ctx, searchPageDoc, searchPageURL)
	if err != nil {
		return NameSearchResponse{}, err
	}

	form := nameFormDoc.Find("form").Has("input[name='lastName']").First()
	if form.Length() == 0 {
		return NameSearchResponse{}, errors.New("could not locate name search form")
	}

	values := formDefaults(form)
	values.Set("lastName", strings.TrimSpace(req.LastName))
	values.Set("firstName", strings.TrimSpace(req.FirstName))
	if req.DOBFrom != "" {
		values.Set("dobDateRange:dateInputBegin", strings.TrimSpace(req.DOBFrom))
	}
	if req.DOBTo != "" {
		values.Set("dobDateRange:dateInputEnd", strings.TrimSpace(req.DOBTo))
	} else if req.DOBFrom != "" {
		values.Set("dobDateRange:dateInputEnd", strings.TrimSpace(req.DOBFrom))
	}

	submit := form.Find("input[type='submit'][name]").First()
	if submit.Length() > 0 {
		submitName := firstAttr(submit, "name")
		if submitName != "" {
			submitValue := firstAttr(submit, "value")
			if submitValue == "" {
				submitValue = "Search"
			}
			values.Set(submitName, submitValue)
		}
	}

	action := firstAttr(form, "action")
	if action == "" {
		return NameSearchResponse{}, errors.New("name search form action is empty")
	}

	resultBody, resultURL, _, err := c.postForm(ctx, absURL(baseForForm, action), values, nil)
	if err != nil {
		return NameSearchResponse{}, fmt.Errorf("submit name search: %w", err)
	}

	resultsDoc, err := goquery.NewDocumentFromReader(bytes.NewReader(resultBody))
	if err != nil {
		return NameSearchResponse{}, fmt.Errorf("parse search results html: %w", err)
	}
	results := parseSearchResults(resultsDoc, resultURL)
	results.PageIndex = 1

	pages := []SearchResults{results}
	if allPages {
		collectedPages, err := c.collectResultsPages(ctx, resultsDoc, resultURL, results, maxPages)
		if err != nil {
			return NameSearchResponse{}, err
		}
		pages = collectedPages
	}
	aggregate := aggregateResults(pages)

	resp := NameSearchResponse{
		Request:      req,
		Results:      aggregate,
		ResultsPages: pages,
	}
	if !includeCases || len(aggregate.Rows) == 0 {
		return resp, nil
	}

	resp.Cases = c.collectCases(ctx, aggregate.Rows, maxCases)
	return resp, nil
}

func (c *Client) SearchByCaseNumber(
	ctx context.Context,
	rawCaseNumber string,
	includeCases bool,
	maxCases int,
	allPages bool,
	maxPages int,
) (CaseSearchResponse, error) {
	rawCaseNumber = strings.TrimSpace(rawCaseNumber)
	if rawCaseNumber == "" {
		return CaseSearchResponse{}, errors.New("case_number is required")
	}

	normalized, err := NormalizeCaseNumber(rawCaseNumber)
	if err != nil {
		return CaseSearchResponse{}, fmt.Errorf("normalize case number: %w", err)
	}

	searchPageDoc, searchPageURL, err := c.enterSearchPage(ctx)
	if err != nil {
		return CaseSearchResponse{}, err
	}

	caseFormDoc, baseForForm, err := c.ensureCaseNumberTab(ctx, searchPageDoc, searchPageURL)
	if err != nil {
		return CaseSearchResponse{}, err
	}

	form := caseFormDoc.Find("form").Has("input[name='caseDscr']").First()
	if form.Length() == 0 {
		return CaseSearchResponse{}, errors.New("could not locate case-number search form")
	}

	values := formDefaults(form)
	values.Set("caseDscr", normalized)

	submit := form.Find("input[type='submit'][name]").First()
	if submit.Length() > 0 {
		submitName := firstAttr(submit, "name")
		if submitName != "" {
			submitValue := firstAttr(submit, "value")
			if submitValue == "" {
				submitValue = "Search"
			}
			values.Set(submitName, submitValue)
		}
	}

	action := firstAttr(form, "action")
	if action == "" {
		return CaseSearchResponse{}, errors.New("case-number form action is empty")
	}

	resultBody, resultURL, _, err := c.postForm(ctx, absURL(baseForForm, action), values, nil)
	if err != nil {
		return CaseSearchResponse{}, fmt.Errorf("submit case-number search: %w", err)
	}

	resultsDoc, err := goquery.NewDocumentFromReader(bytes.NewReader(resultBody))
	if err != nil {
		return CaseSearchResponse{}, fmt.Errorf("parse case-number results html: %w", err)
	}
	results := parseSearchResults(resultsDoc, resultURL)
	results.PageIndex = 1

	pages := []SearchResults{results}
	if allPages {
		collectedPages, err := c.collectResultsPages(ctx, resultsDoc, resultURL, results, maxPages)
		if err != nil {
			return CaseSearchResponse{}, err
		}
		pages = collectedPages
	}
	aggregate := aggregateResults(pages)

	resp := CaseSearchResponse{
		Request: CaseSearchRequest{
			CaseNumber:           rawCaseNumber,
			NormalizedCaseNumber: normalized,
		},
		Results:      aggregate,
		ResultsPages: pages,
	}

	if includeCases && len(aggregate.Rows) > 0 {
		resp.Cases = c.collectCases(ctx, aggregate.Rows, maxCases)
	}
	return resp, nil
}

func (c *Client) enterSearchPage(ctx context.Context) (*goquery.Document, string, error) {
	homeBody, homeURL, _, err := c.get(ctx, c.baseURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("load home page: %w", err)
	}

	homeDoc, err := goquery.NewDocumentFromReader(bytes.NewReader(homeBody))
	if err != nil {
		return nil, "", fmt.Errorf("parse home page: %w", err)
	}

	searchAnchor := homeDoc.Find("a").FilterFunction(func(_ int, s *goquery.Selection) bool {
		return cleanText(s.Text()) == "Search Cases"
	}).First()
	if searchAnchor.Length() == 0 {
		return nil, "", errors.New("could not locate Search Cases button")
	}

	onclick := firstAttr(searchAnchor, "onclick")
	formID, action, submitField, ok := parseWicketSubmit(onclick)
	if !ok {
		return nil, "", errors.New("could not parse home Search Cases action")
	}

	form := homeDoc.Find("form#" + formID)
	if form.Length() == 0 {
		return nil, "", fmt.Errorf("home form %q not found", formID)
	}

	values := formDefaults(form)
	values.Set(submitField, "1")

	headers := map[string]string{
		"Accept":                  "text/xml",
		"Wicket-Ajax":             "true",
		"Wicket-FocusedElementId": firstAttr(searchAnchor, "id"),
	}
	body, finalURL, respHeaders, err := c.postForm(ctx, absURL(homeURL, action), values, headers)
	if err != nil {
		return nil, "", fmt.Errorf("submit Search Cases from home: %w", err)
	}

	if ajaxLocation := strings.TrimSpace(respHeaders.Get("Ajax-Location")); ajaxLocation != "" {
		searchURL := absURL(finalURL, ajaxLocation)
		searchBody, searchFinalURL, _, err := c.get(ctx, searchURL, nil)
		if err != nil {
			return nil, "", fmt.Errorf("load search page from Ajax-Location: %w", err)
		}
		searchDoc, err := goquery.NewDocumentFromReader(bytes.NewReader(searchBody))
		if err != nil {
			return nil, "", fmt.Errorf("parse search page html: %w", err)
		}
		return searchDoc, searchFinalURL, nil
	}

	if strings.Contains(finalURL, "search.page") {
		doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
		if err != nil {
			return nil, "", fmt.Errorf("parse search page html: %w", err)
		}
		return doc, finalURL, nil
	}

	fallbackURL := absURL(homeURL, "search.page")
	fallbackBody, fallbackFinalURL, _, err := c.get(ctx, fallbackURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("fallback search.page load: %w", err)
	}
	fallbackDoc, err := goquery.NewDocumentFromReader(bytes.NewReader(fallbackBody))
	if err != nil {
		return nil, "", fmt.Errorf("parse fallback search page: %w", err)
	}
	return fallbackDoc, fallbackFinalURL, nil
}

func (c *Client) ensureNameTab(ctx context.Context, doc *goquery.Document, pageURL string) (*goquery.Document, string, error) {
	if doc.Find("input[name='lastName']").Length() > 0 {
		return doc, pageURL, nil
	}

	anchor := findNameTabAnchor(doc)
	if anchor == nil || anchor.Length() == 0 {
		return nil, "", errors.New("name tab not found")
	}

	href := firstAttr(anchor, "href")
	if href != "" && href != "#" {
		body, nextURL, _, err := c.get(ctx, absURL(pageURL, href), nil)
		if err == nil {
			nextDoc, parseErr := goquery.NewDocumentFromReader(bytes.NewReader(body))
			if parseErr == nil && nextDoc.Find("input[name='lastName']").Length() > 0 {
				return nextDoc, nextURL, nil
			}
		}
	}

	onclick := firstAttr(anchor, "onclick")
	ajaxURL, ok := parseWicketAjaxGet(onclick)
	if !ok {
		return nil, "", errors.New("failed to parse name tab ajax action")
	}
	ajaxBody, _, _, err := c.get(ctx, absURL(pageURL, ajaxURL), map[string]string{"Accept": "text/xml"})
	if err != nil {
		return nil, "", fmt.Errorf("load name tab ajax response: %w", err)
	}

	xmlText := string(ajaxBody)
	componentHTML, ok := parseAjaxComponent(xmlText, "id5a")
	if !ok {
		return nil, "", errors.New("could not parse name tab component from ajax response")
	}

	nameDoc, err := goquery.NewDocumentFromReader(strings.NewReader(componentHTML))
	if err != nil {
		return nil, "", fmt.Errorf("parse name tab component html: %w", err)
	}
	return nameDoc, pageURL, nil
}

func (c *Client) ensureCaseNumberTab(ctx context.Context, doc *goquery.Document, pageURL string) (*goquery.Document, string, error) {
	if doc.Find("input[name='caseDscr']").Length() > 0 {
		return doc, pageURL, nil
	}

	var anchor *goquery.Selection
	doc.Find("a").EachWithBreak(func(_ int, a *goquery.Selection) bool {
		if cleanText(a.Text()) == "Case Number" {
			anchor = a
			return false
		}
		return true
	})
	if anchor == nil || anchor.Length() == 0 {
		return nil, "", errors.New("case number tab not found")
	}

	href := firstAttr(anchor, "href")
	if href != "" && href != "#" {
		body, nextURL, _, err := c.get(ctx, absURL(pageURL, href), nil)
		if err == nil {
			nextDoc, parseErr := goquery.NewDocumentFromReader(bytes.NewReader(body))
			if parseErr == nil && nextDoc.Find("input[name='caseDscr']").Length() > 0 {
				return nextDoc, nextURL, nil
			}
		}
	}

	onclick := firstAttr(anchor, "onclick")
	ajaxURL, ok := parseWicketAjaxGet(onclick)
	if !ok {
		return nil, "", errors.New("failed to parse case-number tab ajax action")
	}
	ajaxBody, _, _, err := c.get(ctx, absURL(pageURL, ajaxURL), map[string]string{"Accept": "text/xml"})
	if err != nil {
		return nil, "", fmt.Errorf("load case-number tab ajax response: %w", err)
	}

	xmlText := string(ajaxBody)
	componentHTML, ok := parseAjaxComponent(xmlText, "id5a")
	if !ok {
		return nil, "", errors.New("could not parse case-number tab component from ajax response")
	}

	caseDoc, err := goquery.NewDocumentFromReader(strings.NewReader(componentHTML))
	if err != nil {
		return nil, "", fmt.Errorf("parse case-number tab component html: %w", err)
	}
	return caseDoc, pageURL, nil
}

func (c *Client) FetchCase(ctx context.Context, caseURL string) CaseDetails {
	body, finalURL, _, err := c.get(ctx, caseURL, nil)
	if err != nil {
		return CaseDetails{CaseURL: caseURL, Error: err.Error()}
	}

	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return CaseDetails{CaseURL: caseURL, Error: fmt.Sprintf("parse case page html: %v", err)}
	}

	current := pageSnapshot(doc, finalURL)
	caseNumber := ""
	if v, ok := current.LabelValues["Case Number"]; ok {
		caseNumber = v
	}

	tabs := map[string]PageSnapshot{}
	selectedLabel := "Current"
	doc.Find(".tab-row li.selected a span").Each(func(_ int, s *goquery.Selection) {
		if t := cleanText(s.Text()); t != "" {
			selectedLabel = t
		}
	})
	tabs[selectedLabel] = current

	doc.Find(".tab-row li a").Each(func(_ int, a *goquery.Selection) {
		label := cleanText(a.Find("span").Text())
		if label == "" {
			label = cleanText(a.Text())
		}
		if label == "" {
			return
		}
		if _, exists := tabs[label]; exists {
			return
		}
		href := firstAttr(a, "href")
		if href == "" || href == "#" {
			return
		}
		fullURL := absURL(finalURL, href)
		tabBody, tabURL, _, tabErr := c.get(ctx, fullURL, nil)
		if tabErr != nil {
			tabs[label] = PageSnapshot{URL: fullURL, Title: "", H1: "", Tabs: nil, LabelValues: map[string]string{}, Tables: nil, MainTextExcerpt: "tab fetch failed: " + tabErr.Error()}
			return
		}
		tabDoc, parseErr := goquery.NewDocumentFromReader(bytes.NewReader(tabBody))
		if parseErr != nil {
			tabs[label] = PageSnapshot{URL: fullURL, MainTextExcerpt: "tab parse failed: " + parseErr.Error()}
			return
		}
		tabs[label] = pageSnapshot(tabDoc, tabURL)
	})

	return CaseDetails{
		CaseNumber: caseNumber,
		CaseURL:    finalURL,
		Current:    current,
		Tabs:       tabs,
	}
}

func (c *Client) get(ctx context.Context, rawURL string, headers map[string]string) ([]byte, string, http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, "", nil, err
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Connection", "close")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, resp.Request.URL.String(), resp.Header, fmt.Errorf("http %d for %s", resp.StatusCode, rawURL)
	}
	return body, resp.Request.URL.String(), resp.Header, nil
}

func (c *Client) postForm(ctx context.Context, rawURL string, values url.Values, headers map[string]string) ([]byte, string, http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, strings.NewReader(values.Encode()))
	if err != nil {
		return nil, "", nil, err
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Connection", "close")
	for k, v := range headers {
		if strings.TrimSpace(v) != "" {
			req.Header.Set(k, v)
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, resp.Request.URL.String(), resp.Header, fmt.Errorf("http %d for %s", resp.StatusCode, rawURL)
	}
	return body, resp.Request.URL.String(), resp.Header, nil
}

func (c *Client) collectCases(ctx context.Context, rows []SearchResultRow, maxCases int) []CaseDetails {
	if maxCases <= 0 {
		maxCases = 25
	}

	seen := make(map[string]struct{})
	cases := make([]CaseDetails, 0, maxCases)
	for _, row := range rows {
		if len(cases) >= maxCases {
			break
		}
		if strings.TrimSpace(row.CaseURL) == "" {
			continue
		}
		if _, ok := seen[row.CaseURL]; ok {
			continue
		}
		seen[row.CaseURL] = struct{}{}

		caseDetails := c.FetchCase(ctx, row.CaseURL)
		if caseDetails.CaseNumber == "" {
			caseDetails.CaseNumber = row.CaseNumber
		}
		cases = append(cases, caseDetails)
	}
	return cases
}

func (c *Client) collectResultsPages(
	ctx context.Context,
	firstDoc *goquery.Document,
	firstURL string,
	firstPage SearchResults,
	maxPages int,
) ([]SearchResults, error) {
	if maxPages <= 0 {
		maxPages = 20
	}
	pages := []SearchResults{firstPage}
	seen := map[string]struct{}{
		resultsSignature(firstPage): {},
	}

	currentDoc := firstDoc
	currentURL := firstURL

	for pageIndex := 2; pageIndex <= maxPages; pageIndex++ {
		nextURL, ok := findNextResultsPageURL(currentDoc, currentURL)
		if !ok || strings.TrimSpace(nextURL) == "" {
			break
		}

		body, finalURL, _, err := c.get(ctx, nextURL, nil)
		if err != nil {
			return pages, fmt.Errorf("fetch results page %d: %w", pageIndex, err)
		}

		nextDoc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
		if err != nil {
			return pages, fmt.Errorf("parse results page %d html: %w", pageIndex, err)
		}

		nextPage := parseSearchResults(nextDoc, finalURL)
		nextPage.PageIndex = pageIndex
		sig := resultsSignature(nextPage)
		if _, exists := seen[sig]; exists {
			break
		}
		seen[sig] = struct{}{}
		pages = append(pages, nextPage)

		currentDoc = nextDoc
		currentURL = finalURL
		if nextPage.NoRecordsFound {
			break
		}
	}

	return pages, nil
}

func aggregateResults(pages []SearchResults) SearchResults {
	if len(pages) == 0 {
		return SearchResults{}
	}
	base := pages[0]
	combined := make([]SearchResultRow, 0)
	seenRows := make(map[string]struct{})

	for _, page := range pages {
		for _, row := range page.Rows {
			key := rowContentSignature(row)
			if _, ok := seenRows[key]; ok {
				continue
			}
			seenRows[key] = struct{}{}
			combined = append(combined, row)
		}
	}

	base.Rows = combined
	base.NoRecordsFound = len(combined) == 0 && pages[0].NoRecordsFound
	base.PagerSummary = pages[len(pages)-1].PagerSummary
	return base
}

func rowContentSignature(row SearchResultRow) string {
	keys := make([]string, 0, len(row.Values))
	for k := range row.Values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys)+2)
	parts = append(parts, row.CaseURL, row.CaseNumber)
	for _, k := range keys {
		parts = append(parts, k+"="+row.Values[k])
	}
	return strings.Join(parts, "|")
}
