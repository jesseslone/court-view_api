package courtview

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

var (
	reWicketSubmit  = regexp.MustCompile(`wicketSubmitFormById\('([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'`)
	reWicketAjaxGet = regexp.MustCompile(`wicketAjaxGet\('([^']+)'`)
	rePagerSummary  = regexp.MustCompile(`(?i)Showing\s+\d+\s+to\s+\d+\s+of\s+\d+`)
)

func cleanText(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func absURL(baseURL, ref string) string {
	if ref == "" {
		return ""
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return ref
	}
	u, err := url.Parse(ref)
	if err != nil {
		return ref
	}
	return base.ResolveReference(u).String()
}

func firstAttr(sel *goquery.Selection, name string) string {
	v, _ := sel.Attr(name)
	return v
}

func formDefaults(form *goquery.Selection) url.Values {
	values := make(url.Values)

	form.Find("input").Each(func(_ int, input *goquery.Selection) {
		name := firstAttr(input, "name")
		if name == "" {
			return
		}
		typeAttr := strings.ToLower(firstAttr(input, "type"))
		switch typeAttr {
		case "submit", "button", "image", "file", "reset":
			return
		case "checkbox", "radio":
			if _, ok := input.Attr("checked"); !ok {
				return
			}
			value := firstAttr(input, "value")
			if value == "" {
				value = "on"
			}
			values.Add(name, value)
		default:
			values.Add(name, firstAttr(input, "value"))
		}
	})

	form.Find("select").Each(func(_ int, selectEl *goquery.Selection) {
		name := firstAttr(selectEl, "name")
		if name == "" {
			return
		}

		var selected []string
		selectEl.Find("option").Each(func(_ int, option *goquery.Selection) {
			if _, ok := option.Attr("selected"); !ok {
				return
			}
			selected = append(selected, firstAttr(option, "value"))
		})

		if len(selected) == 0 {
			if first := selectEl.Find("option").First(); first.Length() > 0 {
				selected = append(selected, firstAttr(first, "value"))
			}
		}

		for _, v := range selected {
			values.Add(name, v)
		}
	})

	form.Find("textarea").Each(func(_ int, ta *goquery.Selection) {
		name := firstAttr(ta, "name")
		if name == "" {
			return
		}
		values.Add(name, ta.Text())
	})

	return values
}

func parseWicketSubmit(onclick string) (formID, action, submitField string, ok bool) {
	m := reWicketSubmit.FindStringSubmatch(onclick)
	if len(m) != 4 {
		return "", "", "", false
	}
	return m[1], m[2], m[3], true
}

func parseWicketAjaxGet(onclick string) (action string, ok bool) {
	m := reWicketAjaxGet.FindStringSubmatch(onclick)
	if len(m) != 2 {
		return "", false
	}
	return m[1], true
}

func parseAjaxComponent(xmlBody string, componentID string) (string, bool) {
	pattern := `(?s)<component id="` + regexp.QuoteMeta(componentID) + `"[^>]*><!\[CDATA\[(.*?)\]\]></component>`
	re := regexp.MustCompile(pattern)
	m := re.FindStringSubmatch(xmlBody)
	if len(m) != 2 {
		return "", false
	}
	return m[1], true
}

func findNameTabAnchor(doc *goquery.Document) *goquery.Selection {
	var found *goquery.Selection
	doc.Find("a").EachWithBreak(func(_ int, a *goquery.Selection) bool {
		text := strings.ToLower(cleanText(a.Text()))
		if text == "name" || strings.Contains(text, " name") || strings.HasPrefix(text, "name ") {
			found = a
			return false
		}
		return true
	})
	return found
}

func parseAnyAjaxComponent(xmlBody string) []string {
	re := regexp.MustCompile(`(?s)<component id="[^"]+"[^>]*><!\[CDATA\[(.*?)\]\]></component>`)
	matches := re.FindAllStringSubmatch(xmlBody, -1)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		if len(m) == 2 {
			out = append(out, m[1])
		}
	}
	return out
}

func pageSnapshot(doc *goquery.Document, pageURL string) PageSnapshot {
	title := cleanText(doc.Find("title").First().Text())
	h1 := cleanText(doc.Find("#mainContent h1").First().Text())
	if h1 == "" {
		h1 = cleanText(doc.Find("h1").First().Text())
	}

	labelValues := make(map[string]string)
	doc.Find("#mainContent td.label").Each(func(_ int, label *goquery.Selection) {
		k := cleanText(label.Text())
		if k == "" {
			return
		}
		v := cleanText(label.Next().Text())
		if v != "" {
			labelValues[k] = v
		}
	})

	tabs := make([]string, 0)
	doc.Find(".tab-row li a span").Each(func(_ int, s *goquery.Selection) {
		t := cleanText(s.Text())
		if t != "" {
			tabs = append(tabs, t)
		}
	})

	tables := make([]TableSnapshot, 0)
	doc.Find("#mainContent table").Each(func(i int, table *goquery.Selection) {
		headers := make([]string, 0)
		table.Find("thead th").Each(func(_ int, th *goquery.Selection) {
			headers = append(headers, cleanText(th.Text()))
		})
		if len(headers) == 0 {
			table.Find("tr").First().Find("th").Each(func(_ int, th *goquery.Selection) {
				headers = append(headers, cleanText(th.Text()))
			})
		}

		rows := make([][]string, 0)
		table.Find("tbody tr").Each(func(_ int, tr *goquery.Selection) {
			row := make([]string, 0)
			tr.Find("td").Each(func(_ int, td *goquery.Selection) {
				row = append(row, cleanText(td.Text()))
			})
			if len(row) > 0 {
				rows = append(rows, row)
			}
		})
		if len(rows) == 0 {
			table.Find("tr").Each(func(_ int, tr *goquery.Selection) {
				cells := tr.Find("td")
				if cells.Length() == 0 {
					return
				}
				row := make([]string, 0)
				cells.Each(func(_ int, td *goquery.Selection) {
					row = append(row, cleanText(td.Text()))
				})
				rows = append(rows, row)
			})
		}

		tables = append(tables, TableSnapshot{Index: i, Headers: headers, Rows: rows})
	})

	mainText := cleanText(doc.Find("#mainContent").Text())
	const excerptLimit = 4500
	if len(mainText) > excerptLimit {
		mainText = mainText[:excerptLimit]
	}

	return PageSnapshot{
		URL:             pageURL,
		Title:           title,
		H1:              h1,
		Tabs:            tabs,
		LabelValues:     labelValues,
		Tables:          tables,
		MainTextExcerpt: mainText,
	}
}

func parseSearchResults(doc *goquery.Document, sourceURL string) SearchResults {
	criteria := make(map[string]string)
	doc.Find("#searchCriteriaContainer tr").Each(func(_ int, tr *goquery.Selection) {
		cells := tr.Find("td")
		if cells.Length() < 2 {
			return
		}
		k := cleanText(cells.Eq(0).Text())
		v := cleanText(cells.Eq(1).Text())
		if k != "" {
			criteria[k] = v
		}
	})

	noRecords := doc.Find("#srchResultNoticeNomatch").Length() > 0

	grid := doc.Find("#grid")
	if grid.Length() == 0 {
		doc.Find("table").EachWithBreak(func(_ int, t *goquery.Selection) bool {
			headers := cleanText(t.Find("th").First().Text())
			if strings.Contains(strings.ToLower(headers), "case") {
				grid = t
				return false
			}
			return true
		})
	}

	headers := make([]string, 0)
	grid.Find("thead th").Each(func(_ int, th *goquery.Selection) {
		headers = append(headers, cleanText(th.Text()))
	})

	rows := make([]SearchResultRow, 0)
	grid.Find("tbody tr").Each(func(i int, tr *goquery.Selection) {
		cells := tr.Find("td")
		if cells.Length() == 0 {
			return
		}

		values := make(map[string]string)
		cells.Each(func(ci int, td *goquery.Selection) {
			key := ""
			if ci < len(headers) {
				key = headers[ci]
			}
			if key == "" {
				key = "col_" + strings.TrimSpace(strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(td.Text()), " ", "_"), "-", "_"), "/", "_")))
				if key == "col_" {
					key = "col"
				}
			}
			values[key] = cleanText(td.Text())
		})

		link := tr.Find("a").First()
		caseURL := absURL(sourceURL, firstAttr(link, "href"))
		caseNumber := values["Case Number"]
		if caseNumber == "" {
			caseNumber = cleanText(link.Text())
		}

		rows = append(rows, SearchResultRow{
			RowIndex:   i + 1,
			Values:     values,
			CaseNumber: caseNumber,
			CaseURL:    caseURL,
		})
	})

	return SearchResults{
		Criteria:       criteria,
		Headers:        headers,
		Rows:           rows,
		NoRecordsFound: noRecords,
		PagerSummary:   pagerSummaryText(doc),
		SourceURL:      sourceURL,
	}
}

func pagerSummaryText(doc *goquery.Document) string {
	bodyText := cleanText(doc.Find("body").Text())
	if bodyText == "" {
		return ""
	}
	match := rePagerSummary.FindString(bodyText)
	return cleanText(match)
}

func resultsSignature(results SearchResults) string {
	if results.PagerSummary != "" {
		return fmt.Sprintf("%s|%d", results.PagerSummary, len(results.Rows))
	}
	parts := make([]string, 0, 6)
	for i := 0; i < len(results.Rows) && i < 5; i++ {
		row := results.Rows[i]
		key := row.CaseURL
		if key == "" {
			key = row.CaseNumber
		}
		if key == "" {
			key = fmt.Sprintf("row-%d-%d", i, len(row.Values))
		}
		parts = append(parts, key)
	}
	parts = append(parts, fmt.Sprintf("count-%d", len(results.Rows)))
	return strings.Join(parts, "|")
}

func findNextResultsPageURL(doc *goquery.Document, currentURL string) (string, bool) {
	selectors := []string{
		"#grid .k-pager-nav.k-pager-next a",
		"#grid .k-pager-next a",
		".k-pager-wrap .k-pager-nav.k-pager-next a",
		".k-pager-wrap .k-pager-next a",
		".k-pager-wrap a",
	}
	for _, selector := range selectors {
		var nextURL string
		doc.Find(selector).EachWithBreak(func(_ int, a *goquery.Selection) bool {
			if !isLikelyNextPagerControl(a) || isDisabledPagerControl(a) {
				return true
			}

			href := strings.TrimSpace(firstAttr(a, "href"))
			if href != "" && href != "#" && !strings.HasPrefix(strings.ToLower(href), "javascript:") {
				nextURL = absURL(currentURL, href)
				return false
			}
			if onclick := firstAttr(a, "onclick"); onclick != "" {
				if ajaxURL, ok := parseWicketAjaxGet(onclick); ok {
					nextURL = absURL(currentURL, ajaxURL)
					return false
				}
			}
			return true
		})
		if nextURL != "" {
			return nextURL, true
		}
	}
	return "", false
}

func isLikelyNextPagerControl(sel *goquery.Selection) bool {
	class := strings.ToLower(firstAttr(sel, "class"))
	title := strings.ToLower(firstAttr(sel, "title"))
	ariaLabel := strings.ToLower(firstAttr(sel, "aria-label"))
	text := strings.ToLower(cleanText(sel.Text()))

	if strings.Contains(class, "k-pager-next") {
		return true
	}
	if strings.Contains(title, "next") || strings.Contains(ariaLabel, "next") {
		return true
	}
	return text == ">" || text == "next" || text == "›"
}

func isDisabledPagerControl(sel *goquery.Selection) bool {
	class := strings.ToLower(firstAttr(sel, "class"))
	if strings.Contains(class, "disabled") {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(firstAttr(sel, "aria-disabled")), "true") {
		return true
	}
	if _, ok := sel.Attr("disabled"); ok {
		return true
	}
	parentClass := strings.ToLower(firstAttr(sel.Parent(), "class"))
	return strings.Contains(parentClass, "disabled")
}
