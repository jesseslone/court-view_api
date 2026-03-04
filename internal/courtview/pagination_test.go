package courtview

import (
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
)

func TestFindNextResultsPageURL(t *testing.T) {
	html := `
<html><body>
  <div id="grid">
    <div class="k-pager-wrap">
      <a class="k-pager-nav k-pager-next" href="searchresults.page?x=abc123">></a>
    </div>
  </div>
</body></html>`
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		t.Fatalf("parse html: %v", err)
	}

	next, ok := findNextResultsPageURL(doc, "https://records.courts.alaska.gov/eaccess/searchresults.page")
	if !ok {
		t.Fatalf("expected next page URL")
	}
	want := "https://records.courts.alaska.gov/eaccess/searchresults.page?x=abc123"
	if next != want {
		t.Fatalf("got %q, want %q", next, want)
	}
}

func TestFindNextResultsPageURLSkipsDisabled(t *testing.T) {
	html := `
<html><body>
  <div id="grid">
    <div class="k-pager-wrap">
      <a class="k-pager-nav k-pager-next disabled" href="searchresults.page?x=abc123">></a>
    </div>
  </div>
</body></html>`
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		t.Fatalf("parse html: %v", err)
	}

	if next, ok := findNextResultsPageURL(doc, "https://records.courts.alaska.gov/eaccess/searchresults.page"); ok {
		t.Fatalf("expected no next page URL, got %q", next)
	}
}
