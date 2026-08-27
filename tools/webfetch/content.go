package webfetch

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/JohannesKaufmann/html-to-markdown/v2/converter"
	"github.com/JohannesKaufmann/html-to-markdown/v2/plugin/base"
	"github.com/JohannesKaufmann/html-to-markdown/v2/plugin/commonmark"
	"github.com/JohannesKaufmann/html-to-markdown/v2/plugin/strikethrough"
	"github.com/JohannesKaufmann/html-to-markdown/v2/plugin/table"
	"golang.org/x/net/html"
	"golang.org/x/net/html/charset"
)

func responseResult(resp *http.Response, requestedURL, finalURL, format string) (fetchResult, error) {
	body, truncated, err := readBoundedBody(resp.Body)
	if err != nil {
		return fetchResult{}, fmt.Errorf("read response body: %w", err)
	}
	contentType, mediaType, params, err := normalizedContentType(resp.Header.Get("Content-Type"), body)
	if err != nil {
		return fetchResult{}, err
	}
	if !isTextualMIME(mediaType) {
		return fetchResult{}, fmt.Errorf("unsupported content type %q", contentType)
	}
	decoded, err := decodeText(body, mediaType, contentType, params)
	if err != nil {
		return fetchResult{}, err
	}
	content := decoded
	var iconURL string
	if isHTMLMIME(mediaType) {
		content, iconURL, err = extractHTML(decoded, finalURL, format)
		if err != nil {
			return fetchResult{}, err
		}
	}
	return fetchResult{
		URL: requestedURL, FinalURL: finalURL, StatusCode: resp.StatusCode, ContentType: contentType,
		Format: format, Content: strings.TrimSpace(content), BytesRead: int64(len(body)), Truncated: truncated, iconURL: iconURL,
	}, nil
}

func normalizedContentType(header string, body []byte) (string, string, map[string]string, error) {
	header = strings.TrimSpace(header)
	if header == "" {
		header = http.DetectContentType(body)
	}
	mediaType, params, err := mime.ParseMediaType(header)
	if err != nil {
		mediaType = strings.ToLower(strings.TrimSpace(strings.Split(header, ";")[0]))
		params = map[string]string{}
		if mediaType == "" {
			return "", "", nil, errors.New("invalid content type")
		}
	}
	mediaType = strings.ToLower(strings.TrimSpace(mediaType))
	normalized := mime.FormatMediaType(mediaType, params)
	if normalized == "" {
		normalized = mediaType
	}
	return normalized, mediaType, params, nil
}

func isTextualMIME(mediaType string) bool {
	mediaType = strings.ToLower(strings.TrimSpace(mediaType))
	return strings.HasPrefix(mediaType, "text/") ||
		mediaType == "application/json" || strings.HasSuffix(mediaType, "+json") ||
		mediaType == "application/xml" || strings.HasSuffix(mediaType, "+xml") ||
		mediaType == "application/javascript" || mediaType == "application/x-javascript"
}

func isHTMLMIME(mediaType string) bool {
	return mediaType == "text/html" || mediaType == "application/xhtml+xml"
}

func decodeText(body []byte, mediaType, contentType string, params map[string]string) (string, error) {
	if isHTMLMIME(mediaType) {
		reader, err := charset.NewReader(bytes.NewReader(body), contentType)
		if err != nil {
			return "", fmt.Errorf("decode html charset: %w", err)
		}
		decoded, err := io.ReadAll(reader)
		if err != nil {
			return "", fmt.Errorf("decode html charset: %w", err)
		}
		return string(decoded), nil
	}
	if label := strings.TrimSpace(params["charset"]); label != "" && !strings.EqualFold(label, "utf-8") && !strings.EqualFold(label, "us-ascii") {
		reader, err := charset.NewReaderLabel(label, bytes.NewReader(body))
		if err != nil {
			return "", fmt.Errorf("unsupported charset %q", label)
		}
		decoded, err := io.ReadAll(reader)
		if err != nil {
			return "", fmt.Errorf("decode charset %q: %w", label, err)
		}
		return string(decoded), nil
	}
	body = bytes.TrimPrefix(body, []byte{0xef, 0xbb, 0xbf})
	if !utf8.Valid(body) {
		return "", errors.New("invalid text encoding")
	}
	return string(body), nil
}

func extractHTML(source, finalURL, format string) (string, string, error) {
	document, err := html.Parse(strings.NewReader(source))
	if err != nil {
		return "", "", fmt.Errorf("parse html: %w", err)
	}
	if exceedsHTMLDepth(document, 0) {
		return "", "", errors.New("html nesting exceeds the supported limit")
	}
	iconURL := discoverSiteIconURL(document, finalURL)
	pruneUntrustedHTML(document)
	if format == "text" {
		return htmlPlainText(document), iconURL, nil
	}
	markdown := converter.NewConverter(converter.WithPlugins(
		base.NewBasePlugin(),
		commonmark.NewCommonmarkPlugin(),
		table.NewTablePlugin(table.WithCellPaddingBehavior(table.CellPaddingBehaviorMinimal)),
		strikethrough.NewStrikethroughPlugin(),
	))
	for _, tag := range []string{"object", "embed"} {
		markdown.Register.TagType(tag, converter.TagTypeRemove, converter.PriorityStandard)
	}
	converted, err := markdown.ConvertNode(document, converter.WithDomain(finalURL))
	if err != nil {
		return "", "", fmt.Errorf("convert html to markdown: %w", err)
	}
	return strings.TrimSpace(string(converted)), iconURL, nil
}

func discoverSiteIconURL(document *html.Node, finalURL string) string {
	pageURL, err := parseWebURL(finalURL)
	if err != nil {
		return ""
	}
	var iconURL string
	var visit func(*html.Node)
	visit = func(node *html.Node) {
		if iconURL != "" {
			return
		}
		if node.Type == html.ElementNode && strings.EqualFold(node.Data, "link") {
			var rel, href string
			for _, attribute := range node.Attr {
				switch strings.ToLower(attribute.Key) {
				case "rel":
					rel = attribute.Val
				case "href":
					href = attribute.Val
				}
			}
			if hasIconRelation(rel) && strings.TrimSpace(href) != "" {
				candidate, parseErr := pageURL.Parse(strings.TrimSpace(href))
				if parseErr == nil && sameOrigin(pageURL, candidate) {
					if validated, validateErr := parseWebURL(candidate.String()); validateErr == nil {
						iconURL = validated.String()
					}
				}
			}
		}
		for child := node.FirstChild; child != nil && iconURL == ""; child = child.NextSibling {
			visit(child)
		}
	}
	visit(document)
	if iconURL != "" {
		return iconURL
	}
	fallback := &url.URL{Scheme: pageURL.Scheme, Host: pageURL.Host, Path: "/favicon.ico"}
	return fallback.String()
}

func hasIconRelation(value string) bool {
	for _, token := range strings.Fields(strings.ToLower(value)) {
		if token == "icon" {
			return true
		}
	}
	return false
}

func sameOrigin(left, right *url.URL) bool {
	if left == nil || right == nil || !strings.EqualFold(left.Scheme, right.Scheme) || !strings.EqualFold(left.Hostname(), right.Hostname()) {
		return false
	}
	return effectivePort(left) == effectivePort(right)
}

func effectivePort(value *url.URL) string {
	if port := value.Port(); port != "" {
		return port
	}
	if strings.EqualFold(value.Scheme, "http") {
		return "80"
	}
	if strings.EqualFold(value.Scheme, "https") {
		return "443"
	}
	return ""
}

func pruneUntrustedHTML(node *html.Node) {
	for child := node.FirstChild; child != nil; {
		next := child.NextSibling
		if child.Type == html.ElementNode && removedHTMLTag(strings.ToLower(child.Data)) {
			node.RemoveChild(child)
		} else {
			pruneUntrustedHTML(child)
		}
		child = next
	}
}

func exceedsHTMLDepth(node *html.Node, depth int) bool {
	if depth > 512 {
		return true
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if exceedsHTMLDepth(child, depth+1) {
			return true
		}
	}
	return false
}

func htmlPlainText(document *html.Node) string {
	var output strings.Builder
	var visit func(*html.Node, bool)
	visit = func(node *html.Node, inPre bool) {
		if node.Type == html.ElementNode {
			name := strings.ToLower(node.Data)
			if removedHTMLTag(name) {
				return
			}
			if name == "br" || blockHTMLTag(name) {
				appendTextNewline(&output)
			}
			inPre = inPre || name == "pre"
		}
		if node.Type == html.TextNode {
			if inPre {
				output.WriteString(node.Data)
			} else {
				appendCollapsedText(&output, node.Data)
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			visit(child, inPre)
		}
		if node.Type == html.ElementNode && blockHTMLTag(strings.ToLower(node.Data)) {
			appendTextNewline(&output)
		}
	}
	visit(document, false)
	lines := strings.Split(strings.ReplaceAll(output.String(), "\r\n", "\n"), "\n")
	cleaned := make([]string, 0, len(lines))
	blank := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			if len(cleaned) > 0 && !blank {
				cleaned = append(cleaned, "")
				blank = true
			}
			continue
		}
		cleaned = append(cleaned, line)
		blank = false
	}
	return strings.TrimSpace(strings.Join(cleaned, "\n"))
}

func removedHTMLTag(name string) bool {
	switch name {
	case "script", "style", "noscript", "iframe", "object", "embed", "meta", "link":
		return true
	default:
		return false
	}
}

func blockHTMLTag(name string) bool {
	switch name {
	case "address", "article", "aside", "blockquote", "div", "dl", "dt", "dd", "fieldset", "figcaption", "figure", "footer",
		"form", "h1", "h2", "h3", "h4", "h5", "h6", "header", "hr", "li", "main", "nav", "ol", "p", "pre", "section",
		"table", "tbody", "thead", "tfoot", "tr", "td", "th", "ul":
		return true
	default:
		return false
	}
}

func appendCollapsedText(output *strings.Builder, value string) {
	value = strings.Join(strings.Fields(value), " ")
	if value == "" {
		return
	}
	current := output.String()
	if len(current) > 0 {
		last := current[len(current)-1]
		if last != '\n' && last != ' ' {
			output.WriteByte(' ')
		}
	}
	output.WriteString(value)
}

func appendTextNewline(output *strings.Builder) {
	current := output.String()
	if current == "" || strings.HasSuffix(current, "\n") {
		return
	}
	output.WriteByte('\n')
}
