package webfetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
)

type hostResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

type defaultResolver struct{}

func (defaultResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return net.DefaultResolver.LookupIPAddr(ctx, host)
}

func secureHTTPClient(resolver hostResolver, allowBenchmark bool) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy:              nil,
			DialContext:        secureDialer(resolver, allowBenchmark),
			ForceAttemptHTTP2:  true,
			DisableCompression: true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func fetch(ctx context.Context, client *http.Client, resolver hostResolver, allowBenchmark bool, args arguments) (fetchResult, error) {
	requestedURL := args.URL
	resp, current, err := requestFollowingValidatedRedirects(ctx, client, resolver, allowBenchmark, requestedURL, acceptHeader(args.Format))
	if err != nil {
		return fetchResult{}, err
	}
	result, resultErr := responseResult(resp, requestedURL, current.String(), args.Format)
	_ = resp.Body.Close()
	return result, resultErr
}

func requestFollowingValidatedRedirects(
	ctx context.Context,
	client *http.Client,
	resolver hostResolver,
	allowBenchmark bool,
	rawURL string,
	accept string,
) (*http.Response, *url.URL, error) {
	current, err := parseAndValidateURL(ctx, resolver, allowBenchmark, rawURL)
	if err != nil {
		return nil, nil, err
	}
	for redirects := 0; ; redirects++ {
		if redirects > maxRedirects {
			return nil, nil, errors.New("too many redirects")
		}
		resp, err := executeRequest(ctx, client, current, accept)
		if err != nil {
			return nil, nil, fmt.Errorf("fetch %s: %w", current.Redacted(), err)
		}
		if isRedirect(resp.StatusCode) {
			location := strings.TrimSpace(resp.Header.Get("Location"))
			_ = resp.Body.Close()
			if location == "" {
				return nil, nil, errors.New("redirect missing location")
			}
			nextURL, err := current.Parse(location)
			if err != nil {
				return nil, nil, errors.New("invalid redirect location")
			}
			current, err = parseAndValidateURL(ctx, resolver, allowBenchmark, nextURL.String())
			if err != nil {
				return nil, nil, err
			}
			continue
		}
		return resp, current, nil
	}
}

func executeRequest(ctx context.Context, client *http.Client, target *url.URL, accept string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", accept)
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Accept-Encoding", "identity")
	return client.Do(req)
}

func parseAndValidateURL(ctx context.Context, resolver hostResolver, allowBenchmark bool, raw string) (*url.URL, error) {
	parsed, err := parseWebURL(raw)
	if err != nil {
		return nil, err
	}
	host := strings.Trim(parsed.Hostname(), "[]")
	if ip, ok := parseHostIP(host); ok {
		if !isPublicIP(ip) {
			return nil, errors.New("url host resolves to a blocked address")
		}
		return parsed, nil
	}
	addresses, err := resolveHost(ctx, resolver, host)
	if err != nil {
		return nil, err
	}
	for _, ip := range addresses {
		if !isAllowedResolvedIP(ip, allowBenchmark) {
			return nil, errors.New("url host resolves to a blocked address")
		}
	}
	return parsed, nil
}

func secureDialer(resolver hostResolver, allowBenchmark bool) func(context.Context, string, string) (net.Conn, error) {
	if resolver == nil {
		resolver = defaultResolver{}
	}
	dialer := &net.Dialer{}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		host = strings.Trim(host, "[]")
		addresses, err := resolveHost(ctx, resolver, host)
		if err != nil {
			return nil, err
		}
		for _, ip := range addresses {
			if !isAllowedDialIP(host, ip, allowBenchmark) {
				return nil, errors.New("dial target resolves to a blocked address")
			}
		}
		for _, ip := range addresses {
			connection, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return connection, nil
			}
		}
		return nil, errors.New("no resolved address accepted the connection")
	}
}

func resolveHost(ctx context.Context, resolver hostResolver, host string) ([]netip.Addr, error) {
	if ip, ok := parseHostIP(host); ok {
		return []netip.Addr{ip}, nil
	}
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return nil, errors.New("localhost is not allowed")
	}
	if resolver == nil {
		resolver = defaultResolver{}
	}
	resolved, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve host: %w", err)
	}
	if len(resolved) == 0 {
		return nil, errors.New("host did not resolve")
	}
	addresses := make([]netip.Addr, 0, len(resolved))
	for _, value := range resolved {
		ip, ok := netip.AddrFromSlice(value.IP)
		if !ok {
			return nil, errors.New("invalid resolved address")
		}
		addresses = append(addresses, ip.Unmap())
	}
	return addresses, nil
}

func parseHostIP(host string) (netip.Addr, bool) {
	if ip, err := netip.ParseAddr(host); err == nil {
		return ip.Unmap(), true
	}
	if ip := net.ParseIP(host); ip != nil {
		if address, ok := netip.AddrFromSlice(ip); ok {
			return address.Unmap(), true
		}
	}
	return parseEncodedIPv4(host)
}

func parseEncodedIPv4(host string) (netip.Addr, bool) {
	if strings.ContainsAny(host, ":") || strings.TrimSpace(host) == "" {
		return netip.Addr{}, false
	}
	parts := strings.Split(host, ".")
	if len(parts) > 4 {
		return netip.Addr{}, false
	}
	values := make([]uint64, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			return netip.Addr{}, false
		}
		base, text := 10, part
		if strings.HasPrefix(text, "0x") || strings.HasPrefix(text, "0X") {
			base, text = 16, text[2:]
		} else if len(text) > 1 && strings.HasPrefix(text, "0") {
			base = 8
		}
		value, err := strconv.ParseUint(text, base, 32)
		if err != nil {
			return netip.Addr{}, false
		}
		values = append(values, value)
	}
	var ipv4 uint32
	switch len(values) {
	case 1:
		ipv4 = uint32(values[0])
	case 2:
		if values[0] > 255 || values[1] > 0xffffff {
			return netip.Addr{}, false
		}
		ipv4 = uint32(values[0]<<24 | values[1])
	case 3:
		if values[0] > 255 || values[1] > 255 || values[2] > 0xffff {
			return netip.Addr{}, false
		}
		ipv4 = uint32(values[0]<<24 | values[1]<<16 | values[2])
	case 4:
		for _, value := range values {
			if value > 255 {
				return netip.Addr{}, false
			}
		}
		ipv4 = uint32(values[0]<<24 | values[1]<<16 | values[2]<<8 | values[3])
	default:
		return netip.Addr{}, false
	}
	return netip.AddrFrom4([4]byte{byte(ipv4 >> 24), byte(ipv4 >> 16), byte(ipv4 >> 8), byte(ipv4)}), true
}

func isPublicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}
	blocked := []string{
		"0.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "192.0.0.0/24", "192.0.2.0/24",
		"192.31.196.0/24", "192.52.193.0/24", "192.88.99.0/24", "192.175.48.0/24", "198.18.0.0/15",
		"198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
		"::/96", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64", "2001::/23", "2001:db8::/32",
		"2002::/16", "2620:4f:8000::/48", "3fff::/20", "5f00::/16", "fc00::/7", "fe80::/10", "ff00::/8",
	}
	for _, prefix := range blocked {
		if addrInPrefix(ip, prefix) {
			return false
		}
	}
	return true
}

func isAllowedResolvedIP(ip netip.Addr, allowBenchmark bool) bool {
	return isPublicIP(ip) || allowBenchmark && isBenchmarkIP(ip)
}

func isAllowedDialIP(host string, ip netip.Addr, allowBenchmark bool) bool {
	if _, literal := parseHostIP(host); literal {
		return isPublicIP(ip)
	}
	return isAllowedResolvedIP(ip, allowBenchmark)
}

func isBenchmarkIP(ip netip.Addr) bool { return addrInPrefix(ip.Unmap(), "198.18.0.0/15") }

func addrInPrefix(ip netip.Addr, raw string) bool {
	prefix, err := netip.ParsePrefix(raw)
	return err == nil && prefix.Contains(ip)
}

func isRedirect(status int) bool {
	switch status {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	default:
		return false
	}
}

func readBoundedBody(body io.Reader) ([]byte, bool, error) {
	content, err := io.ReadAll(io.LimitReader(body, maxBodyBytes+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(content)) <= maxBodyBytes {
		return content, false, nil
	}
	return content[:maxBodyBytes], true, nil
}

func acceptHeader(format string) string {
	if format == "text" {
		return "text/plain;q=1.0, text/markdown;q=0.9, text/html;q=0.8, application/json;q=0.7, application/xml;q=0.6, */*;q=0.1"
	}
	return "text/markdown;q=1.0, text/x-markdown;q=0.9, text/plain;q=0.8, text/html;q=0.7, application/json;q=0.6, application/xml;q=0.5, */*;q=0.1"
}
