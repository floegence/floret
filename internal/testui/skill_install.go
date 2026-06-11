package testui

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/floegence/floret/internal/tools/skills"
)

const (
	managedSkillRootRel      = ".floret-test-ui/skills"
	managedArtifactsRootRel  = ".floret-test-ui/artifacts"
	maxSkillInstallFiles     = 64
	maxSkillInstallTotal     = 512 * 1024
	maxSkillInstallFileBytes = 256 * 1024
	maxSkillInstallDepth     = 8
	maxGitHubArchiveBytes    = 32 * 1024 * 1024
)

var (
	githubContentsAPIBase = "https://api.github.com/repos"
	githubRawContentBase  = "https://raw.githubusercontent.com"
	githubArchiveBase     = "https://codeload.github.com"
)

type githubSkillSource struct {
	Owner      string
	Repo       string
	Ref        string
	SourcePath string
	SingleFile string
}

type githubContentEntry struct {
	Name        string `json:"name"`
	Path        string `json:"path"`
	Type        string `json:"type"`
	Size        int64  `json:"size"`
	URL         string `json:"url"`
	DownloadURL string `json:"download_url"`
}

type stagedSkillFile struct {
	Path string
	Data []byte
}

type stagedSkill struct {
	Source  githubSkillSource
	Files   []stagedSkillFile
	Preview SkillInstallPreview
}

type skillDownloadBudget struct {
	FileCount  int
	TotalBytes int64
}

func (r *Runner) PreviewSkillInstall(ctx context.Context, req SkillInstallPreviewRequest) (SkillInstallPreview, error) {
	staged, err := r.stageSkillInstall(ctx, req.URL)
	if err != nil {
		return SkillInstallPreview{}, err
	}
	return staged.Preview, nil
}

func (r *Runner) InstallSkill(ctx context.Context, req SkillInstallRequest) (SkillInstallResponse, error) {
	if strings.TrimSpace(req.PreviewToken) == "" {
		return SkillInstallResponse{}, fmt.Errorf("preview_token is required")
	}
	staged, err := r.stageSkillInstall(ctx, req.URL)
	if err != nil {
		return SkillInstallResponse{}, err
	}
	if staged.Preview.PreviewToken != req.PreviewToken {
		return SkillInstallResponse{}, fmt.Errorf("preview token does not match current skill contents")
	}
	if staged.Preview.RequiresReplace && !req.Replace {
		return SkillInstallResponse{}, fmt.Errorf("skill %q is already installed; set replace to true to overwrite it", staged.Preview.Name)
	}
	if err := writeStagedSkill(staged.Preview.TargetPath, staged.Files); err != nil {
		return SkillInstallResponse{}, err
	}
	sourceRoot := r.managedSkillRoot()
	envUpdated, err := r.ensureManagedSkillEnv(sourceRoot)
	if err != nil {
		return SkillInstallResponse{}, err
	}
	state := r.capabilityStateFromEnv()
	state.Diagnostics = append(state.Diagnostics, r.skillEnvOverrideDiagnostics(sourceRoot)...)
	return SkillInstallResponse{
		Skill:        staged.Preview,
		Capabilities: state,
		SourceRoot:   sourceRoot,
		EnvUpdated:   envUpdated,
	}, nil
}

func (r *Runner) skillEnvOverrideDiagnostics(sourceRoot string) []CapabilityDiagnostic {
	values, err := readDotEnv(r.EnvFile)
	if err != nil {
		return nil
	}
	expectedEnabled := strings.TrimSpace(values["FLORET_SKILLS_ENABLED"])
	expectedPaths := strings.TrimSpace(values["FLORET_SKILLS_PATHS"])
	diagnostics := []CapabilityDiagnostic{}
	if env := strings.TrimSpace(os.Getenv("FLORET_SKILLS_ENABLED")); env != "" && expectedEnabled != "" && env != expectedEnabled {
		diagnostics = append(diagnostics, CapabilityDiagnostic{
			Kind:       "skills_env_overridden",
			Capability: "skills",
			Message:    "FLORET_SKILLS_ENABLED from the process environment overrides .env.local.",
			NextAction: "Restart the test UI without the override or update the process environment to enable installed skills.",
		})
	}
	if env := strings.TrimSpace(os.Getenv("FLORET_SKILLS_PATHS")); env != "" && expectedPaths != "" && env != expectedPaths {
		diagnostics = append(diagnostics, CapabilityDiagnostic{
			Kind:       "skills_env_overridden",
			Capability: "skills",
			Message:    "FLORET_SKILLS_PATHS from the process environment overrides .env.local.",
			NextAction: fmt.Sprintf("Include %s in FLORET_SKILLS_PATHS or restart the test UI without the override.", sourceRoot),
		})
	}
	return diagnostics
}

func (r *Runner) stageSkillInstall(ctx context.Context, rawURL string) (stagedSkill, error) {
	source, err := parseGitHubSkillURL(rawURL)
	if err != nil {
		return stagedSkill{}, err
	}
	files, err := downloadGitHubSkill(ctx, source)
	if err != nil {
		return stagedSkill{}, err
	}
	if len(files) == 0 {
		return stagedSkill{}, fmt.Errorf("skill source did not contain any files")
	}
	if len(files) > maxSkillInstallFiles {
		return stagedSkill{}, fmt.Errorf("skill source contains %d files; limit is %d", len(files), maxSkillInstallFiles)
	}
	var total int64
	for _, file := range files {
		total += int64(len(file.Data))
		if len(file.Data) > maxSkillInstallFileBytes {
			return stagedSkill{}, fmt.Errorf("skill file %q is too large", file.Path)
		}
	}
	if total > maxSkillInstallTotal {
		return stagedSkill{}, fmt.Errorf("skill source is too large")
	}
	skillFile := stagedFileByPath(files, "SKILL.md")
	if skillFile == nil {
		return stagedSkill{}, fmt.Errorf("skill source must contain SKILL.md")
	}
	meta, err := skills.ParseMetadata(string(skillFile.Data))
	if err != nil {
		return stagedSkill{}, err
	}
	name := strings.TrimSpace(meta["name"])
	description := strings.TrimSpace(meta["description"])
	if name == "" {
		name = path.Base(strings.Trim(source.SourcePath, "/"))
	}
	if err := skills.ValidateName(name); err != nil {
		return stagedSkill{}, err
	}
	if path.Base(strings.Trim(source.SourcePath, "/")) != name {
		return stagedSkill{}, fmt.Errorf("skill directory must match name %q", name)
	}
	if _, err := validateStagedSkillWithCoreContract(files, name); err != nil {
		return stagedSkill{}, err
	}
	if description == "" {
		return stagedSkill{}, fmt.Errorf("skill %q description is required", name)
	}
	license := strings.TrimSpace(meta["license"])
	if license == "" {
		if licenseFile := stagedFileByPath(files, "LICENSE.txt"); licenseFile != nil {
			license = "LICENSE.txt"
		}
	}
	target, err := managedSkillTarget(r.managedSkillRoot(), name)
	if err != nil {
		return stagedSkill{}, err
	}
	existingHash := ""
	if hash, err := installedSkillContentHash(target); err == nil {
		existingHash = hash
	}
	contentHash := stagedContentHash(files)
	preview := SkillInstallPreview{
		URL:             strings.TrimSpace(rawURL),
		Repo:            source.Owner + "/" + source.Repo,
		Ref:             source.Ref,
		SourcePath:      source.SourcePath,
		Name:            name,
		Description:     description,
		License:         license,
		Files:           installFileSummaries(files),
		TotalBytes:      total,
		TargetPath:      target,
		ExistingHash:    existingHash,
		ContentHash:     contentHash,
		RequiresReplace: existingHash != "",
	}
	preview.PreviewToken = previewToken(preview)
	return stagedSkill{Source: source, Files: files, Preview: preview}, nil
}

func parseGitHubSkillURL(raw string) (githubSkillSource, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return githubSkillSource{}, fmt.Errorf("url is required")
	}
	u, err := url.Parse(value)
	if err != nil {
		return githubSkillSource{}, err
	}
	host := strings.ToLower(u.Hostname())
	parts := cleanPathParts(u.EscapedPath())
	switch host {
	case "github.com", "www.github.com":
		return parseGitHubWebPath(parts)
	case "raw.githubusercontent.com":
		return parseGitHubRawPath(parts)
	default:
		return githubSkillSource{}, fmt.Errorf("only GitHub skill URLs are supported")
	}
}

func parseGitHubWebPath(parts []string) (githubSkillSource, error) {
	if len(parts) < 5 {
		return githubSkillSource{}, fmt.Errorf("GitHub URL must include owner, repo, tree/blob, ref, and path")
	}
	kind := parts[2]
	if kind != "tree" && kind != "blob" {
		return githubSkillSource{}, fmt.Errorf("GitHub URL must use tree or blob")
	}
	sourcePath := strings.Join(parts[4:], "/")
	source := githubSkillSource{Owner: parts[0], Repo: parts[1], Ref: parts[3], SourcePath: sourcePath}
	if kind == "blob" {
		if path.Base(sourcePath) != "SKILL.md" {
			return githubSkillSource{}, fmt.Errorf("GitHub blob URL must point to SKILL.md")
		}
		source.SingleFile = "SKILL.md"
		source.SourcePath = strings.TrimSuffix(sourcePath, "/SKILL.md")
	}
	return validateGitHubSource(source)
}

func parseGitHubRawPath(parts []string) (githubSkillSource, error) {
	if len(parts) < 5 {
		return githubSkillSource{}, fmt.Errorf("raw GitHub URL must include owner, repo, ref, and SKILL.md path")
	}
	sourcePath := strings.Join(parts[3:], "/")
	if path.Base(sourcePath) != "SKILL.md" {
		return githubSkillSource{}, fmt.Errorf("raw GitHub URL must point to SKILL.md")
	}
	return validateGitHubSource(githubSkillSource{
		Owner:      parts[0],
		Repo:       parts[1],
		Ref:        parts[2],
		SourcePath: strings.TrimSuffix(sourcePath, "/SKILL.md"),
		SingleFile: "SKILL.md",
	})
}

func validateGitHubSource(source githubSkillSource) (githubSkillSource, error) {
	for _, value := range []string{source.Owner, source.Repo, source.Ref, source.SourcePath} {
		if value == "" || value == "." || strings.Contains(value, "..") || strings.HasPrefix(value, "/") {
			return githubSkillSource{}, fmt.Errorf("GitHub skill URL contains an unsafe path")
		}
	}
	return source, nil
}

func cleanPathParts(escaped string) []string {
	raw := strings.Trim(path.Clean("/"+escaped), "/")
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, "/")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if decoded, err := url.PathUnescape(part); err == nil {
			part = decoded
		}
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func downloadGitHubSkill(ctx context.Context, source githubSkillSource) ([]stagedSkillFile, error) {
	budget := &skillDownloadBudget{}
	files, archiveErr := downloadGitHubArchiveSkill(ctx, source, budget)
	if archiveErr == nil {
		return files, nil
	}
	apiURL := fmt.Sprintf("%s/%s/%s/contents/%s?ref=%s", strings.TrimRight(githubContentsAPIBase, "/"), source.Owner, source.Repo, source.SourcePath, url.QueryEscape(source.Ref))
	files, contentsErr := downloadGitHubDirectory(ctx, apiURL, "", 0, &skillDownloadBudget{})
	if contentsErr == nil {
		return files, nil
	}
	return nil, fmt.Errorf("download GitHub skill archive: %v; contents API: %v", archiveErr, contentsErr)
}

func downloadGitHubArchiveSkill(ctx context.Context, source githubSkillSource, budget *skillDownloadBudget) ([]stagedSkillFile, error) {
	refKind := "heads"
	refValue := source.Ref
	if strings.HasPrefix(source.Ref, "refs/") {
		parts := strings.SplitN(strings.TrimPrefix(source.Ref, "refs/"), "/", 2)
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			refKind = parts[0]
			refValue = parts[1]
		}
	}
	archiveURL := fmt.Sprintf("%s/%s/%s/zip/refs/%s/%s", strings.TrimRight(githubArchiveBase, "/"), source.Owner, source.Repo, refKind, refValue)
	data, err := downloadBytesLimited(ctx, archiveURL, maxGitHubArchiveBytes)
	if err != nil {
		return nil, err
	}
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("parse GitHub archive: %w", err)
	}
	sourcePrefix := strings.Trim(strings.Trim(source.SourcePath, "/"), ".")
	if sourcePrefix == "" {
		return nil, fmt.Errorf("GitHub skill path is required")
	}
	sourcePrefix += "/"
	files := []stagedSkillFile{}
	for _, file := range reader.File {
		name := strings.TrimPrefix(file.Name, "/")
		parts := strings.SplitN(name, "/", 2)
		if len(parts) != 2 {
			continue
		}
		relInRepo := parts[1]
		if !strings.HasPrefix(relInRepo, sourcePrefix) {
			continue
		}
		rel := strings.TrimPrefix(relInRepo, sourcePrefix)
		if rel == "" || strings.HasSuffix(rel, "/") {
			continue
		}
		if strings.HasPrefix(rel, "/") || strings.Contains(rel, "..") {
			return nil, fmt.Errorf("GitHub archive contains unsafe file path %q", rel)
		}
		if file.FileInfo().IsDir() {
			continue
		}
		if !file.FileInfo().Mode().IsRegular() {
			return nil, fmt.Errorf("GitHub archive contains non-regular file %q", rel)
		}
		if file.UncompressedSize64 > uint64(maxSkillInstallFileBytes) {
			return nil, fmt.Errorf("skill file %q is too large", rel)
		}
		if budget.FileCount+1 > maxSkillInstallFiles {
			return nil, fmt.Errorf("skill source contains too many files")
		}
		if budget.TotalBytes+int64(file.UncompressedSize64) > maxSkillInstallTotal {
			return nil, fmt.Errorf("skill source is too large")
		}
		rc, err := file.Open()
		if err != nil {
			return nil, err
		}
		fileData, err := io.ReadAll(io.LimitReader(rc, maxSkillInstallFileBytes+1))
		closeErr := rc.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if len(fileData) > maxSkillInstallFileBytes {
			return nil, fmt.Errorf("skill file %q is too large", rel)
		}
		budget.FileCount++
		budget.TotalBytes += int64(len(fileData))
		if budget.TotalBytes > maxSkillInstallTotal {
			return nil, fmt.Errorf("skill source is too large")
		}
		files = append(files, stagedSkillFile{Path: rel, Data: fileData})
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("GitHub archive did not contain skill path %q", strings.TrimSuffix(sourcePrefix, "/"))
	}
	slices.SortFunc(files, func(a, b stagedSkillFile) int {
		return strings.Compare(a.Path, b.Path)
	})
	return files, nil
}

func downloadGitHubDirectory(ctx context.Context, apiURL string, base string, depth int, budget *skillDownloadBudget) ([]stagedSkillFile, error) {
	if depth > maxSkillInstallDepth {
		return nil, fmt.Errorf("skill source directory is too deep")
	}
	data, err := downloadBytes(ctx, apiURL)
	if err != nil {
		return nil, err
	}
	var entries []githubContentEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		var single githubContentEntry
		if singleErr := json.Unmarshal(data, &single); singleErr != nil {
			return nil, fmt.Errorf("parse GitHub contents: %w", err)
		}
		entries = []githubContentEntry{single}
	}
	out := []stagedSkillFile{}
	for _, entry := range entries {
		name := strings.TrimSpace(entry.Name)
		if name == "" || strings.Contains(name, "/") || name == "." || name == ".." {
			return nil, fmt.Errorf("GitHub contents returned unsafe file name")
		}
		rel := path.Join(base, name)
		switch entry.Type {
		case "file":
			if entry.DownloadURL == "" {
				return nil, fmt.Errorf("GitHub file %q has no download URL", rel)
			}
			if entry.Size > maxSkillInstallFileBytes {
				return nil, fmt.Errorf("skill file %q is too large", rel)
			}
			if budget.FileCount+1 > maxSkillInstallFiles {
				return nil, fmt.Errorf("skill source contains too many files")
			}
			if budget.TotalBytes+entry.Size > maxSkillInstallTotal {
				return nil, fmt.Errorf("skill source is too large")
			}
			fileData, err := downloadBytes(ctx, entry.DownloadURL)
			if err != nil {
				return nil, err
			}
			if len(fileData) > maxSkillInstallFileBytes {
				return nil, fmt.Errorf("skill file %q is too large", rel)
			}
			budget.FileCount++
			budget.TotalBytes += int64(len(fileData))
			if budget.TotalBytes > maxSkillInstallTotal {
				return nil, fmt.Errorf("skill source is too large")
			}
			out = append(out, stagedSkillFile{Path: rel, Data: fileData})
		case "dir":
			childrenURL := strings.TrimSpace(entry.URL)
			if childrenURL == "" {
				return nil, fmt.Errorf("GitHub directory %q is missing API URL", rel)
			}
			children, err := downloadGitHubDirectory(ctx, childrenURL, rel, depth+1, budget)
			if err != nil {
				return nil, err
			}
			out = append(out, children...)
		case "symlink", "submodule":
			return nil, fmt.Errorf("skill source must not contain %s %q", entry.Type, rel)
		default:
			return nil, fmt.Errorf("unsupported GitHub entry type %q", entry.Type)
		}
	}
	slices.SortFunc(out, func(a, b stagedSkillFile) int {
		return strings.Compare(a.Path, b.Path)
	})
	return out, nil
}

func downloadBytes(ctx context.Context, rawURL string) ([]byte, error) {
	return downloadBytesLimited(ctx, rawURL, maxSkillInstallTotal)
}

func downloadBytesLimited(ctx context.Context, rawURL string, maxBytes int) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "floret-test-ui")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("download %s failed: %s", rawURL, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, int64(maxBytes)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxBytes {
		return nil, fmt.Errorf("download exceeded size limit")
	}
	return data, nil
}

func writeStagedSkill(target string, files []stagedSkillFile) error {
	parent := filepath.Dir(target)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(parent, ".skill-install-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	for _, file := range files {
		if err := safeWriteSkillFile(tmp, file.Path, file.Data); err != nil {
			return err
		}
	}
	previous := ""
	if _, err := os.Stat(target); err == nil {
		stash, stashErr := os.MkdirTemp(parent, ".skill-install-previous-*")
		if stashErr != nil {
			return stashErr
		}
		previous = filepath.Join(stash, filepath.Base(target))
		if renameErr := os.Rename(target, previous); renameErr != nil {
			_ = os.RemoveAll(stash)
			return renameErr
		}
		defer os.RemoveAll(stash)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		if previous != "" {
			_ = os.Rename(previous, target)
		}
		return err
	}
	return nil
}

func validateStagedSkillWithCoreContract(files []stagedSkillFile, name string) (skills.Skill, error) {
	tmp, err := os.MkdirTemp("", "floret-skill-preview-*")
	if err != nil {
		return skills.Skill{}, err
	}
	defer os.RemoveAll(tmp)
	skillDir := filepath.Join(tmp, name)
	for _, file := range files {
		if err := safeWriteSkillFile(skillDir, file.Path, file.Data); err != nil {
			return skills.Skill{}, err
		}
	}
	catalog, err := skills.Discover([]skills.Source{{Root: tmp, Kind: skills.SourceConfig, Enabled: true, DisplayLabel: "preview"}})
	if err != nil {
		return skills.Skill{}, err
	}
	if len(catalog.Diagnostics) > 0 {
		return skills.Skill{}, fmt.Errorf("%s", catalog.Diagnostics[0].Message)
	}
	if len(catalog.Skills) != 1 || catalog.Skills[0].Name != name {
		return skills.Skill{}, fmt.Errorf("skill preview did not validate as %q", name)
	}
	return catalog.Skills[0], nil
}

func managedSkillTarget(root, name string) (string, error) {
	if err := skills.ValidateName(name); err != nil {
		return "", err
	}
	target := filepath.Join(root, name)
	if err := ensurePathInsideRoot(root, target); err != nil {
		return "", err
	}
	return target, nil
}

func safeWriteSkillFile(root, rel string, data []byte) error {
	if rel == "" || strings.HasPrefix(rel, "/") || strings.Contains(rel, "..") {
		return fmt.Errorf("unsafe skill file path %q", rel)
	}
	dst := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o644)
}

func (r *Runner) ensureManagedSkillEnv(sourceRoot string) (bool, error) {
	values, err := readDotEnv(r.EnvFile)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	next := map[string]string{
		"FLORET_SKILLS_ENABLED": "true",
		"FLORET_SKILLS_PATHS":   joinSkillPaths(values["FLORET_SKILLS_PATHS"], sourceRoot),
	}
	return writeManagedEnvValues(r.EnvFile, next)
}

func writeManagedEnvValues(path string, values map[string]string) (bool, error) {
	existingValues, _ := readDotEnv(path)
	changed := false
	for key, value := range values {
		if existingValues[key] != value {
			changed = true
			break
		}
	}
	if !changed {
		return false, nil
	}
	keys := make(map[string]struct{}, len(values))
	for key := range values {
		keys[key] = struct{}{}
	}
	var b strings.Builder
	if existing, err := os.ReadFile(path); err == nil {
		for _, line := range strings.Split(string(existing), "\n") {
			trimmed := strings.TrimSpace(line)
			key, _, ok := strings.Cut(trimmed, "=")
			if ok {
				if _, managed := keys[strings.TrimSpace(key)]; managed {
					continue
				}
			}
			if trimmed != "" {
				b.WriteString(line)
				b.WriteByte('\n')
			}
		}
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
	}
	b.WriteString("# Managed by Floret Test Console skills.\n")
	for _, key := range sortedKeys(values) {
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(envQuote(values[key]))
		b.WriteByte('\n')
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, err
	}
	return true, os.WriteFile(path, []byte(b.String()), 0o600)
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func joinSkillPaths(raw string, add string) string {
	add = strings.TrimSpace(add)
	seen := map[string]struct{}{}
	out := []string{}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	if _, ok := seen[add]; !ok && add != "" {
		out = append(out, add)
	}
	return strings.Join(out, ",")
}

func (r *Runner) managedSkillRoot() string {
	return filepath.Join(r.Root, managedSkillRootRel)
}

func (r *Runner) managedArtifactsRoot() string {
	return filepath.Join(r.Root, managedArtifactsRootRel)
}

func stagedFileByPath(files []stagedSkillFile, rel string) *stagedSkillFile {
	for i := range files {
		if files[i].Path == rel {
			return &files[i]
		}
	}
	return nil
}

func installFileSummaries(files []stagedSkillFile) []SkillInstallFile {
	out := make([]SkillInstallFile, 0, len(files))
	for _, file := range files {
		out = append(out, SkillInstallFile{Path: file.Path, Bytes: int64(len(file.Data))})
	}
	return out
}

func stagedContentHash(files []stagedSkillFile) string {
	h := sha256.New()
	for _, file := range files {
		h.Write([]byte(file.Path))
		h.Write([]byte{0})
		h.Write(file.Data)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func installedSkillContentHash(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	files := []stagedSkillFile{}
	var walk func(string, []os.DirEntry) error
	walk = func(base string, dirEntries []os.DirEntry) error {
		for _, entry := range dirEntries {
			rel := path.Join(base, entry.Name())
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("installed skill contains symlink %q", rel)
			}
			full := filepath.Join(dir, filepath.FromSlash(rel))
			if entry.IsDir() {
				children, err := os.ReadDir(full)
				if err != nil {
					return err
				}
				if err := walk(rel, children); err != nil {
					return err
				}
				continue
			}
			if !entry.Type().IsRegular() {
				return fmt.Errorf("installed skill contains non-regular file %q", rel)
			}
			data, err := os.ReadFile(full)
			if err != nil {
				return err
			}
			files = append(files, stagedSkillFile{Path: rel, Data: data})
		}
		return nil
	}
	if err := walk("", entries); err != nil {
		return "", err
	}
	slices.SortFunc(files, func(a, b stagedSkillFile) int {
		return strings.Compare(a.Path, b.Path)
	})
	return stagedContentHash(files), nil
}

func previewToken(preview SkillInstallPreview) string {
	value := strings.Join([]string{preview.URL, preview.Name, preview.ContentHash, preview.TargetPath}, "\x00")
	sum := sha256.Sum256([]byte(value))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func licenseForInstalledSkill(dir string) string {
	data, err := os.ReadFile(filepath.Join(dir, "SKILL.md"))
	if err != nil {
		return ""
	}
	meta, err := skills.ParseMetadata(string(data))
	if err != nil {
		return ""
	}
	license := strings.TrimSpace(meta["license"])
	if license != "" {
		return license
	}
	if _, err := os.Stat(filepath.Join(dir, "LICENSE.txt")); err == nil {
		return "LICENSE.txt"
	}
	return ""
}

func artifactFile(root, rawPath string) (string, error) {
	value := strings.TrimPrefix(strings.TrimSpace(rawPath), "/")
	if value == "" {
		return "", errors.New("artifact path is required")
	}
	for _, segment := range strings.Split(value, "/") {
		if segment == ".." {
			return "", errors.New("artifact path is invalid")
		}
	}
	cleaned := path.Clean("/" + value)
	if cleaned == "/" || strings.Contains(cleaned, "..") {
		return "", errors.New("artifact path is invalid")
	}
	full := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(cleaned, "/")))
	if err := ensurePathInsideRoot(root, full); err != nil {
		return "", errors.New("artifact path escapes artifact root")
	}
	return full, nil
}

func ensurePathInsideRoot(root, full string) error {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	absFull, err := filepath.Abs(full)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(absRoot, absFull)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return errors.New("path escapes root")
	}
	return nil
}

func ensureRealPathInsideRoot(root, full string) error {
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	realFull, err := filepath.EvalSymlinks(full)
	if err != nil {
		return err
	}
	return ensurePathInsideRoot(realRoot, realFull)
}

func managedSkillRootDiagnostic(root string) CapabilityDiagnostic {
	return CapabilityDiagnostic{
		Kind:       "skills_not_configured",
		Capability: "skills",
		Message:    "No Agent Skills source is enabled.",
		NextAction: fmt.Sprintf("Install a skill from the Skills page or set FLORET_SKILLS_PATHS to include %s.", root),
	}
}
