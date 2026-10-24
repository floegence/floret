package activityfields

import (
	"context"
	"encoding/json"
	"strings"
)

// Value reads one selected subtree from a single caller-owned snapshot. The
// metadata pass proves its budget before any string body is read. Callbacks
// must keep the same immutable resident value or storage transaction throughout.
func Value(ctx context.Context, path string, maximumBytes, maximumNodes int,
	structure func(string, int, int) (Page, error), text func(string, int, int, int) (string, error),
) (json.RawMessage, error) {
	if maximumBytes < 1 || maximumBytes > 256<<10 || maximumNodes < 1 || maximumNodes > 256 {
		return nil, ErrRange
	}
	type node struct {
		field    Field
		children []*node
	}
	bytes, nodes := maximumBytes, maximumNodes
	var plan func(string) (*node, error)
	plan = func(path string) (*node, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		nodes--
		if nodes < 0 {
			return nil, ErrRange
		}
		page, err := structure(path, 0, 64)
		if err != nil {
			return nil, err
		}
		n := &node{field: page.Field}
		bytes -= len(path) + 8
		switch page.Field.Kind {
		case "string":
			bytes -= page.Field.Bytes
		case "scalar":
			bytes -= len(page.Field.Value)
		case "array", "object":
			if page.Field.Count > nodes {
				return nil, ErrRange
			}
		default:
			return nil, ErrPath
		}
		if bytes < 0 {
			return nil, ErrRange
		}
		for {
			for _, child := range page.Children {
				child, err := plan(child.Path)
				if err != nil {
					return nil, err
				}
				n.children = append(n.children, child)
			}
			if !page.HasMore {
				break
			}
			next := page.Next
			page, err = structure(path, next, 64)
			if err != nil {
				return nil, err
			}
			if page.Next <= next {
				return nil, ErrRange
			}
		}
		return n, nil
	}
	root, err := plan(path)
	if err != nil {
		return nil, err
	}
	var read func(*node) (any, error)
	read = func(n *node) (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch n.field.Kind {
		case "scalar":
			return n.field.Value, nil
		case "string":
			var out strings.Builder
			for out.Len() < n.field.Bytes {
				part, err := text(n.field.Path, out.Len(), min(16<<10, n.field.Bytes-out.Len()), n.field.Bytes)
				if err != nil {
					return nil, err
				}
				if len(part) == 0 || out.Len()+len(part) > n.field.Bytes {
					return nil, ErrRange
				}
				out.WriteString(part)
			}
			return out.String(), nil
		case "array":
			out := make([]any, 0, len(n.children))
			for _, child := range n.children {
				value, err := read(child)
				if err != nil {
					return nil, err
				}
				out = append(out, value)
			}
			return out, nil
		default:
			out := make(map[string]any, len(n.children))
			for _, child := range n.children {
				value, err := read(child)
				if err != nil {
					return nil, err
				}
				key := strings.TrimPrefix(child.field.Path, n.field.Path+"/")
				key = strings.ReplaceAll(strings.ReplaceAll(key, "~1", "/"), "~0", "~")
				out[key] = value
			}
			return out, nil
		}
	}
	value, err := read(root)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(raw) > maximumBytes {
		return nil, ErrRange
	}
	return raw, nil
}
