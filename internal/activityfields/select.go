package activityfields

import "strings"

// TextFields validates explicitly deferred direct string children. Their
// sources are returned beside a selected value from the same snapshot.
func TextFields(path string, fields, textFields []string) ([]string, error) {
	if len(path) > 4096 || path != "" && !strings.HasPrefix(path, "/") {
		return nil, ErrPath
	}
	if len(textFields) > 8 || len(textFields) > 0 && fields == nil {
		return nil, ErrRange
	}
	wanted := map[string]bool{}
	for _, field := range fields {
		wanted[field] = true
	}
	paths := make([]string, 0, len(textFields))
	for _, field := range textFields {
		if field == "" || len(field) > 128 || strings.ContainsAny(field, "/~") || wanted[field] {
			return nil, ErrPath
		}
		wanted[field] = true
		paths = append(paths, path+"/"+field)
	}
	return paths, nil
}

// Select narrows one object's direct children before the budget traversal.
// It never visits hidden sibling subtrees. Missing requested children remain
// absent, since one renderer may use the same request for successive states.
func Select(path string, fields []string, read func(string, int, int) (Page, error)) (func(string, int, int) (Page, error), error) {
	if fields == nil {
		return read, nil
	}
	if len(fields) == 0 || len(fields) > 64 {
		return nil, ErrRange
	}
	wanted := map[string]bool{}
	for _, field := range fields {
		if field == "" || len(field) > 128 || strings.ContainsAny(field, "/~") || wanted[field] {
			return nil, ErrPath
		}
		wanted[field] = true
	}
	return func(request string, offset, limit int) (Page, error) {
		if request != path {
			return read(request, offset, limit)
		}
		page, err := read(request, 0, 64)
		if err != nil {
			return page, err
		}
		if page.Field.Kind != "object" || page.HasMore {
			return Page{}, ErrRange
		}
		children := make([]Field, 0, len(fields))
		for _, child := range page.Children {
			if wanted[strings.TrimPrefix(child.Path, path+"/")] {
				children = append(children, child)
			}
		}
		if offset < 0 || offset > len(children) || limit < 1 {
			return Page{}, ErrRange
		}
		page.Field.Count = len(children)
		page.Next = min(len(children), offset+limit)
		page.HasMore = page.Next < len(children)
		page.Children = children[offset:page.Next]
		return page, nil
	}, nil
}
