package provider

import (
	"encoding/json"
	"errors"
)

// Opaque history binds image content to its source descriptor and digest. Bytes
// belong only to the prepared request and are resolved again under host authority
// on subsequent requests; they are never copied into durable provider state.
type deepSeekImageReference struct {
	Attachment Attachment `json:"attachment"`
	SHA256     string     `json:"sha256"`
}

type deepSeekImageContent struct {
	Type     string                  `json:"type"`
	Text     string                  `json:"text,omitempty"`
	Image    *deepSeekImageReference `json:"image,omitempty"`
	ImageURL string                  `json:"image_url,omitempty"`
}

func expandDeepSeekImages(input []json.RawMessage, images map[string]string) ([]json.RawMessage, error) {
	for i, raw := range input {
		var item struct {
			Type    string          `json:"type"`
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
			Output  json.RawMessage `json:"output"`
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, err
		}
		isMessageImage := item.Type == "message" && item.Role == "user"
		isToolOutputImage := item.Type == "function_call_output"
		content := item.Content
		if isToolOutputImage {
			content = item.Output
		}
		if !isMessageImage && !isToolOutputImage || len(content) == 0 || content[0] != '[' {
			continue
		}
		var parts []deepSeekImageContent
		if isToolOutputImage {
			var output []deepSeekImageContent
			if err := json.Unmarshal(content, &output); err != nil {
				return nil, err
			}
			parts = output
		} else if err := json.Unmarshal(content, &parts); err != nil {
			return nil, err
		}
		for j, part := range parts {
			switch part.Type {
			case "input_text":
			case "image_reference":
				if part.Image == nil {
					return nil, errors.New("DeepSeek image reference is incomplete")
				}
				uri, ok := images[deepSeekHash(part.Image)]
				if !ok {
					return nil, errors.New("DeepSeek image reference has no authorized request content")
				}
				parts[j] = deepSeekImageContent{Type: "input_image", ImageURL: uri}
			default:
				return nil, errors.New("unsupported DeepSeek user content in response state")
			}
		}
		var err error
		if isToolOutputImage {
			var output struct {
				Type   string `json:"type"`
				CallID string `json:"call_id"`
			}
			if err := json.Unmarshal(raw, &output); err != nil {
				return nil, err
			}
			input[i], err = json.Marshal(deepSeekInput{Type: "function_call_output", CallID: output.CallID, Output: parts})
		} else {
			input[i], err = json.Marshal(deepSeekInput{Type: "message", Role: "user", Content: parts})
		}
		if err != nil {
			return nil, err
		}
	}
	return input, nil
}
