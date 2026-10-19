package searchcap

// ToolWebSearch and wire shapes are protocol identifiers, not runtime policy.
const ToolWebSearch = "web_search"

type HostedWireShape string

const WireShapeAnthropicServerWebSearch HostedWireShape = "anthropic_server_web_search"
const WireShapeDeepSeekResponsesWebSearch HostedWireShape = "deepseek_responses_web_search"
