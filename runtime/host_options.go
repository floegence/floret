package runtime

import (
	"errors"
)

func (options turnExecutionOptions) validate() error {
	if !options.initialized {
		return errors.New("turn execution options require an immutable Agent")
	}
	return validateProviderHostOptions(options.config, options.modelGateway, options.modelGatewayIdentity, options.modelGatewayCapabilities, options.loopLimits, options.threadTitleMode)
}

func (options threadCompactionOptions) validate() error {
	if !options.initialized {
		return errors.New("thread compaction options require an immutable Agent")
	}
	return validateProviderHostOptions(options.config, options.modelGateway, options.modelGatewayIdentity, options.modelGatewayCapabilities, options.loopLimits, ThreadTitleModeHostOwned)
}

func (options subAgentOptions) validate() error {
	if !options.initialized {
		return errors.New("subagent options require an immutable Agent")
	}
	if options.subAgentRunTimeout < 0 {
		return errors.New("subagent run timeout cannot be negative")
	}
	return validateProviderHostOptions(options.config, options.modelGateway, options.modelGatewayIdentity, options.modelGatewayCapabilities, options.loopLimits, options.threadTitleMode)
}

func validateProviderHostOptions(cfg runtimeConfig, gateway modelGateway, identity modelGatewayIdentity, capabilities modelGatewayCapabilities, limits LoopLimits, titleMode ThreadTitleMode) error {
	if gateway == nil {
		return errors.New("provider gateway is required")
	}
	if err := capabilities.validate(gateway); err != nil {
		return err
	}
	if _, err := normalizeModelGatewayIdentity(identity); err != nil {
		return err
	}
	if _, err := resolveModelGatewayHostConfig(cfg, identity); err != nil {
		return err
	}
	if limits.MaxEmptyProviderRetries < 0 || limits.NoProgressLimit < 0 || limits.DuplicateToolLimit < 0 || limits.WallTime < 0 {
		return errors.New("loop limits cannot be negative")
	}
	_, err := normalizeThreadTitleMode(titleMode)
	return err
}
