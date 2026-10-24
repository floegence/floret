package agentharness

import "github.com/floegence/floret/v7/internal/sessiontree"

type threadActivityIdentityProjection struct {
	sessiontree.ActivityIdentityProjection
}

func (p *threadActivityIdentityProjection) observe(e sessiontree.Entry) { p.Observe(e) }
func (p *threadActivityIdentityProjection) validate() error             { return p.Validate() }
