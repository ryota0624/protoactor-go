package postgresql

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/persistence"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Provider struct {
	pool        *pgxpool.Pool
	actorSystem *actor.ActorSystem
}

func NewProvider(pool *pgxpool.Pool, actorSystem *actor.ActorSystem) *Provider {
	return &Provider{pool: pool, actorSystem: actorSystem}
}

func (p *Provider) GetState() persistence.ProviderState {
	return NewProviderState(p.pool, p.actorSystem.Logger())
}

var _ persistence.Provider = &Provider{}
