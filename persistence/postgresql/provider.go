package postgresql

import (
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/persistence"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Provider struct {
	pool             *pgxpool.Pool
	actorSystem      *actor.ActorSystem
	snapshotInterval int
}

func NewProvider(pool *pgxpool.Pool, actorSystem *actor.ActorSystem, snapshotInterval int) *Provider {
	return &Provider{pool: pool, actorSystem: actorSystem, snapshotInterval: snapshotInterval}
}

func (p *Provider) GetState() persistence.ProviderState {
	return NewProviderState(p.pool, p.actorSystem.Logger(), p.snapshotInterval)
}

var _ persistence.Provider = &Provider{}
