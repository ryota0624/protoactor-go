package postgresql

import (
	context2 "context"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/persistence"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
)

type Provider struct {
	connConfig  *pgxpool.Config
	actorSystem *actor.ActorSystem
}

func NewProvider(connConfig *pgxpool.Config, actorSystem *actor.ActorSystem) *Provider {
	return &Provider{connConfig: connConfig, actorSystem: actorSystem}
}

func (p *Provider) GetState() persistence.ProviderState {
	pool, err := pgxpool.NewWithConfig(context2.Background(), p.connConfig)
	if err != nil {
		p.actorSystem.Logger().Error("failed to create pgx pool", slog.Any("error", err))
		panic(err)
	}

	return NewProviderState(pool, p.actorSystem.Logger())
}

var _ persistence.Provider = &Provider{}
