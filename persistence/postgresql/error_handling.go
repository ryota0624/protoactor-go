package postgresql

import (
	"github.com/asynkron/protoactor-go/actor"
	"log/slog"
)

type PersistenceErrorOccurred struct {
	*PersistenceError
}

func HandlePersistenceError() actor.ReceiverMiddleware {
	return func(next actor.ReceiverFunc) actor.ReceiverFunc {
		return func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) {
			defer func() {
				recovered := recover()
				if err, ok := recovered.(error); ok && IsPersistenceError(err) {
					if envelope.Sender != nil {
						c.ActorSystem().Root.RequestWithCustomSender(c.Self(), &PersistenceErrorOccurred{err.(*PersistenceError)}, envelope.Sender)
					}
					return
				}

				if recovered != nil {
					c.ActorSystem().Logger().Debug("delegate panic to supervisor", slog.Any("errors", recovered))
					panic(recovered)
				}
			}()
			next(c, envelope)
		}
	}
}
