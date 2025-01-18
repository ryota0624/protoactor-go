package postgresql

import (
	goCtx "context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asynkron/protoactor-go/persistence"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"log/slog"
	"sync"
)

type PersistenceError struct {
	Cause error
}

func (e *PersistenceError) Error() string {
	return fmt.Sprintf("persistence error: %v", e.Cause)
}

func (e *PersistenceError) Unwrap() error {
	return e.Cause
}

func newPersistenceError(err error) *PersistenceError {
	return &PersistenceError{Cause: err}
}

func IsPersistenceError(err error) bool {
	_, ok := err.(*PersistenceError)
	return ok
}

type ProviderState struct {
	connPool         *pgxpool.Pool
	logger           *slog.Logger
	wg               sync.WaitGroup
	snapshotInterval int
}

func NewProviderState(conn *pgxpool.Pool, logger *slog.Logger, snapshotInterval int) *ProviderState {
	return &ProviderState{connPool: conn, logger: logger, snapshotInterval: snapshotInterval}
}

func (s *ProviderState) GetSnapshot(actorName string) (snapshot interface{}, eventIndex int, ok bool) {
	rows, err := s.connPool.Query(goCtx.Background(),
		"SELECT actor_name, snapshot, message_type, snapshot_index FROM snapshots WHERE actor_name = $1 ORDER BY snapshot_index DESC LIMIT 1", actorName,
	)
	if err != nil {
		s.logger.Error("Error getting snapshot", slog.Any("error", err))
		return nil, 0, false
	}
	snapshotRow, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[snapshotsTableRow])
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, 0, false
	} else if err != nil {
		s.logger.Error("Error getting snapshot", slog.Any("error", err))
		return nil, 0, false
	}
	snapshot, err = unmarshalProtoMessage(snapshotRow.Snapshot, snapshotRow.MessageType)
	if err != nil {
		s.logger.Error("Error unmarshalling snapshot", slog.Any("error", err))
		return nil, 0, false
	}

	return snapshot, snapshotRow.SnapshotIndex, true
}

func (s *ProviderState) PersistSnapshot(actorName string, snapshotIndex int, snapshot proto.Message) {
	s.wg.Add(1)
	defer s.wg.Done()
	bytes, err := json.Marshal(snapshot)
	if err != nil {
		panic(newPersistenceError(fmt.Errorf("error marshalling snapshot: %w", err)))
	}
	_, err = s.connPool.Exec(goCtx.Background(),
		`
INSERT INTO snapshots (actor_name, snapshot, message_type, snapshot_index) 
	VALUES ($1, $2, $3, $4)
  ON CONFLICT ON CONSTRAINT snapshots_pk DO UPDATE SET snapshot=$2, message_type=$3, snapshot_index=$4;
`,
		actorName, bytes, proto.MessageName(snapshot), snapshotIndex)
	if err != nil {
		panic(newPersistenceError(fmt.Errorf("error persisting snapshot: %w", err)))
	}
}

func (s *ProviderState) DeleteSnapshots(actorName string, inclusiveToIndex int) {
	_, err := s.connPool.Exec(goCtx.Background(),
		"DELETE FROM snapshots WHERE actor_name = $1 AND snapshot_index <= $2",
		actorName, inclusiveToIndex)
	if err != nil {
		s.logger.Error("Error deleting snapshots", slog.Any("error", err))
	}
}

func (s *ProviderState) GetEvents(actorName string, eventIndexStart int, eventIndexEnd int, callback func(e interface{})) {
	if eventIndexEnd == 0 {
		eventIndexEnd = 9999
	}
	rows, err := s.connPool.Query(goCtx.Background(),
		`SELECT
    				actor_name, event, message_type, event_index 
					FROM event_journals 
					WHERE 
					  actor_name = $1 AND event_index >= $2 AND event_index <= $3 
					ORDER BY event_index
					`,
		actorName, eventIndexStart, eventIndexEnd)
	if err != nil {
		s.logger.Error("Error getting events", slog.Any("error", err))
		return
	}
	eventRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[journalEventsTableRow])
	events := make([]proto.Message, len(eventRows))
	for i, row := range eventRows {
		event, err := unmarshalProtoMessage(row.Event, row.MessageType)
		if err != nil {
			s.logger.Error("Error unmarshalling event", slog.Any("error", err))
			return
		}
		events[i] = event
	}

	for _, event := range events {
		callback(event)
	}
}

func (s *ProviderState) PersistEvent(actorName string, eventIndex int, event proto.Message) {
	s.wg.Add(1)
	defer s.wg.Done()
	bytes, err := json.Marshal(event)
	if err != nil {
		panic(newPersistenceError(fmt.Errorf("error marshalling event: %w", err)))
	}
	_, err = s.connPool.Exec(goCtx.Background(),
		`INSERT INTO 
    				event_journals (actor_name, event, message_type, event_index) 
					VALUES ($1, $2, $3, $4)`,
		actorName,
		bytes,
		proto.MessageName(event),
		eventIndex,
	)
	if err != nil {
		panic(newPersistenceError(fmt.Errorf("error persisting event: %w", err)))
	}
}

func (s *ProviderState) DeleteEvents(actorName string, inclusiveToIndex int) {
	_, err := s.connPool.Exec(goCtx.Background(),
		"DELETE FROM event_journals WHERE actor_name = $1 AND event_index <= $2",
		actorName, inclusiveToIndex)
	if err != nil {
		s.logger.Error("Error deleting events", slog.Any("error", err))
	}
}

func (s *ProviderState) Restart() {
	s.wg.Wait()
}

func (s *ProviderState) GetSnapshotInterval() int {
	return s.snapshotInterval
}

var _ persistence.ProviderState = &ProviderState{}

type snapshotsTableRow struct {
	ActorName     string `db:"actor_name"`
	Snapshot      []byte `db:"snapshot"`
	SnapshotIndex int    `db:"snapshot_index"`
	MessageType   string `db:"message_type"`
}

type journalEventsTableRow struct {
	ActorName   string `db:"actor_name"`
	Event       []byte `db:"event"`
	EventIndex  int    `db:"event_index"`
	MessageType string `db:"message_type"`
}

func unmarshalProtoMessage(bytes []byte, messageTypeName string) (proto.Message, error) {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(messageTypeName))
	if err != nil {
		return nil, fmt.Errorf("error finding message type %s: %w", messageTypeName, err)
	}

	pm := mt.New().Interface()
	err = json.Unmarshal(bytes, pm)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling message %s: %w", messageTypeName, err)
	}
	return pm, nil
}
