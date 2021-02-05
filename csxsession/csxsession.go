package csxsession

import (
	"context"
	"errors"
	"net/http"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"github.com/gorilla/sessions"
)

var client *goredis.Client

// SessionManager is a new base struct for management sessions
type SessionManager struct {
	client  *goredis.Client
	config  *Config
	options sessions.Options
	// key prefix with which the session will be stored
	keyPrefix string
	// key generator
	keyGen KeyGenFunc
	// session serializer
	serializer SessionSerializer
}

// KeyGenFunc defines a function used by store to generate a key
type KeyGenFunc func() (string, error)

// SessionSerializer provides an interface for serialize/deserialize a session
type SessionSerializer interface {
	Serialize(s *sessions.Session) ([]byte, error)
	Deserialize(b []byte, s *sessions.Session) error
	SerializePartial(d []byte, s *sessions.Session, key string, value interface{}) error
	DeserializePartial(d []byte, s *sessions.Session, key string) error
}

//Config is using for config session params
type Config struct {
	TTL time.Duration
}

// NewSessionManager create new CsxSession instance
func NewSessionManager(config *Config) *SessionManager {
	new := SessionManager{}
	new.client = client
	new.config = config
	return &new
}

// Start starts new session
func (mgr *SessionManager) Start(ctx context.Context, r *http.Request, w http.ResponseWriter, session *sessions.Session) error {
	// Delete if max-age is <= 0
	if session.Options.MaxAge <= 0 {
		if err := mgr.delete(r.Context(), session); err != nil {
			return err
		}
		http.SetCookie(w, sessions.NewCookie(session.Name(), "", session.Options))
		return nil
	}

	if session.ID == "" {
		id, err := mgr.keyGen()
		if err != nil {
			return errors.New("redisstore: failed to generate session id")
		}
		session.ID = id
	}
	if err := mgr.save(r.Context(), session); err != nil {
		return err
	}

	http.SetCookie(w, sessions.NewCookie(session.Name(), session.ID, session.Options))
	return nil
}

// Get retrieves value by key
func (mgr *SessionManager) Get(ctx context.Context, key string) (string, error) {
	val, err := mgr.client.Get(ctx, key).Result()
	if err != nil {
		return val, err
	}
	return val, nil
}

// Set sets value by key
func (mgr *SessionManager) Set(ctx context.Context, key string, value interface{}) error {
	err := mgr.client.Set(ctx, key, value, mgr.config.TTL).Err()
	if err != nil {
		return err
	}
	return nil
}

// Delete deletes value by key
func (mgr *SessionManager) Delete(ctx context.Context, key string) error {
	err := mgr.client.Del(ctx, key).Err()
	if err != nil {
		return err
	}
	return nil
}

// Close closes current session
func (mgr *SessionManager) Close(ctx context.Context) error {
	return nil
}

// save writes session in Redis
func (mgr *SessionManager) save(ctx context.Context, session *sessions.Session) error {

	b, err := mgr.serializer.Serialize(session)
	if err != nil {
		return err
	}

	return mgr.client.Set(ctx, mgr.keyPrefix+session.ID, b, time.Duration(session.Options.MaxAge)*time.Second).Err()
}

// load reads session from Redis
func (mgr *SessionManager) load(ctx context.Context, session *sessions.Session) error {

	cmd := mgr.client.Get(ctx, mgr.keyPrefix+session.ID)
	if cmd.Err() != nil {
		return cmd.Err()
	}

	b, err := cmd.Bytes()
	if err != nil {
		return err
	}

	return mgr.serializer.Deserialize(b, session)
}

// delete deletes session in Redis
func (mgr *SessionManager) delete(ctx context.Context, session *sessions.Session) error {
	return mgr.client.Del(ctx, mgr.keyPrefix+session.ID).Err()
}

// Init initializes redis client for store to work with
func Init(connCfg *ConnectionConfig) {
	client = goredis.NewClient(GetRedisConfigOptions(connCfg))
}
