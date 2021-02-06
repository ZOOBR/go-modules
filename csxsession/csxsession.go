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

// Manager is main instance for working with sessions
var Manager *SessionManager

// DefaultCookieID is using for default cookie id
const DefaultCookieID = "0NpqOh6INMv_8OVgOcjzIY8EBcIk-aV3GOYeuL3dHI4"

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
	SerializePartial(d []byte, value []byte, s *sessions.Session, keys ...string) ([]byte, error)
	DeserializePartial(d []byte, s *sessions.Session, keys ...string) error
}

//Config is using for config session params
type Config struct {
	Session   *sessions.Options
	KeyPrefix string
	KeyGen    KeyGenFunc
	CookieID  string
}

// NewSessionManager create new SessionManager instance
func NewSessionManager(ctx context.Context, config *Config) (*SessionManager, error) {
	sm := &SessionManager{
		options: sessions.Options{
			Path:   "/",
			MaxAge: config.Session.MaxAge,
		},
		client:     client,
		config:     config,
		keyPrefix:  config.KeyPrefix,
		keyGen:     generateRandomKey,
		serializer: CsxSerializer{},
	}
	if config.KeyGen != nil {
		sm.keyGen = config.KeyGen
	}
	return sm, sm.client.Ping(ctx).Err()
}

// Get returns a session for the given name after adding it to the registry.
func (mgr *SessionManager) Get(r *http.Request, name string) (*sessions.Session, error) {
	if name == "" {
		if mgr.config.CookieID != "" {
			name = mgr.config.CookieID
		} else {
			name = DefaultCookieID
		}
	}
	return sessions.GetRegistry(r).Get(mgr, name)
}

// New returns a session for the given name without adding it to the registry.
func (mgr *SessionManager) New(r *http.Request, name string) (*sessions.Session, error) {

	session := sessions.NewSession(mgr, name)
	opts := mgr.options
	session.Options = &opts
	session.IsNew = true

	if name == "" {
		if mgr.config.CookieID != "" {
			name = mgr.config.CookieID
		} else {
			name = DefaultCookieID
		}
	}

	c, err := r.Cookie(name)
	if err != nil {
		return session, nil
	}
	session.ID = c.Value

	err = mgr.load(r.Context(), session)
	if err == nil {
		session.IsNew = false
	} else if err == goredis.Nil {
		err = nil // no data stored
	}
	return session, err
}

// Save adds a single session to the response.
func (mgr *SessionManager) Save(r *http.Request, w http.ResponseWriter, session *sessions.Session) error {
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

// GetValueByKey retrieves value by key
func (mgr *SessionManager) GetValueByKey(ctx context.Context, key string) (string, error) {
	val, err := mgr.client.Get(ctx, key).Result()
	if err != nil {
		return val, err
	}
	return val, nil
}

// SetValueByKey sets value by key
func (mgr *SessionManager) SetValueByKey(ctx context.Context, key string, value interface{}) error {
	err := mgr.client.Set(ctx, key, value, time.Duration(mgr.config.Session.MaxAge)*time.Second).Err()
	if err != nil {
		return err
	}
	return nil
}

// DeleteValueByKey deletes value by key
func (mgr *SessionManager) DeleteValueByKey(ctx context.Context, key string) error {
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

// SetKeyPrefix sets the key prefix to store session in Redis
func (mgr *SessionManager) SetKeyPrefix(keyPrefix string) {
	mgr.keyPrefix = keyPrefix
}

// SetKeyGen sets the key generator function
func (mgr *SessionManager) SetKeyGen(f KeyGenFunc) {
	mgr.keyGen = f
}

// save writes session in Redis
func (mgr *SessionManager) save(ctx context.Context, session *sessions.Session) error {
	b, err := mgr.serializer.Serialize(session)
	if err != nil {
		return err
	}
	return mgr.client.Set(ctx, mgr.keyPrefix+session.ID, b, time.Duration(mgr.config.Session.MaxAge)*time.Second).Err()
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
func Init(ctx context.Context, connCfg *ConnectionConfig, config *Config) error {
	client = goredis.NewClient(GetRedisConfigOptions(connCfg))
	if client == nil {
		return errors.New("error create new redis client")
	}
	sessionMgr, err := NewSessionManager(ctx, config)
	if err != nil {
		return err
	}
	Manager = sessionMgr
	return nil
}
