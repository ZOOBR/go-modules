package csxsession

import (
	"context"
	"crypto/tls"
	"time"

	goredis "github.com/go-redis/redis/v8"
)

// ConnectionConfig is using for config redis client connection
type ConnectionConfig struct {
	Addr      string
	Password  string
	DB        int
	OnConnect func(ctx context.Context, cn *goredis.Conn) error
	// Maximum number of retries before giving up.
	// Default is to not retry failed commands.
	MaxRetries int
	// Minimum backoff between each retry.
	// Default is 8 milliseconds; -1 disables backoff.
	MinRetryBackoff time.Duration
	// Maximum backoff between each retry.
	// Default is 512 milliseconds; -1 disables backoff.
	MaxRetryBackoff time.Duration

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	DialTimeout time.Duration
	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking.
	// Default is 3 seconds.
	ReadTimeout time.Duration
	// Timeout for socket writes. If reached, commands will fail
	// with a timeout instead of blocking.
	// Default is ReadTimeout.
	WriteTimeout time.Duration

	// Maximum number of socket connections.
	// Default is 10 connections per every CPU as reported by runtime.NumCPU.
	PoolSize int
	// Amount of time client waits for connection if all connections
	// are busy before returning an error.
	// Default is ReadTimeout + 1 second.
	PoolTimeout time.Duration
	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// Default is 5 minutes.
	IdleTimeout time.Duration
	// Frequency of idle checks.
	// Default is 1 minute.
	// When minus value is set, then idle check is disabled.
	IdleCheckFrequency time.Duration

	// TLS Config to use. When set TLS will be negotiated.
	TLSConfig *tls.Config
}

// GetRedisConfigOptions returns object with connection options for redis client
func GetRedisConfigOptions(connCfg *ConnectionConfig) *goredis.Options {
	return &goredis.Options{
		Addr:               connCfg.Addr,
		Password:           connCfg.Password,
		OnConnect:          connCfg.OnConnect,
		DB:                 connCfg.DB,
		MaxRetries:         connCfg.MaxRetries,
		MinRetryBackoff:    connCfg.MinRetryBackoff,
		MaxRetryBackoff:    connCfg.MaxRetryBackoff,
		DialTimeout:        connCfg.DialTimeout,
		ReadTimeout:        connCfg.ReadTimeout,
		WriteTimeout:       connCfg.WriteTimeout,
		PoolSize:           connCfg.PoolSize,
		PoolTimeout:        connCfg.PoolTimeout,
		IdleTimeout:        connCfg.IdleTimeout,
		IdleCheckFrequency: connCfg.IdleCheckFrequency,
		TLSConfig:          connCfg.TLSConfig,
	}
}
