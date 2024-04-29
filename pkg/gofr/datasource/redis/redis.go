package redis

import (
	"context"
	"fmt"
	otel "github.com/redis/go-redis/extra/redisotel/v9"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"gofr.dev/pkg/gofr/config"
	"gofr.dev/pkg/gofr/datasource"
)

const (
	redisPingTimeout = 5 * time.Second
	defaultRedisPort = 6379
)

type Config struct {
	HostName string
	Port     int
	Options  *redis.Options
}

type Redis struct {
	*redis.Client
	logger datasource.Logger
	config *Config
}

// NewClient return a redis client if connection is successful based on Config.
// In case of error, it returns an error as second parameter.
func NewClient(c config.Config, logger datasource.Logger, metrics Metrics) *Redis {
	var redisConfig = &Config{}

	if redisConfig.HostName = c.Get("REDIS_HOST"); redisConfig.HostName == "" {
		return nil
	}

	port, err := strconv.Atoi(c.Get("REDIS_PORT"))
	if err != nil {
		port = defaultRedisPort
	}

	redisConfig.Port = port

	gofrRedis := &Redis{Client: nil, config: redisConfig, logger: logger}

	options := new(redis.Options)

	if options.Addr == "" {
		options.Addr = fmt.Sprintf("%s:%d", gofrRedis.config.HostName, gofrRedis.config.Port)
	}

	redisConfig.Options = options

	rc := redis.NewClient(gofrRedis.config.Options)
	rc.AddHook(&redisHook{logger: gofrRedis.logger, metrics: metrics})

	gofrRedis.Client = rc

	ctx, cancel := context.WithTimeout(context.TODO(), redisPingTimeout)
	defer cancel()

	if err := otel.InstrumentTracing(rc); err != nil {
		logger.Errorf("could not add tracing instrumentation, error: %s", err)
	}

	if err := rc.Ping(ctx).Err(); err != nil {
		logger.Errorf("could not connect to redis at %s:%d. error: %s", gofrRedis.config.HostName, gofrRedis.config.Port, err)
	} else {
		logger.Logf("connected to redis at %s:%d", redisConfig.HostName, redisConfig.Port)
	}

	go retryConnection(gofrRedis)

	return &Redis{Client: rc, config: redisConfig, logger: logger}
}

func retryConnection(rc *Redis) {
	const connRetryFrequencyInSeconds = 10

	for {
		ctx, cancel := context.WithTimeout(context.TODO(), redisPingTimeout)
		defer cancel()

		if rc.Client.Ping(ctx).Err() != nil {
			rc.logger.Log("retrying REDIS connection")

			for {
				if err := rc.Client.Ping(ctx).Err(); err != nil {
					rc.logger.Debugf("could not connect to redis at %s:%d. error: %s", rc.config.HostName, rc.config.Port, err)

					time.Sleep(connRetryFrequencyInSeconds * time.Second)
				} else {
					rc.logger.Logf("connected to redis at %v:%v", rc.config.HostName, rc.config.Port)

					break
				}
			}
		}

		time.Sleep(connRetryFrequencyInSeconds * time.Second)
	}
}

// TODO - if we make Redis an interface and expose from container we can avoid c.Redis(c, command) using methods on c and still pass c.
// type Redis interface {
//	Get(string) (string, error)
// }
