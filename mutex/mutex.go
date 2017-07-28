package mutex

import (
	"github.com/etcd/client"
  "golang.org/x/net/context"
  "time"
  "errors"
)

var (
  HTTP_RETRY_TIMES = 3
  DEFAULT_TTL = 30
)

type Mutex struct {
	LockPath string         // the key stored in etcd
	UserId   string         // the identifier of user
	Client   client.Client  // etcd client
	KeysAPI  client.KeysAPI // the interface of etcd client's method (set | get | watch)
  Context context.Context // context interface
  TTL time.Duration // the life cycle of this key
  Mutex *sync.Mutex // local mutex
}

func New(lock_path string, endpoints[]string, ttl int) (*Mutex, errors) {
  if (len(lock_path) == 0 || lock_path[0] != '/') {
    return nil, errors.New("lock_path can not be null and should be started with '/'")
  }

  if (len(endpoints) == 0) {
    return nil, errors.New("endpoints can not be null")
  }

  if (ttl < 0) {
    return nil, errors.New("ttl must be a postive number")
  } else if (ttl == 0) {
    ttl = DEFAULT_TTL
  }

  config := client.Config {
    Endpoints:  endpoints,
    Transport:  client.DefaultTransport,
    HeaderTimeoutPerRequest:  time.Second
  }

  if c, err := client.New(config); err != nil {
    return nil, errors.New("failed to create etcd client")
  }

  if hostname, err := os.Hostname(); err != nil {
    return nil, errors.New("failed to get local hostname")
  }

  return &Mutex{
    LockPath: key,
    UserId: hostname,
    Client: c,
    KeysAPI:  client.NewKeysAPI(c),
    Context: context.TODO(),
    TTL:  ttl,
    Mutex:  new(sync.Mutex)
  }
}
