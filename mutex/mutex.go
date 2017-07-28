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
  NEXT_REQUEST_WAIT_SECOND = 1
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

func (m *Mutex) TryLock() (err errors) {
  m.Mutex.Lock()
  return m.tryLock()
}

func (m *Mutex) Lock() (err errors) {
  m.Mutex.Lock()
  for retry := 0; retry <= HTTP_RETRY_TIMES; retry++ {
    if err = m.lock(); err == nil {
      return nil
    }
    time.Sleep(NEXT_REQUEST_WAIT_SECOND * time.Second)
  }
  return err
}

func (m *Mutex) tryLock() (err errors) {
  options := &client.SetOptions {
    PrevExist: client.PrevNoExist,
    TTL: m.TTL
  }

  if resp, err := m.KeysAPI.set(m.Context, m.LockPath, m.UserId, options); err != nil {
    return err
  } else {
    return nil
  }
}

func (m *Mutex) lock() (err errors) {
  options := &client.SetOptions {
    PrevExist: client.PrevNoExist,
    TTL: m.TTL
  }

  resp, err := m.KeysAPI.set(m.Context, m.LockPath, m.UserId, options)
  // transfer err to client.Error
  e, ok := err.(client.Error)
  if !ok {
    return err
  }

  if e.Code != client.ErrorCodeNodeExist {
    // this key not existed, but create failed, return error
    return err
  }

  // this key has existed, watch this key
  if resp, err = m.KeysAPI.Get(m.Context, m.LockPath, nil); err != nil {
    return err
  }

  watcherOptions := &client.WatcherOptions {
    AfterIndex: resp.Index,
    Recursive:  false
  }

  watcher := m.KeysAPI.Watcher(m.LockPath, watcherOptions)
  for {
    if resp, err := watcher.Next(m.Context); err != nil {
      return err
    }
    if resp.Action == "delete" || resp.Action == "expire" {
      // try to create this key again
      if resp, err := m.KeysAPI.set(m.Context, m.LockPath, m.UserId, options); err != nil {
        return err
      } else {
        return nil
      }
    }
  }
}

func (m *Mutex) UnLock() (err errors) {
  defer m.Mutex.UnLock()
  for retry := 0; retry < HTTP_RETRY_TIMES; retry++ {
    resp, err := m.KeysAPI.Delete(m.Context, m.LockPath, nil)
    if err != nil {
      time.Sleep(NEXT_REQUEST_WAIT_SECOND * time.Second)
    } else {
      return nil
    }
  }
  return err
}
