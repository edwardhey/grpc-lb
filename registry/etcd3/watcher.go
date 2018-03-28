package etcd

import (
	"encoding/json"
	"time"

	etcd3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/naming"
)

// EtcdWatcher is the implementation of grpc.naming.Watcher
type EtcdWatcher struct {
	key      string
	client   *etcd3.Client
	updateCH chan *naming.Update
	ctx      context.Context
	cancel   context.CancelFunc
	hbTimes  map[string]time.Time
}

func (w *EtcdWatcher) Close() {
	w.cancel()
}

func newEtcdWatcher(key string, cli *etcd3.Client) naming.Watcher {
	ctx, cancel := context.WithCancel(context.Background())
	w := &EtcdWatcher{
		key:      key,
		client:   cli,
		ctx:      ctx,
		updateCH: make(chan *naming.Update, 1000),
		hbTimes:  make(map[string]time.Time),
		cancel:   cancel,
	}

	go func() {
		rch := w.client.Watch(w.ctx, w.key, etcd3.WithPrefix())
		for wresp := range rch {
			for _, ev := range wresp.Events {
				key := string(ev.Kv.Key)
				switch ev.Type {
				case mvccpb.PUT:
					nodeData := NodeData{}
					err := json.Unmarshal([]byte(ev.Kv.Value), &nodeData)
					if err != nil {
						grpclog.Println("Parse node data error:", err)
						continue
					}
					t, ok := w.hbTimes[key]
					if !ok || time.Now().Sub(t) > time.Second*300 {
						w.updateCH <- &naming.Update{Op: naming.Add, Addr: nodeData.Addr, Metadata: &nodeData.Metadata}
						w.hbTimes[key] = time.Now()
					}
				case mvccpb.DELETE:
					nodeData := NodeData{
						Addr: key,
					}
					delete(w.hbTimes, key)
					w.updateCH <- &naming.Update{Op: naming.Delete, Addr: nodeData.Addr, Metadata: &nodeData.Metadata}
				}

			}
		}
	}()
	return w
}

func (w *EtcdWatcher) Next() (updates []*naming.Update, err error) {
	return append(updates, <-w.updateCH), nil
}

func extractAddrs(resp *etcd3.GetResponse) []NodeData {
	addrs := []NodeData{}

	if resp == nil || resp.Kvs == nil {
		return addrs
	}

	for i := range resp.Kvs {
		if v := resp.Kvs[i].Value; v != nil {
			nodeData := NodeData{}
			err := json.Unmarshal(v, &nodeData)
			if err != nil {
				grpclog.Println("Parse node data error:", err)
				continue
			}
			addrs = append(addrs, nodeData)
		}
	}

	return addrs
}
