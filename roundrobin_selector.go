package grpclb

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type RoundRobinSelector struct {
	BaseSelector
	next int
}

func NewRoundRobinSelector() Selector {
	return &RoundRobinSelector{
		next:         0,
		BaseSelector: BaseSelector{addrMap: make(map[string]*AddrInfo)},
	}
}

func (r *RoundRobinSelector) Get(ctx context.Context) (addr grpc.Address, err error) {
	if len(r.addrs) == 0 {
		err = AddrListEmptyErr
		return
	}

	if r.next >= len(r.addrs) {
		r.next = 0
	}
	next := r.next
	for {
		a := r.addrs[next]
		next = (next + 1) % len(r.addrs)
		if addrInfo, ok := r.addrMap[a]; ok {
			if addrInfo.Connected {
				addr = addrInfo.Addr
				addrInfo.Load++
				r.next = next
				return
			}
			if next == r.next {
				// Has iterated all the possible address but none is connected.
				addr = addrInfo.Addr
				addrInfo.Load++
				r.next = next
				return
			}
		}
	}
}
