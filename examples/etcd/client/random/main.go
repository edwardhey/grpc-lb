package main

import (
	etcd "github.com/coreos/etcd/client"
	grpclb "github.com/edwardhey/grpc-lb"
	"github.com/edwardhey/grpc-lb/examples/proto"
	registry "github.com/edwardhey/grpc-lb/registry/etcd"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"time"
)

func main() {
	etcdConfg := etcd.Config{
		Endpoints: []string{"http://120.24.44.201:2379"},
	}
	r := registry.NewResolver("/grpc-lb", "test", etcdConfg)
	b := grpclb.NewBalancer(r, grpclb.NewRandomSelector())
	c, err := grpc.Dial("", grpc.WithInsecure(), grpc.WithBalancer(b))
	if err != nil {
		log.Printf("grpc dial: %s", err)
		return
	}
	defer c.Close()

	client := proto.NewTestClient(c)

	for i := 0; i < 5; i++ {
		resp, err := client.Say(context.Background(), &proto.SayReq{Content: "random"})
		if err != nil {
			log.Println(err)
			time.Sleep(time.Second)
			continue
		}
		log.Printf(resp.Content)
	}
}
