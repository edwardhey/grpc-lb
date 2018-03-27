package main

import (
	"log"

	grpclb "github.com/edwardhey/grpc-lb"
	"github.com/edwardhey/grpc-lb/examples/proto"
	"github.com/edwardhey/grpc-lb/registry/consul"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func main() {
	r := consul.NewResolver("test", "http://120.24.44.201:8500")
	b := grpclb.NewBalancer(r, grpclb.NewRandomSelector())
	c, err := grpc.Dial("", grpc.WithInsecure(), grpc.WithBalancer(b))
	if err != nil {
		log.Printf("grpc dial: %s", err)
		return
	}
	defer c.Close()

	client := proto.NewTestClient(c)

	resp, err := client.Say(context.Background(), &proto.SayReq{Content: "consul"})
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf(resp.Content)

}
