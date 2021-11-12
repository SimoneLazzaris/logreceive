package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc/metadata"
	"log"
)

type immucfg struct {
	IpAddr   string
	Port     int
	Username string
	Password string
	DBName   string
}

func connect(ctx context.Context, config immucfg) (context.Context, immuclient.ImmuClient) {
	opts := immuclient.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)

	client, err := immuclient.NewImmuClient(opts)
	if err != nil {
		log.Fatalln("Failed to connect. Reason:", err.Error())
	}

	login, err := client.Login(ctx, []byte(config.Username), []byte(config.Password))
	if err != nil {
		log.Fatalln("Failed to login. Reason:", err.Error())
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", login.GetToken()))

	udr, err := client.UseDatabase(ctx, &schema.Database{DatabaseName: config.DBName})
	if err != nil {
		log.Fatalln("Failed to use the database. Reason:", err)
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", udr.GetToken()))
	return ctx, client
}

func pushmsg(ctx context.Context, client immuclient.ImmuClient, msgs []lambdaMsg) error {
	l := len(msgs)
	ops := make([]*schema.Op, 2*l)
	for i, msg := range msgs {
		key := []byte(fmt.Sprintf("L:%s/%s@%s:%f",
			msg.Kubernetes.Namespace,
			msg.Kubernetes.PodName,
			msg.Kubernetes.Host,
			msg.Date,
		))
		ptr := []byte(fmt.Sprintf("T:%f", msg.Date))
		val, _ := json.Marshal(msg)
		ops[2*i] = &schema.Op{
			Operation: &schema.Op_Kv{
				Kv: &schema.KeyValue{
					Key:   key,
					Value: val,
				},
			},
		}
		ops[2*i+1] = &schema.Op{
			Operation: &schema.Op_Ref{
				Ref: &schema.ReferenceRequest{
					Key:           ptr,
					ReferencedKey: key,
				},
			},
		}
	}
	opList := &schema.ExecAllRequest{Operations: ops}
	ret, err := client.ExecAll(ctx, opList)
	if err != nil {
		log.Printf("Error sending data to immudb: %s", err.Error())
		return err
	}
	log.Printf("Sent %d messages, tx %d", l, ret.Id)
	return nil
}
