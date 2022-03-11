package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/lensesio/tableprinter"
	"github.com/streadway/amqp"
)

const (
	actionRepublishQueue = iota
	actionRepublishExchange
	actionSaveToFile
	actionDiscard
)

var errUsage = errors.New("usage: amqp-dead-letter <url> <queue>")

func main() {
	if err := run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 3 {
		return errUsage
	}

	url, queue := os.Args[1], os.Args[2]
	if url == "" {
		return errUsage
	}
	if queue == "" {
		return errUsage
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	return work(ctx, ch, queue)

}

func work(ctx context.Context, ch *amqp.Channel, queue string) error {
	for {
		select {
		case <-ctx.Done():
			return nil

		default:
			del, ok, err := ch.Get(queue, false)
			if err != nil {
				return err
			}
			if !ok {
				fmt.Println("no messages left")
				return nil
			}
			if err := process(ch, del); err != nil {
				return err
			}
		}
	}
}

func process(ch *amqp.Channel, del amqp.Delivery) error {
	fmt.Printf("MESSAGE %d (%d remaining)\n", del.DeliveryTag, del.MessageCount)
	printDelivery(os.Stdout, del)

	exchange := getHeader(del.Headers, "x-first-death-exchange")
	queue := getHeader(del.Headers, "x-first-death-queue")

	var options []string
	var actions []int

	if queue != "" {
		options = append(options, fmt.Sprintf("republish to queue %s", queue))
		actions = append(actions, actionRepublishQueue)
	}

	if exchange != "" {
		options = append(options, fmt.Sprintf("republish to exchange %s", exchange))
		actions = append(actions, actionRepublishExchange)
	}

	if del.MessageId != "" {
		options = append(options, fmt.Sprintf("save to file %s", filename(del)))
		actions = append(actions, actionSaveToFile)
	}

	options = append(options, "discard")
	actions = append(actions, actionDiscard)

	prompt := &survey.Select{
		Message: "choose an action",
		Options: options,
	}
	var answer int
	if err := survey.AskOne(prompt, &answer); err != nil {
		return err
	}

	switch actions[answer] {
	case actionRepublishQueue:
		if err := ch.Publish("", queue, false, false, publishing(del)); err != nil {
			return err
		}
		fmt.Println("republished directly to queue", queue)
		return del.Acknowledger.Ack(del.DeliveryTag, false)

	case actionRepublishExchange:
		if err := ch.Publish(exchange, del.RoutingKey, false, false, publishing(del)); err != nil {
			return err
		}
		fmt.Println("republished to exchange", exchange)
		return del.Acknowledger.Ack(del.DeliveryTag, false)

	case actionSaveToFile:
		if del.MessageId == "" {
			return errors.New("cannot save to file without message_id")
		}
		f, err := os.Create(filename(del))
		if err != nil {
			return err
		}
		if err := printDelivery(f, del); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		return del.Acknowledger.Ack(del.DeliveryTag, false)

	case actionDiscard:
		return del.Acknowledger.Ack(del.DeliveryTag, false)
	}

	return nil
}

func filename(del amqp.Delivery) string {
	return fmt.Sprintf("dead-letter-%s.txt", del.MessageId)
}

func printDelivery(f *os.File, del amqp.Delivery) error {
	if err := printProperties(f, del); err != nil {
		return err
	}
	if err := printHeaders(f, del); err != nil {
		return err
	}
	if err := printPayload(f, del); err != nil {
		return err
	}
	return nil
}

func publishing(del amqp.Delivery) amqp.Publishing {
	return amqp.Publishing{
		Headers:         del.Headers,
		ContentType:     del.ContentType,
		ContentEncoding: del.ContentEncoding,
		DeliveryMode:    del.DeliveryMode,
		Priority:        del.Priority,
		CorrelationId:   del.CorrelationId,
		ReplyTo:         del.ReplyTo,
		Expiration:      del.Expiration,
		MessageId:       del.MessageId,
		Timestamp:       del.Timestamp,
		Type:            del.Type,
		UserId:          del.UserId,
		AppId:           del.AppId,
		Body:            del.Body,
	}
}

func getHeader(table amqp.Table, key string) string {
	for k, v := range table {
		if k == key {
			if v, ok := v.(string); ok {
				return v
			}
			return ""
		}
	}
	return ""
}

type property struct {
	Name  string `header:"property"`
	Value string `header:"value"`
}

func printProperties(f *os.File, del amqp.Delivery) error {
	_, err := fmt.Fprintln(f, "PROPERTIES")
	if err != nil {
		return err
	}
	props := []property{
		{Name: "message_id", Value: del.MessageId},
		{Name: "type", Value: del.Type},
		{Name: "routing_key", Value: del.RoutingKey},
		{Name: "timestamp", Value: timestamp(del)},
		{Name: "content_type", Value: del.ContentType},
		{Name: "content_encoding", Value: del.ContentEncoding},
		{Name: "correlation_id", Value: del.CorrelationId},
		{Name: "reply_to", Value: del.ReplyTo},
		{Name: "expiration", Value: del.Expiration},
		{Name: "user_id", Value: del.UserId},
		{Name: "app_id", Value: del.AppId},
	}

	setProps := make([]property, 0, len(props))
	for _, p := range props {
		if p.Value != "" {
			setProps = append(setProps, p)
		}
	}

	sort.Slice(setProps, func(i, j int) bool {
		return setProps[i].Name < setProps[j].Name
	})

	tableprinter.Print(f, setProps)
	return nil
}

type header struct {
	Key   string      `header:"key"`
	Value interface{} `header:"value"`
}

func printHeaders(f *os.File, del amqp.Delivery) error {
	_, err := fmt.Fprintln(f, "HEADERS")
	if err != nil {
		return err
	}
	var headers []header
	for k, v := range del.Headers {
		headers = append(headers, header{Key: k, Value: v})
	}
	sort.Slice(headers, func(i, j int) bool {
		return headers[i].Key < headers[j].Key
	})
	tableprinter.Print(f, headers)
	return nil
}

func printPayload(f *os.File, del amqp.Delivery) error {
	_, err := fmt.Fprintln(f, "PAYLOAD")
	if err != nil {
		return err
	}
	if del.ContentType == "application/json" {
		var payload interface{}
		if err := json.Unmarshal(del.Body, &payload); err == nil {
			enc := json.NewEncoder(f)
			enc.SetIndent("", "  ")
			if err := enc.Encode(payload); err != nil {
				return err
			}
		}
		return nil
	}
	_, err = fmt.Fprintln(f, string(del.Body))
	return err
}

func timestamp(del amqp.Delivery) string {
	if del.Timestamp.IsZero() {
		return ""
	}
	return fmt.Sprintf("%d (%s)", del.Timestamp.Unix(), del.Timestamp.Format(time.RFC3339))
}
