package transport

import (
	"sync"
	"testing"
	"time"
)

func collect() (*[]Message, *sync.Mutex, Handler) {
	var mu sync.Mutex
	var got []Message
	return &got, &mu, func(m Message) {
		mu.Lock()
		got = append(got, m)
		mu.Unlock()
	}
}

func TestBasicDelivery(t *testing.T) {
	net := NewNetwork(Config{Seed: 1})
	defer net.Close()
	got, mu, h := collect()
	net.Connect("a", func(Message) {})
	net.Connect("b", h)
	net.Connect("c", h)
	ca := net.Connect("a", func(Message) {})
	ca.Broadcast(Message{Kind: "op", Data: []byte("hi")})
	net.Flush()
	mu.Lock()
	defer mu.Unlock()
	if len(*got) != 2 {
		t.Fatalf("broadcast should reach 2 peers, got %d", len(*got))
	}
}

func TestDropAndDup(t *testing.T) {
	// 全丢
	net := NewNetwork(Config{DropRate: 1, Seed: 1})
	defer net.Close()
	got, mu, h := collect()
	net.Connect("b", h)
	ca := net.Connect("a", func(Message) {})
	for i := 0; i < 10; i++ {
		ca.Send("b", Message{Kind: "op"})
	}
	net.Flush()
	mu.Lock()
	if len(*got) != 0 {
		t.Fatalf("all dropped expected, got %d", len(*got))
	}
	mu.Unlock()

	// 全重复
	net2 := NewNetwork(Config{DupRate: 1, Seed: 1})
	defer net2.Close()
	got2, mu2, h2 := collect()
	net2.Connect("b", h2)
	ca2 := net2.Connect("a", func(Message) {})
	ca2.Send("b", Message{Kind: "op"})
	net2.Flush()
	mu2.Lock()
	if len(*got2) != 2 {
		t.Fatalf("expected duplicate delivery, got %d", len(*got2))
	}
	mu2.Unlock()
}

func TestDisconnectDrops(t *testing.T) {
	net := NewNetwork(Config{Seed: 1})
	defer net.Close()
	got, mu, h := collect()
	cb := net.Connect("b", h)
	ca := net.Connect("a", func(Message) {})
	cb.Disconnect()
	ca.Send("b", Message{Kind: "op"})
	net.Flush()
	mu.Lock()
	if len(*got) != 0 {
		t.Fatalf("disconnected peer should not receive, got %d", len(*got))
	}
	mu.Unlock()
	cb.Reconnect()
	ca.Send("b", Message{Kind: "op"})
	net.Flush()
	mu.Lock()
	if len(*got) != 1 {
		t.Fatalf("reconnected peer should receive, got %d", len(*got))
	}
	mu.Unlock()
}

func TestDeterministicSeed(t *testing.T) {
	run := func() int {
		net := NewNetwork(Config{DropRate: 0.5, DupRate: 0.3, MaxLatency: time.Microsecond, Seed: 7})
		defer net.Close()
		var mu sync.Mutex
		count := 0
		net.Connect("b", func(Message) { mu.Lock(); count++; mu.Unlock() })
		ca := net.Connect("a", func(Message) {})
		for i := 0; i < 50; i++ {
			ca.Send("b", Message{Kind: "op"})
		}
		net.Flush()
		mu.Lock()
		defer mu.Unlock()
		return count
	}
	if run() != run() {
		t.Fatal("same seed should give same delivery count")
	}
}
