// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// runInboxInterestLeafPair starts a hub (accepting leaf connections) and a
// spoke (soliciting up to the hub). When lr is true the "_LR_" mechanism is
// left at its default (on, capability-negotiated); when false it is disabled on
// both ends. Any spokePatterns are configured as the spoke remote's
// CompactInterest (extending the default "_INBOX.>"). Returns once connected.
func runInboxInterestLeafPair(t *testing.T, lr bool, spokePatterns ...string) (hub, spoke *Server) {
	t.Helper()

	lo := DefaultTestOptions
	lo.Port = -1
	lo.LeafNode.Host = lo.Host
	lo.LeafNode.Port = -1
	lo.NoSystemAccount = true
	lo.LeafNode.NoCompactInterest = !lr
	hub = RunServer(&lo)

	so := DefaultTestOptions
	so.Port = -1
	so.NoSystemAccount = true
	so.LeafNode.NoCompactInterest = !lr
	rurl, _ := url.Parse(fmt.Sprintf("nats-leaf://%s:%d", lo.LeafNode.Host, lo.LeafNode.Port))
	so.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{rurl}, CompactInterest: spokePatterns}}
	so.LeafNode.ReconnectInterval = 50 * time.Millisecond
	spoke = RunServer(&so)

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, spoke)
	return hub, spoke
}

// inboxSubBreakdown classifies subscriptions in a server's global account by
// origin: local client subs, remote leaf subs (excluding the internal "$LDS."
// loop-detection sub), and the count of leaf subs that are "_LR_" reply
// wildcards.
func inboxSubBreakdown(s *Server) (local, remote, lds, lr int) {
	acc := s.globalAccount()
	var subs []*subscription
	acc.sl.All(&subs)
	for _, sub := range subs {
		switch sub.client.kind {
		case CLIENT:
			local++
		case LEAF:
			switch {
			case bytes.HasPrefix(sub.subject, []byte(leafNodeLoopDetectionSubjectPrefix)):
				lds++
			case isLeafReplySubject(sub.subject):
				remote++
				lr++
			default:
				remote++
			}
		}
	}
	return local, remote, lds, lr
}

// Setup:
//   - one hub and one spoke, the spoke solicits a leaf node connection up to the hub.
//   - one client on the spoke with 10 inbox subscriptions.
//   - one client on the hub with 10 inbox subscriptions.
//
// Questions answered by this test:
//   - How many subscriptions are live on the spoke, and how many on the hub?
//   - Does the subject interest graph propagate across the leaf connection?
//   - Do both servers end up with 10 local subs and 10 remote-registered subs?
func TestLeafNodeInboxInterestPropagation(t *testing.T) {
	// Hub: accepts leaf node connections. Disable "_LR_" so this documents the
	// uncollapsed baseline (disabling on one end is enough: it is mutually
	// negotiated).
	lo := DefaultTestOptions
	lo.Port = -1
	lo.LeafNode.Host = lo.Host
	lo.LeafNode.Port = -1
	lo.NoSystemAccount = true
	lo.LeafNode.NoCompactInterest = true
	hub := RunServer(&lo)
	defer hub.Shutdown()

	// Spoke: solicits a leaf node connection up to the hub.
	spoke, _ := runSolicitLeafServer(&lo)
	defer spoke.Shutdown()

	// Wait until the leaf connection is established on both ends.
	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, spoke)

	const numInboxes = 10

	// Client on the spoke with 10 inbox subscriptions.
	ncSpoke := natsConnect(t, spoke.ClientURL())
	defer ncSpoke.Close()
	spokeInboxes := make([]string, numInboxes)
	for i := range spokeInboxes {
		spokeInboxes[i] = nats.NewInbox()
		natsSubSync(t, ncSpoke, spokeInboxes[i])
	}
	natsFlush(t, ncSpoke)

	// Client on the hub with 10 inbox subscriptions.
	ncHub := natsConnect(t, hub.ClientURL())
	defer ncHub.Close()
	hubInboxes := make([]string, numInboxes)
	for i := range hubInboxes {
		hubInboxes[i] = nats.NewInbox()
		natsSubSync(t, ncHub, hubInboxes[i])
	}
	natsFlush(t, ncHub)

	// Classify every subscription living in a server's global account by origin:
	//   - local:  registered by a directly-connected client (kind CLIENT).
	//   - remote: registered on behalf of the other server's interest over the
	//             leaf connection (kind LEAF), excluding the internal "$LDS."
	//             loop-detection subscription.
	//   - lds:    the internal "$LDS." loop-detection subscription (also a LEAF
	//             sub, but not user interest).
	subBreakdown := func(s *Server) (local, remote, lds int) {
		acc := s.globalAccount()
		var subs []*subscription
		acc.sl.All(&subs)
		for _, sub := range subs {
			switch sub.client.kind {
			case CLIENT:
				local++
			case LEAF:
				if bytes.HasPrefix(sub.subject, []byte(leafNodeLoopDetectionSubjectPrefix)) {
					lds++
				} else {
					remote++
				}
			}
		}
		return local, remote, lds
	}

	// Wait for the interest graph to propagate across the leaf connection and
	// assert the local/remote split on each server.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if local, remote, _ := subBreakdown(spoke); local != numInboxes || remote != numInboxes {
			return fmt.Errorf("spoke: want %d local + %d remote, got %d local + %d remote",
				numInboxes, numInboxes, local, remote)
		}
		if local, remote, _ := subBreakdown(hub); local != numInboxes || remote != numInboxes {
			return fmt.Errorf("hub: want %d local + %d remote, got %d local + %d remote",
				numInboxes, numInboxes, local, remote)
		}
		return nil
	})

	// Explicitly confirm the subject interest graph propagated: every inbox
	// subscribed on one side must have matching interest on the other side.
	for _, subj := range hubInboxes {
		if !spoke.globalAccount().SubscriptionInterest(subj) {
			t.Fatalf("spoke is missing propagated interest for hub inbox %q", subj)
		}
	}
	for _, subj := range spokeInboxes {
		if !hub.globalAccount().SubscriptionInterest(subj) {
			t.Fatalf("hub is missing propagated interest for spoke inbox %q", subj)
		}
	}

	// Report the full picture for both servers.
	sLocal, sRemote, sLDS := subBreakdown(spoke)
	hLocal, hRemote, hLDS := subBreakdown(hub)
	t.Logf("spoke: total=%d (local=%d, remote=%d, lds=%d)",
		spoke.NumSubscriptions(), sLocal, sRemote, sLDS)
	t.Logf("hub:   total=%d (local=%d, remote=%d, lds=%d)",
		hub.NumSubscriptions(), hLocal, hRemote, hLDS)
}

// With the "_LR_" leaf reply mechanism enabled, the same setup (10 inbox subs
// per side) should collapse the propagated interest: each server advertises a
// single per-server reply wildcard instead of 10 unique _INBOX.<nuid> subjects.
// This is the leaf analog of the gateway "_GR_" reply prefix.
func TestLeafNodeInboxInterestLRCompaction(t *testing.T) {
	hub, spoke := runInboxInterestLeafPair(t, true)
	defer hub.Shutdown()
	defer spoke.Shutdown()

	const numInboxes = 10

	ncSpoke := natsConnect(t, spoke.ClientURL())
	defer ncSpoke.Close()
	for i := 0; i < numInboxes; i++ {
		natsSubSync(t, ncSpoke, nats.NewInbox())
	}
	natsFlush(t, ncSpoke)

	ncHub := natsConnect(t, hub.ClientURL())
	defer ncHub.Close()
	for i := 0; i < numInboxes; i++ {
		natsSubSync(t, ncHub, nats.NewInbox())
	}
	natsFlush(t, ncHub)

	// Each side should now register exactly ONE remote (leaf) sub for the
	// peer's inboxes: the peer's "_LR_.<hash>.>" reply wildcard, down from 10.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if local, remote, _, lr := inboxSubBreakdown(spoke); local != numInboxes || remote != 1 || lr != 1 {
			return fmt.Errorf("spoke: want %d local + 1 remote(_LR_), got %d local + %d remote (%d _LR_)",
				numInboxes, local, remote, lr)
		}
		if local, remote, _, lr := inboxSubBreakdown(hub); local != numInboxes || remote != 1 || lr != 1 {
			return fmt.Errorf("hub: want %d local + 1 remote(_LR_), got %d local + %d remote (%d _LR_)",
				numInboxes, local, remote, lr)
		}
		return nil
	})

	// The single remote sub on each side must be the *peer's* reply wildcard.
	if !spoke.globalAccount().SubscriptionInterest(hub.lrReplyWildcard) {
		t.Fatalf("spoke missing interest for hub reply wildcard %q", hub.lrReplyWildcard)
	}
	if !hub.globalAccount().SubscriptionInterest(spoke.lrReplyWildcard) {
		t.Fatalf("hub missing interest for spoke reply wildcard %q", spoke.lrReplyWildcard)
	}

	sLocal, sRemote, sLDS, _ := inboxSubBreakdown(spoke)
	hLocal, hRemote, hLDS, _ := inboxSubBreakdown(hub)
	t.Logf("spoke: total=%d (local=%d, remote=%d, lds=%d)  [reply wildcard %q]",
		spoke.NumSubscriptions(), sLocal, sRemote, sLDS, spoke.lrReplyWildcard)
	t.Logf("hub:   total=%d (local=%d, remote=%d, lds=%d)  [reply wildcard %q]",
		hub.NumSubscriptions(), hLocal, hRemote, hLDS, hub.lrReplyWildcard)
}

// Correctness: with "_LR_" compaction enabled, request/reply must still work
// across the leaf in both directions even though the requester's inbox is not
// individually propagated. The reply subject is rewritten to the requester's
// "_LR_" prefix on the way out and restored to the original _INBOX on the way
// back.
func TestLeafNodeInboxInterestLRRequestReply(t *testing.T) {
	hub, spoke := runInboxInterestLeafPair(t, true)
	defer hub.Shutdown()
	defer spoke.Shutdown()

	ncHub := natsConnect(t, hub.ClientURL())
	defer ncHub.Close()
	ncSpoke := natsConnect(t, spoke.ClientURL())
	defer ncSpoke.Close()

	// Direction 1: requester on the spoke, responder on the hub.
	natsSub(t, ncHub, "service.hub", func(m *nats.Msg) {
		m.Respond([]byte("from-hub"))
	})
	natsFlush(t, ncHub)
	checkSubInterestServer(t, spoke, "service.hub")

	resp, err := ncSpoke.Request("service.hub", []byte("ping"), 2*time.Second)
	if err != nil {
		t.Fatalf("spoke->hub request failed: %v", err)
	}
	if string(resp.Data) != "from-hub" {
		t.Fatalf("unexpected reply: %q", resp.Data)
	}

	// Direction 2: requester on the hub, responder on the spoke.
	natsSub(t, ncSpoke, "service.spoke", func(m *nats.Msg) {
		m.Respond([]byte("from-spoke"))
	})
	natsFlush(t, ncSpoke)
	checkSubInterestServer(t, hub, "service.spoke")

	resp, err = ncHub.Request("service.spoke", []byte("ping"), 2*time.Second)
	if err != nil {
		t.Fatalf("hub->spoke request failed: %v", err)
	}
	if string(resp.Data) != "from-spoke" {
		t.Fatalf("unexpected reply: %q", resp.Data)
	}
}

// checkSubInterestServer waits until the given server has interest for subj in
// the global account.
func checkSubInterestServer(t *testing.T, s *Server, subj string) {
	t.Helper()
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if !s.globalAccount().SubscriptionInterest(subj) {
			return fmt.Errorf("no interest for %q yet", subj)
		}
		return nil
	})
}

// Per-remote configuration extends the eligible set beyond the default
// "_INBOX.>". Here the spoke also collapses "deliver.>". Subscriptions matching
// either pattern collapse into the single reply wildcard; a non-eligible
// subject still propagates as its own interest entry.
func TestLeafNodeInboxInterestLRCustomPatterns(t *testing.T) {
	hub, spoke := runInboxInterestLeafPair(t, true, "deliver.>")
	defer hub.Shutdown()
	defer spoke.Shutdown()

	ncSpoke := natsConnect(t, spoke.ClientURL())
	defer ncSpoke.Close()

	// 5 inbox subs + 5 deliver subs (both eligible) -> collapse to 1 remote.
	for i := 0; i < 5; i++ {
		natsSubSync(t, ncSpoke, nats.NewInbox())
		natsSubSync(t, ncSpoke, fmt.Sprintf("deliver.%d", i))
	}
	// One non-eligible subject -> must still propagate as its own interest.
	natsSubSync(t, ncSpoke, "other.subject")
	natsFlush(t, ncSpoke)

	// On the hub: 1 collapsed "_LR_" wildcard + 1 normal "other.subject" sub.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		_, remote, _, lr := inboxSubBreakdown(hub)
		if remote != 2 || lr != 1 {
			return fmt.Errorf("hub: want 2 remote (1 _LR_ + 1 plain), got %d remote (%d _LR_)", remote, lr)
		}
		return nil
	})
	if !hub.globalAccount().SubscriptionInterest("other.subject") {
		t.Fatalf("hub should still carry the non-eligible 'other.subject' interest")
	}
	if !hub.globalAccount().SubscriptionInterest(spoke.lrReplyWildcard) {
		t.Fatalf("hub should carry the spoke reply wildcard %q", spoke.lrReplyWildcard)
	}

	// Correctness: a request whose reply is a custom-eligible subject (deliver.*)
	// must still route back across the leaf.
	ncHub := natsConnect(t, hub.ClientURL())
	defer ncHub.Close()
	natsSub(t, ncHub, "svc", func(m *nats.Msg) { m.Respond([]byte("ok")) })
	natsFlush(t, ncHub)
	checkSubInterestServer(t, spoke, "svc")

	reply := "deliver.reply.42"
	sub := natsSubSync(t, ncSpoke, reply)
	natsFlush(t, ncSpoke)
	if err := ncSpoke.PublishRequest("svc", reply, []byte("ping")); err != nil {
		t.Fatalf("publish request: %v", err)
	}
	if msg := natsNexMsg(t, sub, 2*time.Second); string(msg.Data) != "ok" {
		t.Fatalf("unexpected reply on custom subject: %q", msg.Data)
	}
}

// Capability gating: if either end does not support "_LR_", nothing is
// collapsed and interest propagates per-subject as before.
func TestLeafNodeInboxInterestLRDisabled(t *testing.T) {
	hub, spoke := runInboxInterestLeafPair(t, false)
	defer hub.Shutdown()
	defer spoke.Shutdown()

	ncSpoke := natsConnect(t, spoke.ClientURL())
	defer ncSpoke.Close()
	for i := 0; i < 10; i++ {
		natsSubSync(t, ncSpoke, nats.NewInbox())
	}
	natsFlush(t, ncSpoke)

	// No collapse: 10 individual inbox subs propagate to the hub, 0 are "_LR_".
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		_, remote, _, lr := inboxSubBreakdown(hub)
		if remote != 10 || lr != 0 {
			return fmt.Errorf("hub: want 10 remote and 0 _LR_, got %d remote (%d _LR_)", remote, lr)
		}
		return nil
	})
}

// The config parser accepts compact_interest patterns per remote.
func TestLeafNodeInboxInterestLRConfigParse(t *testing.T) {
	conf := createConfFile(t, []byte(`
		leafnodes {
			remotes [
				{
					url: "nats-leaf://127.0.0.1:7422"
					compact_interest: ["_INBOX.>", "deliver.>", "$KV.>"]
				}
			]
		}
	`))
	opts, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("parsing config: %v", err)
	}
	if len(opts.LeafNode.Remotes) != 1 {
		t.Fatalf("expected 1 remote, got %d", len(opts.LeafNode.Remotes))
	}
	got := opts.LeafNode.Remotes[0].CompactInterest
	want := []string{"_INBOX.>", "deliver.>", "$KV.>"}
	if len(got) != len(want) {
		t.Fatalf("compact_interest: want %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("compact_interest[%d]: want %q, got %q", i, want[i], got[i])
		}
	}
}
