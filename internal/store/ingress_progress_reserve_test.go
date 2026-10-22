package store

import (
	"sync"
	"testing"

	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
)

func TestIngressWorkingBoundaryExchangesOnlyBoundedUnusedInput(t *testing.T) {
	for _, boundary := range []struct {
		name         string
		budget, free uint64
	}{
		{name: "larger-budget", budget: 501079517, free: 7286},
		{name: "smaller-budget", budget: 460395314, free: 6112},
	} {
		t.Run(boundary.name, func(t *testing.T) {
			memory := NewExecutionMemory()
			memory.known, memory.trusted, memory.budget = true, true, boundary.budget
			work := plan(1, "working", false)
			work.Command = nativeCall()
			work.Bytes = 310293 - nativeQueuedWindow
			lease, failure, _ := memory.admit(work)
			if failure != nil {
				t.Fatal(failure)
			}
			defer lease.release()
			var accounts []*IngressMemory
			defer func() {
				for _, account := range accounts {
					account.Close()
					account.Retire()
				}
				lease.release()
				if snapshot := memory.Snapshot(); snapshot.UsedBytes != 0 || snapshot.ResidentBytes != 0 || snapshot.InputReservedBytes != 0 || snapshot.InputUsedBytes != 0 {
					t.Error("owners retained after cleanup", snapshot)
				}
			}()
			before := memory.Snapshot()
			working := int(boundary.budget-before.UsedBytes-boundary.free) - (lease.publication - lease.window)
			if started, _ := lease.start(working, defaultNativeIOReserve); !started {
				t.Fatal("could not form observed total-budget boundary", memory.Snapshot())
			}
			before = memory.Snapshot()
			if before.BudgetBytes-before.UsedBytes != boundary.free || before.InputReservedBytes != 32<<20 || before.InputUsedBytes != 310293 {
				t.Fatal("wrong boundary", before)
			}
			account, err := memory.NewIngress(t.Context(), 8504)
			if err != nil {
				t.Fatal("fresh ingress starved behind unused queue reservation", err)
			}
			accounts = append(accounts, account)
			after := memory.Snapshot()
			shortage := uint64(8504) - boundary.free
			if after.UsedBytes != after.BudgetBytes || after.InputReservedBytes != before.InputReservedBytes-shortage || after.InputUsedBytes != before.InputUsedBytes || after.ResidentBytes != before.ResidentBytes+boundary.free {
				t.Fatal("exchange did not conserve exact ownership", before, after)
			}
			// Spend the remainder through a second real owner. Repeated requests
			// cannot exchange more than the existing bounded progress allowance.
			remainder := uint64(ingressProgressReserve) - shortage
			account, err = memory.NewIngress(t.Context(), int(remainder))
			if err != nil {
				t.Fatal(err)
			}
			accounts = append(accounts, account)
			before = memory.Snapshot()
			if _, err := memory.NewIngress(t.Context(), 1); err == nil {
				t.Fatal("repeated ingress drained the input promise")
			}
			after = memory.Snapshot()
			if after.UsedBytes != before.UsedBytes || after.ResidentBytes != before.ResidentBytes || after.InputReservedBytes != before.InputReservedBytes || after.InputUsedBytes != before.InputUsedBytes || before.InputReservedBytes != (32<<20)-ingressProgressReserve {
				t.Fatal("denied ingress mutated accounting", before, after)
			}
			// Already admitted input remains fully promised, while future input
			// admission draws from the reduced reservation without a new charge.
			work.Bytes = 128
			second, failure, _ := memory.admit(work)
			if failure != nil {
				t.Fatal("exchange revoked an admitted input promise", failure)
			}
			second.release()
			for _, account := range accounts {
				account.Close()
				account.Retire()
			}
			memory.mu.Lock()
			memory.resizeInputLocked()
			memory.mu.Unlock()
			after = memory.Snapshot()
			if after.InputReservedBytes != 32<<20 || after.UsedBytes != boundary.budget-boundary.free || after.InputUsedBytes != 310293 {
				t.Fatal("released ingress did not permit exact reservation restoration", after)
			}
			account, err = memory.NewIngress(t.Context(), 8504)
			if err != nil {
				t.Fatal(err)
			}
			accounts = append(accounts, account)
			if err := account.ReserveMessage(86); err != nil {
				t.Fatal("short ingress could not progress past its header", err)
			}
			input := account.Input(account.Decoded())
			short := plan(2, "short", true)
			input.Bind(short)
			before = memory.Snapshot()
			second, failure, _ = memory.admit(short)
			if failure != nil {
				input.Release()
				t.Fatal("decoded ingress could not hand off at working capacity", failure)
			}
			after = memory.Snapshot()
			if after.UsedBytes > before.UsedBytes || after.UsedBytes > after.BudgetBytes || after.InputUsedBytes > after.InputReservedBytes || after.ResidentBytes > memory.residentLimitLocked() {
				t.Fatal("handoff minted credit or lost input ownership", before, after)
			}
			if err := second.terminal(200); err != nil {
				t.Fatal("handoff lost its promised publication window", err)
			}
			second.release()
			var group sync.WaitGroup
			for index := range 4 {
				group.Go(func() {
					concurrent, err := memory.NewIngress(t.Context(), 8504)
					if err != nil {
						t.Error("concurrent ingress failed", err)
						return
					}
					defer concurrent.Retire()
					defer concurrent.Close()
					if err := concurrent.ReserveMessage(86); err != nil {
						t.Error("concurrent message failed", err)
						return
					}
					input := concurrent.Input(concurrent.Decoded())
					request := plan(uint64(index+3), "concurrent", true)
					input.Bind(request)
					admitted, failure, _ := memory.admit(request)
					if failure != nil {
						input.Release()
						t.Error("concurrent handoff failed", failure)
						return
					}
					defer admitted.release()
					if err := admitted.terminal(200); err != nil {
						t.Error("concurrent publication lost its promise", err)
					}
				})
			}
			group.Wait()
		})
	}
}

func TestIngressExchangePreservesLiveInputAndResidentLimit(t *testing.T) {
	memory := testExecutionMemory(768 << 20)
	work := plan(1, "queued", false)
	work.Bytes = int(memory.inputTargetLocked()) - nativeQueuedWindow - 100
	lease, failure, _ := memory.admit(work)
	if failure != nil {
		t.Fatal(failure)
	}
	defer lease.release()
	before := memory.Snapshot()
	account, err := memory.NewIngress(t.Context(), int(memory.residentLimitLocked()-before.ResidentBytes))
	if err != nil {
		t.Fatal(err)
	}
	defer account.Retire()
	defer account.Close()
	before = memory.Snapshot()
	if _, err := memory.NewIngress(t.Context(), 101); err == nil {
		t.Fatal("ingress revoked live queued input")
	}
	after := memory.Snapshot()
	if after.UsedBytes != before.UsedBytes || after.ResidentBytes != before.ResidentBytes || after.InputReservedBytes != before.InputReservedBytes {
		t.Fatal("failed resident/input exchange changed accounting", before, after)
	}
	short, err := memory.NewIngress(t.Context(), 100)
	if err != nil {
		t.Fatal(err)
	}
	defer short.Retire()
	defer short.Close()
	after = memory.Snapshot()
	if after.InputReservedBytes != after.InputUsedBytes || after.ResidentBytes != before.ResidentBytes || after.ResidentBytes != memory.residentLimitLocked() || after.UsedBytes != before.UsedBytes {
		t.Fatal("exchange crossed real input or resident limit", before, after)
	}
}

func TestIngressPartialUploadsKeepShortRequestProgressAcrossInputPromise(t *testing.T) {
	for _, scenario := range []string{"first-promise", "observer-refill"} {
		t.Run(scenario, func(t *testing.T) {
			memory := NewExecutionMemory()
			// The failed B512 formation had this exact trusted Node budget,
			// 34 connection owners and 3 admitted, incomplete 12MiB messages.
			sample := overload.Snapshot{RecoveryAllowanceKnown: true, RecoveryAllowance: 368_154_213}
			memory.ObserveMemory(sample)
			var accounts []*IngressMemory
			var releases []func()
			var leases []*executionLease
			t.Cleanup(func() {
				for _, lease := range leases {
					lease.release()
				}
				for _, account := range accounts {
					account.Close()
					account.Retire()
				}
				for _, release := range releases {
					release()
				}
				if snapshot := memory.Snapshot(); snapshot.UsedBytes != 0 || snapshot.ResidentBytes != 0 || snapshot.IngressConnections != 0 || snapshot.IngressBuffers != 0 || snapshot.IngressMessages != 0 || snapshot.InputUsedBytes != 0 {
					t.Error("progress test retained actual owners", snapshot)
				}
			})
			for range 34 {
				release, err := memory.ReserveIngressConnection()
				if err != nil {
					t.Fatal(err)
				}
				releases = append(releases, release)
			}
			for index := range 32 {
				account, err := memory.NewIngress(t.Context(), 9632)
				if err != nil {
					t.Fatal(err)
				}
				accounts = append(accounts, account)
				if index < 3 {
					if err := account.ReserveMessage(12_582_998); err != nil {
						t.Fatal(err)
					}
				} else {
					// An unadmitted stream retains its original 64KiB DATA window;
					// all four actual frame capacities and owners remain charged.
					for range 4 {
						release, err := account.Buffer(16 << 10)
						if err != nil {
							t.Fatal(err)
						}
						releases = append(releases, release)
					}
				}
			}
			formation := memory.Snapshot()
			if formation.BudgetBytes != 245_436_142 || formation.UsedBytes != 171_152_904 || formation.IngressReservedBytes != 153_327_112 || formation.IngressConnections != 34 || formation.IngressMessages != 3 || formation.Count != 0 {
				t.Fatal("test did not recreate B formation's real reservations", formation)
			}
			first, failure, _ := memory.admit(plan(1, "first-read", true))
			if failure != nil {
				t.Fatal(failure)
			}
			leases = append(leases, first)
			started, waiting := first.start(48_002_886, 48_000_000)
			if !started || waiting {
				t.Fatal("first short Read did not start", memory.Snapshot())
			}
			if scenario == "observer-refill" {
				// Retire only the unused promise, then exercise the real observer
				// growth path while this first operation still owns all its input
				// and IO workspace. Refill must not consume short-request space.
				memory.mu.Lock()
				unused := memory.inputReserved - memory.inputUsed
				memory.inputReserved -= unused
				memory.used -= unused
				memory.resident -= unused
				memory.mu.Unlock()
				before := memory.Snapshot()
				memory.ObserveMemory(sample)
				after := memory.Snapshot()
				if after.InputReservedBytes <= before.InputReservedBytes || after.InputUsedBytes != before.InputUsedBytes || after.UsedBytes-after.ResidentBytes != before.UsedBytes-before.ResidentBytes {
					t.Fatal("refill did not preserve real input/IO ownership", before, after)
				}
			}
			// Native and Read on the independent SDK and the selected upload's
			// ClientConn all need native header and body admission concurrently.
			var short [4]*IngressMemory
			var errors [4]error
			var group sync.WaitGroup
			for index := range short {
				group.Go(func() {
					short[index], errors[index] = memory.NewIngress(t.Context(), 9632)
					if errors[index] == nil {
						errors[index] = short[index].ReserveMessage(86)
					}
				})
			}
			group.Wait()
			for index, account := range short {
				if account != nil {
					accounts = append(accounts, account)
				}
				if errors[index] != nil {
					t.Errorf("short RPC %d refused while unused Q occupied the entry space: %v; formation=%+v current=%+v", index, errors[index], formation, memory.Snapshot())
				}
			}
			if t.Failed() {
				return
			}
			for index, account := range short {
				input := account.Input(account.Decoded())
				work := plan(uint64(index+2), "short", true)
				work.ResultBytes = execution.ResultOverheadBytes
				if index%2 != 0 {
					work.Command = nativeCall()
				}
				input.Bind(work)
				lease, failure, _ := memory.admit(work)
				if failure != nil {
					input.Release()
					t.Fatal("short input could not exchange its existing owner", failure, memory.Snapshot())
				}
				leases = append(leases, lease)
			}
			first.finishWorking()
			first.release()
			for _, lease := range leases[1:] {
				started, waiting := lease.start(48_008_364, 48_000_000)
				if !started || waiting {
					t.Fatal("bounded Read/Native failed to resume after actual credit returned", memory.Snapshot())
				}
				if err := lease.terminal(200); err != nil {
					t.Fatal(err)
				}
				lease.release()
			}
		})
	}
}
