package store

import (
	"sync"
	"testing"

	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
)

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
