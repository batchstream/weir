M26R Job API response from command-0050.out (SHA256 644eaf059fa603a476e797b4b8b9feefe2ee519aad1741bd9c1abd648c4744d4). Only namespace/owner, Job UID and node name are replaced with synthetic bindings. Quantity forms, omitted probe zero values, defaults, container/init order and execution fields are preserved. The test Pod combines this spec with the recorded M25 Pod admission defaults; it is an offline construction, not an M26 runtime observation.

`eks-loopback-check-before-m26r3.sh` is the unchanged shell guard from commit
`0e35d1e5c515d9ed84a9d07baad5b0ed1369a9cf`. Only offline tests rewrite its fixed
proc paths and supply fake HTTP commands, then execute both versions against the
same complete table bytes to compare the existing address/port/state/UDP rules.
It is never mounted or sourced by a live fixture.

`eks-cleanup-m26r5.json` retains all 44 rows from the actual failed M26R5
metadata inventory, all registered/default UIDs, the actual metrics API discovery,
and the original command resource catalog. Its provenance includes each raw
file SHA256. The original seven-field metadata template did not emit namespace;
the replay CLI appends the namespace bound by that recorded command. Other
object status fields in the replay are synthetic and are not native evidence.
The same existing CLI fixture emits this complete recorded list with the first
resource batch and exercises UID deletion and final-inventory refusal.
