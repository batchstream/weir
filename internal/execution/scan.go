package execution

import "github.com/batchstream/weir-protocol/api/protocol"

// A Scan fetch owns one bounded result batch while its backend permit is free
// during publication. These bounds apply to retained output, not database pages.
const ScanBatchDocuments = 128
const ScanBatchBytes = 4 << 20
const ScanResultBytes = ScanBatchBytes + ScanBatchDocuments*protocol.ResultOverhead + protocol.MaxScanToken + protocol.ResultOverhead
