# Azure sync client

Because Debian has python3-azure-storage out-of-the-box, and it works
nicely to push files into Azure blob storage... let's try writing a
file sync client, inspired by Ken Faulkner's
[BlobSync](https://github.com/kpfaulkner/BlobSync) but avoiding the
separate storage file(s) he uses to hold rsync-compatible signatures,
we'll work entirely with Azure metadata :)

## Principle

Files are sync'ed based on:
 * Size - different needs syncing
 * Hash (held as file property in Azure: CONTENT-MD5) - different needs syncing
 * Timestamp - newer is pushed to older

NB: Azure storage will always set 'last modified' to the time of last blob write,
thus in order to preserve the source file timestamp, we use a metadata property:
indeed we also wish to preserve file mode and ownership, so we store these in
blob metedata too.

Sync for each file uses dynamic content slicing, taking advantage of Azure blob
blocks, to reduce the amount of data that needs to be copied across the network.
Slices are determined by the C program slice.c, which uses a moving sum to find
suitable slice points, and calcuates MD5 hashes of each slice and the whole file.

Each slice can then be compared to it's corresponding Azure block, replacing those
that are of different length or hash value.

NB: Azure blob blocks have an 'id' string that can hold the MD5 hash of the block
and a 'length' field which together provides all the information we need.

Process is:
 * Gather list of files and properties from both ends (local uses slice.c)
 * Build lists of files to transfer each way, based on size/hash/timestamp
 * Start job(s) to sync changed files
 * Wait for completion

Per-file process is:
 * Compare list of Azure block metadata (length, hash) with local slice data
 * Build list of changed/new/deleted blocks/slices
 * If updating Azure, transfer slices to blocks, commit new block list
 * Else, patch temp file from slices and transferred blocks, rename when done
