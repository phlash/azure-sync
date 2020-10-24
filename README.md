# Azure sync client

Because Debian has python3-azure-storage out-of-the-box, and it works
nicely to push files into Azure blob storage... let's try writing a
file sync client!

## Principle

Files are sync'ed based on:
 * Size - different needs syncing
 * Hash (held as file property in Azure: CONTENT-MD5) - different needs syncing
 * Timestamp - newer is pushed to older

NB: Azure storage will always set 'last modified' to the time of last blob write,
thus in order to preserve the source file timestamp, we use a metadata propery.

Process is:
 * Gather list of files and properties from both ends
 * Build lists of files to transfer each way
 * Start job(s) to copy changed files
 * Wait for completion
