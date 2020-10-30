#! /usr/bin/env python3
#
# Sync files between local storage and Azure blob storage
# @see README.md for principles used

import sys, os, tempfile, datetime, subprocess, base64
from dotenv import load_dotenv
from azure.storage.blob import BlockBlobService, BlobBlock, Include, ContentSettings

# Settings/secrets are in .env file - pull 'em into environment vars
load_dotenv()

# verbosity in environment
verb = int(os.getenv('AZURE_SYNC_VERBOSE', '0'))
def log(v, m):
    if v < 1 or (verb and v <= verb):
        print(m)

# Calcuate local file slices and MD5 hashes using 'slice'
def getslices(pth):
    slc = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'slice')
    with open(pth,'rb') as fil:
        proc = subprocess.run(slc, stdin=fil, capture_output=True)
        if proc.returncode:
            log(0, 'slice failed: %d: %s'%(proc.returncode,proc.stderr.decode()))
            return None
        slices = []
        for l in proc.stdout.decode().splitlines():
            s = l.split(' ')
            slices.append((int(s[0]),s[1]))
        return slices

# connect to Azure storage
blob_client = BlockBlobService(account_name=os.getenv('AZURE_STORAGE_ACCOUNT'), account_key=os.getenv('AZURE_STORAGE_KEY'))
container = os.getenv('AZURE_SYNC_CONTAINER')
if not container:
    print('.env missing AZURE_SYNC_CONTAINER')
    sys.exit(1)

# read safety prefix for local writes
pfx = os.getenv('AZURE_SYNC_WRITE_PREFIX')
if not pfx:
    pfx = ''

# iterate (and remember) blobs in backup container, filtered by input paths
log(0, 'reading blob info..')
blist = {}
for tgt in sys.argv[1:]:
    for blob in blob_client.list_blobs(container, prefix=tgt, include=Include(metadata=True)):
        blist[blob.name] = blob
        if (len(blist)%1000)==0:
            log(0, ' %d blobs..'%(len(blist),))
log(0, ' %d blobs'%(len(blist,)))

def gettimestamp(b):
    btim = b.properties.last_modified
    # Override blob storage's last modified with metadata if present
    if 'localtimestamp' in b.metadata:
        ts = b.metadata['localtimestamp']
        btim = datetime.datetime.fromisoformat(ts)
    else:
        log(0, 'warning: %s has no timestamp in metadata'%(b.name,))
    return btim

# iterate (and remember) contents of specified folder(s) to backup
log(0, 'reading local file info..')
push = []
pull = []
cnt = 0
for tgt in sys.argv[1:]:
    for root, subs, files in os.walk(tgt):
        for fil in files:
            cnt += 1
            nam = os.path.join(root, fil);
            # Skip symlinks entirely
            if os.path.islink(nam):
                log(1, ' symlink: %s'%(nam,))
                continue
            # Always get slices..
            lslc = getslices(nam)
            # Existing backup file, check if transfer required (and which way)
            if nam in blist:
                # blob info..
                bsiz = blist[nam].properties.content_length
                bhsh = blist[nam].properties.content_settings.content_md5
                btim = gettimestamp(blist[nam])
                # now remove from list, as we have 'seen' this locally
                blist.pop(nam, None)
                # local file info (whole file hash is last slice info)
                lsiz = os.path.getsize(nam)
                lhsh = lslc[-1:][0][1]
                ltim = datetime.datetime.fromtimestamp(os.path.getmtime(nam), datetime.timezone.utc)
                log(2, ' siz:%d/%d hsh:%s/%s tim:%s/%s: %s'%
                    (bsiz,lsiz,bhsh,lhsh,btim.isoformat(),ltim.isoformat(),nam))
                # Skip matching files (size & hash)
                if lsiz == bsiz and lhsh == bhsh:
                    log(2, ' - skip (same)')
                    continue
                # We'll need the block list then..
                blks = blob_client.get_block_list(container, nam, block_list_type='committed').committed_blocks
                if btim > ltim:
                    # Remote is newer, put on pull list as tuple: (name,slices,blocks,timestamp)
                    pull.append((nam,lslc,blks,btim))
                    log(2, ' - pull (diff)')
                else:
                    # Local is newer (or the same timestamp but different hash/size), put on push list
                    # as tuple (name,slices,blocks)
                    push.append((nam,lslc,blks))
                    log(2, ' - push (diff)')
            else:
                # New local file, push it as tuple (name,slices,None)
                push.append((nam,lslc,None))
                log(2, ' - push (no blob): '+nam)

log(0, ' %d local files'%(cnt,))

# Any remaining blobs are non-local files, pull 'em as tuple (name,None,None,timestamp)
for nam in blist:
    pull.append((nam,None,None,gettimestamp(blist[nam])))

# set no-write flag
nowr = os.getenv('AZURE_SYNC_NOWRITE', None)

# helper function to load a chunk of a local file
def loadChunk(nam, off, siz):
    with open(nam, 'rb') as f:
        f.seek(off)
        return f.read(siz)

# local-only file, push all the blocks and commit the blob
def localOnlyPush(nam, slcs, md, cs):
    log(1, ' L %s'%(nam,))
    off = 0
    blst = []
    for slc in slcs:
        # skip zero length slices
        if slc[0] == 0:
            continue
        log(1, '  > %d->%d (%s)'%(off, off+slc[0], slc[1]))
        if not nowr:
            blob_client.put_block(container, nam, loadChunk(nam, off, slc[0]), slc[1])
        blst.append(BlobBlock(slc[1]))
        off += slc[0]
    log(1, ' > %s: %s'%(nam, str([b.id for b in blst])))
    if not nowr:
        blob_client.put_block_list(container, nam, blst, metadata=md, content_settings=cs)

# local modified file, assemble blocks from existing, or push non-existing, commit the blob
def localModifiedPush(nam, slcs, blks, md, cs):
    log(1, ' M %s: %s'%(nam, str([b.id for b in blks])))
    off = 0
    blst = []
    for slc in slcs:
        # skip zero length slices
        if slc[0] == 0:
            continue
        # search blks for existing hash
        blk = next((b for b in blks if b.id == slc[1]), None)
        if blk:
            # existing block, put back on list
            blst.append(blk)
            log(1, '  | %d->%d (%s)'%(off, off+slc[0], slc[1]))
        else:
            # non-existing block, push and add to list
            log(1, '  > %d->%d (%s)'%(off, off+slc[0], slc[1]))
            if not nowr:
                blob_client.put_block(container, nam, loadChunk(nam, off, slc[0]), slc[1])
            blst.append(BlobBlock(slc[1]))
        off += slc[0]
    log(1, ' > %s: %s'%(nam, str([b.id for b in blst])))
    if not nowr:
        blob_client.put_block_list(container, nam, blst, metadata=md, content_settings=cs)
        
tot = len(push)
cnt = 0
log(0, 'pushing local changes (%d)..'%(tot,))
for (nam,slcs,blks) in push:
    # Add blob metadata: timestamp in local file system, MD5 hash of file
    md = {}
    ts = datetime.datetime.fromtimestamp(os.path.getmtime(nam), datetime.timezone.utc)
    md['localtimestamp'] = ts.isoformat()
    lhsh = slcs[-1:][0][1]
    cs = ContentSettings(content_md5=lhsh)
    if None==blks:
        localOnlyPush(nam, slcs, md, cs)
    else:
        localModifiedPush(nam, slcs, blks, md, cs)
    cnt += 1
    if (cnt%10)==0:
        log(0, ' %d of %d: %s'%(cnt, tot, nam))

def remoteOnlyPull(nam, ts):
    # Whole file, so use blob pull direct to target
    pth = pfx+nam
    log(1, ' W %s->%s'%(nam,pth))
    if not nowr:
        os.makedirs(os.path.dirname(pth),exist_ok=True)
        blob_client.get_blob_to_path(container, nam, pth)
        # Set timestamp
        os.utime(pth, times=(ts, ts))

def remoteModifiedPull(nam, slcs, blks, ts):
    # Patching required, assemble temp file from local and modified blocks, rename when done
    pth = pfx+nam
    log(1, ' P %s->%s'%(nam,pth))
    fld = os.path.dirname(pth)
    if not nowr:
        os.makedirs(fld,exist_ok=True)
    tmp = None
    off = 0
    with tempfile.NamedTemporaryFile(mode='wb',dir=fld,delete=False) as out:
        tmp = out.name
        log(1, '  T: %s'%(tmp,))
        for blk in blks:
            slc = next((s for s in slcs if s[1] == blk.id), None)
            if slc:
                # Existing slice - copy from local file
                chk=0
                for s in slcs:
                    if blk.id == s[1]:
                        break
                    chk += s[0]
                log(1, '  %% %d->%d (%s)'%(chk, chk+slc[0], slc[1]))
                if not nowr:
                    out.write(loadChunk(nam, chk, slc[0]))
            else:
                # Remote block - pull from blob
                log(1, '  < %d->%d (%s)'%(off, off+blk.size, blk.id))
                if not nowr:
                    blob_client.get_blob_to_stream(container, nam, out, start_range=off, end_range=(off+blk.size-1))
            off += blk.size
    if not nowr:
        if os.path.exists(pth):
            os.remove(pth)
        os.rename(tmp, pth)
        # Set timestamp
        os.utime(pth, times=(ts, ts))
    else:
        os.remove(tmp)

log(0, 'pulling remote changes (%d) to: %s'%(len(pull),pfx))
tot = len(pull)
cnt = 0
for (nam,slcs,blks,ts) in pull:
    if None==slcs or None==blks:
        remoteOnlyPull(nam, ts.timestamp())
    else:
        remoteModifiedPull(nam, slcs, blks, ts.timestamp())
    cnt += 1
    if (cnt%10)==0:
        log(0, ' %d of %d: %s'%(cnt, tot, pth))

log(0, 'sync done!')
