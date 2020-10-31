#! /usr/bin/env python3
#
# Sync files between local storage and Azure blob storage
# @see README.md for principles used

import sys, os, tempfile, datetime, subprocess, json
from dotenv import load_dotenv
from azure.storage.blob import BlockBlobService, BlobBlock, Include, ContentSettings

# Settings/secrets are in .env file - pull 'em into environment vars
load_dotenv()

# verbosity in environment
verb = int(os.getenv('AZURE_SYNC_VERBOSE', '0'))
def log(v, m):
    if v <= verb:
        print(m)


### Functions ###

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

# list remote blobs, filtered by input paths
def listblobs(blob_client, paths):
    log(0, 'reading blob info..')
    blist = {}
    for tgt in paths:
        for blob in blob_client.list_blobs(container, prefix=tgt, include=Include(metadata=True)):
            blist[blob.name] = blob
            if (len(blist)%1000)==0:
                log(0, ' %d blobs..'%(len(blist),))
    log(0, '%d blobs'%(len(blist,)))
    return blist

# parse file stat object from dict, with default
def getfilestat(md, btim):
    # Check for v0.2 metadata (partial stat object)
    st = None
    if 'filestat' in md:
        tmp = json.loads(md['filestat'])
        st = os.stat_result((tmp[0], 0, 0, 0, tmp[1], tmp[2], 0, tmp[3], tmp[3], tmp[3]))
    # .. v0.1 only held timestamp, use that or fall back to Azure timestamp
    elif 'localtimestamp' in md:
        ts = md['localtimestamp']
        btim = datetime.datetime.fromisoformat(ts)
        ts = btim.timestamp()
        st = os.stat_result((0, 0, 0, 0, 0, 0, 0, ts, ts, ts))
    else:
        log(0, 'warning: no timestamp in metadata: %s'%(str(md),))
        ts = btim.timestamp()
        st = os.stat_result((0, 0, 0, 0, 0, 0, 0, ts, ts, ts))
    return st

# add file stat metadata from stat object to dict
def addfilestat(md, st):
    tmp = (st.st_mode, st.st_uid, st.st_gid, st.st_mtime)
    md['filestat'] = json.dumps(tmp)

# read local files, determine actions / read block lists
def readlocal(blob_client, blist, paths):
    log(0, 'reading local file info..')
    push = []
    pull = []
    cnt = 0
    for tgt in paths:
        for root, subs, files in os.walk(tgt):
            for fil in files:
                cnt += 1
                nam = os.path.join(root, fil);
                # Skip symlinks entirely
                if os.path.islink(nam):
                    log(1, ' symlink: %s'%(nam,))
                    continue
                # Always get slices & stat..
                lslc = getslices(nam)
                lstt = os.stat(nam)
                log(1, ' sliced(%d): %s'%(len(lslc)-1,nam))
                # Existing backup file, check if transfer required (and which way)
                if nam in blist:
                    # blob info..
                    bsiz = blist[nam].properties.content_length
                    bhsh = blist[nam].properties.content_settings.content_md5
                    bstt = getfilestat(blist[nam].metadata, blist[nam].properties.last_modified)
                    btim = bstt.st_mtime
                    # now remove from blob list, as we have 'seen' this locally
                    blist.pop(nam, None)
                    # local file info (whole file hash is last slice info)
                    lsiz = os.path.getsize(nam)
                    lhsh = lslc[-1:][0][1]
                    ltim = lstt.st_mtime
                    log(2, '  [b/l](siz:%d/%d hsh:%s/%s tim:%d/%d)'%(bsiz,lsiz,bhsh,lhsh,btim,ltim))
                    # Skip matching files (size & hash)
                    if lsiz == bsiz and lhsh == bhsh:
                        log(2, '  skip (same size/hash)')
                        continue
                    # We'll need the block list then..
                    blks = blob_client.get_block_list(container, nam, block_list_type='committed').committed_blocks
                    if btim > ltim:
                        # Remote is newer, put on pull list as tuple: (name,slices,blocks,stat)
                        pull.append((nam,lslc,blks,bstt))
                        log(2, '  pull (blob newer)')
                    else:
                        # Local is newer (or the same timestamp but different hash/size), put on push list
                        # as tuple (name,slices,blocks,stat)
                        push.append((nam,lslc,blks,lstt))
                        log(2, '  push (local same/newer)')
                else:
                    # New local file, push it as tuple (name,slices,None,stat)
                    push.append((nam,lslc,None,lstt))
                    log(2, '  push (no blob)')
    log(0, '%d local files'%(cnt,))
    return (push, pull)


# load a chunk of a local file
def loadChunk(nam, off, siz):
    with open(nam, 'rb') as f:
        f.seek(off)
        return f.read(siz)

# local-only file, push all the blocks and commit the blob
def localOnlyPush(nam, slcs, md, cs, nowr):
    log(1, ' L %s'%(nam,))
    off = 0
    blst = []
    for slc in slcs:
        # skip zero length slices
        if slc[0] == 0:
            continue
        log(2, '  > %d->%d (%s)'%(off, off+slc[0], slc[1]))
        if not nowr:
            blob_client.put_block(container, nam, loadChunk(nam, off, slc[0]), slc[1])
        blst.append(BlobBlock(slc[1]))
        off += slc[0]
    log(2, ' > %s: %s'%(nam, str([b.id for b in blst])))
    if not nowr:
        blob_client.put_block_list(container, nam, blst, metadata=md, content_settings=cs)

# local modified file, assemble blocks from existing, or push non-existing, commit the blob
def localModifiedPush(nam, slcs, blks, md, cs, nowr):
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
            log(2, '  | %d->%d (%s)'%(off, off+slc[0], slc[1]))
        else:
            # non-existing block, push and add to list
            log(2, '  > %d->%d (%s)'%(off, off+slc[0], slc[1]))
            if not nowr:
                blob_client.put_block(container, nam, loadChunk(nam, off, slc[0]), slc[1])
            blst.append(BlobBlock(slc[1]))
        off += slc[0]
    log(2, ' > %s: %s'%(nam, str([b.id for b in blst])))
    if not nowr:
        blob_client.put_block_list(container, nam, blst, metadata=md, content_settings=cs)
        
# remote-only file, pull to temporary file, rename
def remoteOnlyPull(pfx, nam, nowr):
    pth = pfx+nam
    log(1, ' W %s->%s'%(nam,pth))
    fld = os.path.dirname(pth)
    if not nowr:
        os.makedirs(os.path.dirname(pth),exist_ok=True)
        tmp = None
        with tempfile.NamedTemporaryFile(mode='wb',dir=fld,delete=False) as out:
            tmp = out.name
            blob_client.get_blob_to_stream(container, nam, out)
        if os.path.exists(pth):
            os.remove(pth)
        os.rename(tmp, pth)

def remoteModifiedPull(pfx, nam, slcs, blks, nowr):
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
        log(2, '  T: %s'%(tmp,))
        for blk in blks:
            slc = next((s for s in slcs if s[1] == blk.id), None)
            if slc:
                # Existing slice - copy from local file
                chk=0
                for s in slcs:
                    if blk.id == s[1]:
                        break
                    chk += s[0]
                log(2, '  %% %d->%d (%s)'%(chk, chk+slc[0], slc[1]))
                if not nowr:
                    out.write(loadChunk(nam, chk, slc[0]))
            else:
                # Remote block - pull from blob
                log(2, '  < %d->%d (%s)'%(off, off+blk.size, blk.id))
                if not nowr:
                    blob_client.get_blob_to_stream(container, nam, out, start_range=off, end_range=(off+blk.size-1))
            off += blk.size
    if not nowr:
        if os.path.exists(pth):
            os.remove(pth)
        os.rename(tmp, pth)
    else:
        os.remove(tmp)

def applystat(pfx, nam, st, nowr):
    # Update file with correct owner, mode & timestamp
    pth = pfx+nam
    log(2, '  S (%o,%d/%d,%d)'%(st.st_mode,st.st_uid,st.st_gid,st.st_mtime))
    if not nowr:
        if os.geteuid() == 0:
            os.chown(pth, st.st_uid, st.st_gid) # nb: only possible if effectively root
        os.utime(pth, times=(st.st_atime, st.st_mtime))
        os.chmod(pth, st.st_mode)   # nb: last in case it's zero and we nuke ourselves..


### Entry point ###

if __name__ == '__main__':
    # argument processing
    dopush = True
    dopull = True
    paths = []
    for arg in sys.argv[1:]:
        if arg.startswith('-pull') or arg.startswith('--pull'):
            dopush = False
        elif arg.startswith('-push') or arg.startswith('--push'):
            dopull = False
        elif arg.startswith('-h') or arg.startswith('--h'):
            print('usage: azure-sync.py [-pullonly] [-pushonly] <path> [...]')
            sys.exit(0)
        else:
            paths.append(arg)
    # connect to Azure storage
    blob_client = BlockBlobService(account_name=os.getenv('AZURE_STORAGE_ACCOUNT'), account_key=os.getenv('AZURE_STORAGE_KEY'))
    container = os.getenv('AZURE_SYNC_CONTAINER')
    if not container:
        print('.env missing AZURE_SYNC_CONTAINER')
        sys.exit(1)

    # read safety prefix for local writes
    pfx = os.getenv('AZURE_SYNC_WRITE_PREFIX', '/tmp/azure-sync-writes')

    # read no-write flag
    nowr = os.getenv('AZURE_SYNC_NOWRITE', None)

    # list blobs..
    blist = listblobs(blob_client, paths)

    # read local files, determine actions..
    (push, pull) = readlocal(blob_client, blist, paths)

    # Any remaining blobs are non-local files, pull 'em as tuple (name,None,None,stat)
    for nam in blist:
        log(1, ' non-local: %s'%(nam,))
        pull.append((nam,None,None,getfilestat(blist[nam].metadata,blist[nam].properties.last_modified)))
    log(0, '%d non-local files'%(len(blist),))

    # Take actions!
    if dopush:
        tot = len(push)
        cnt = 0
        log(0, 'pushing local changes (%d)..'%(tot,))
        for (nam,slcs,blks,st) in push:
            # Add blob metadata
            md = {}
            addfilestat(md, st)
            lhsh = slcs[-1:][0][1]
            cs = ContentSettings(content_md5=lhsh)
            if None==blks:
                localOnlyPush(nam, slcs, md, cs, nowr)
            else:
                localModifiedPush(nam, slcs, blks, md, cs, nowr)
            cnt += 1
            log(0, ' %d of %d: %s'%(cnt, tot, nam))
    else:
        log(0, 'NOT pushed: %d changes'%(len(push),))

    if dopull:
        tot = len(pull)
        cnt = 0
        log(0, 'pulling remote changes to prefix: %s (%d)..'%(pfx,tot))
        for (nam,slcs,blks,st) in pull:
            if None==slcs or None==blks:
                remoteOnlyPull(pfx, nam, nowr)
            else:
                remoteModifiedPull(pfx, nam, slcs, blks, nowr)
            applystat(pfx, nam, st, nowr)
            cnt += 1
            log(0, ' %d of %d: %s'%(cnt, tot, nam))
    else:
        log(0, 'NOT pulled: %d changes'%(len(pull),))

    log(0, 'sync done!')
