#! /usr/bin/env python3
#
# Sync files between local storage and Azure blob storage
# @see README.md for principles used

import sys, os, datetime, hashlib, base64
from dotenv import load_dotenv
from azure.storage.blob import BlockBlobService, Include

# Calculate a local file hash
BLK = 1048576
def gethash(pth):
    md5 = hashlib.md5()
    with open(pth, 'rb') as f:
        b = f.read(BLK)
        while (len(b)>0):
            md5.update(b)
            b = f.read(BLK)
    return md5.digest()

# Settings/secrets are in .env file - pull 'em into environment vars
load_dotenv()

# connect to Azure storage
blob_client = BlockBlobService(account_name=os.getenv('AZURE_STORAGE_ACCOUNT'), account_key=os.getenv('AZURE_STORAGE_KEY'))
container = os.getenv('AZURE_BACKUP_CONTAINER')
if not container:
    print('.env missing AZURE_BACKUP_CONTAINER')
    sys.exit(1)

# read safety prefix for local writes
pfx = os.getenv('AZURE_BACKUP_WRITE_PREFIX')
if not pfx:
    pfx = ''

# verbosity in environment
verb = int(os.getenv('AZURE_BACKUP_VERBOSE', '0'))
def log(v, m):
    if v < 1 or (verb and v <= verb):
        print(m)

# iterate (and remember) blobs in backup container
log(0, 'reading blob info..')
blist = {}
for blob in blob_client.list_blobs(container, include=Include(metadata=True)):
    blist[blob.name] = blob
    if (len(blist)%1000)==0:
        log(0, ' %d blobs..'%(len(blist),))
log(0, ' %d blobs'%(len(blist,)))

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
            # Existing backup file, check if transfer required (and which way)
            if nam in blist:
                # blob info..
                bsiz = blist[nam].properties.content_length
                bhsh = blist[nam].properties.content_settings.content_md5
                btim = blist[nam].properties.last_modified
                # Override blob storage's last modified with metadata if present
                if 'localtimestamp' in blist[nam].metadata:
                    ts = blist[nam].metadata['localtimestamp']
                    btim = datetime.datetime.fromisoformat(ts)
                else:
                    log(0, 'warning: %s has no timestamp in metadata'%(nam,))
                # now remove from list, as we have 'seen' this locally
                blist.pop(nam, None)
                # local file info
                lsiz = os.path.getsize(nam)
                lhsh = base64.b64encode(gethash(nam)).decode('ascii')
                ltim = datetime.datetime.fromtimestamp(os.path.getmtime(nam), datetime.timezone.utc)
                log(1, ' siz:%d/%d hsh:%s/%s tim:%s/%s: %s'%
                    (bsiz,lsiz,bhsh,lhsh,btim.isoformat(),ltim.isoformat(),nam))
                if lsiz == bsiz and lhsh == bhsh:
                    # Skip matching files (size & hash)
                    log(1, ' - skip (same)')
                    continue
                if btim > ltim:
                    # Remote is newer, put on pull list
                    pull.append(nam)
                    log(1, ' - pull (diff)')
                else:
                    # Local is newer (or the same timestamp but different hash/size), put on push list
                    push.append(nam)
                    log(1, ' - push (diff)')
            else:
                # New local file, push it
                push.append(nam)
                log(1, ' - push (no blob)')

log(0, ' %d local files'%(cnt,))

# Any remaining blobs are non-local files, pull 'em
for b in blist:
    pull.append(b)

log(0, 'pushing local changes (%d)..'%(len(push),))
tot = len(push)
cnt = 0
for nam in push:
    md = {}
    ts = datetime.datetime.fromtimestamp(os.path.getmtime(nam), datetime.timezone.utc)
    md['localtimestamp'] = ts.isoformat()
    log(1, ' %d: %s'%(os.path.getsize(nam), nam))
    blob_client.create_blob_from_path(container, nam, nam, metadata=md)
    cnt += 1
    if (cnt%10)==0:
        log(0, ' %d of %d: %s'%(cnt, tot, nam))

log(0, 'pulling remote changes (%d) to: %s'%(len(pull),pfx))
tot = len(pull)
cnt = 0
for nam in pull:
    pth = pfx+nam
    os.makedirs(os.path.dirname(pth),exist_ok=True)
    blob_client.get_blob_to_path(container, nam, pth)
    cnt += 1
    if (cnt%10)==0:
        log(0, ' %d of %d: %s'%(cnt, tot, pth))

log(0, 'sync done!')
