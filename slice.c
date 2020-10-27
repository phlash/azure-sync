// Generate block sizes for content-based-slicing
// using a plain moving sum with an 8196 byte window.
// (this is similar to gzip --rsyncable apparantly)
// Emit blocks when least significant 17 bits are
// zero, which /should/ give an average block of 1MiB.
// Constrains blocks to at least 256k, at most 8MiB.
// Also calculates MD5 hash for each block and file.
// Reads file on stdin, emits boundaries on stdout
#include <stdio.h>
#include <openssl/md5.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>

#define WINSIZ  8196
#define MINBLK  (256*1024)
#define MAXBLK  (8*1024*1024)
#define MSKBLK  0x0001FFFF

void emit(unsigned long len, MD5_CTX *pmd5) {
    unsigned char h[MD5_DIGEST_LENGTH];
    BIO *bmem, *b64;
    BUF_MEM *bptr;
    MD5_Final(h, pmd5);
    // all this fugly code to produce base64 text using libcrypto, from:
    // https://ioncannon.net/programming/34/howto-base64-encode-with-cc-and-openssl/
    bmem = BIO_new(BIO_s_mem());
    b64 = BIO_new(BIO_f_base64());
    b64 = BIO_push(b64, bmem);
    BIO_write(b64, h, MD5_DIGEST_LENGTH);
    BIO_flush(b64);
    BIO_get_mem_ptr(b64, &bptr);
    printf("%ld %.*s\n", len, bptr->length-1, bptr->data);
    BIO_free_all(b64);
}

int main() {
    int n, d=1;
    unsigned int sum=0;
    unsigned char old[WINSIZ], buf[BUFSIZ];
    unsigned long l=0, p=0;
    MD5_CTX mdb, mdt;
    MD5_Init(&mdb);
    MD5_Init(&mdt);
    while ((n=fread(buf, 1, BUFSIZ, stdin))>0) {
        // byte-by-painful-byte (but not the I/O!)
        for (int i=0; i<n; i++) {
            // set dirty bit
            d=1;
            // add to MD5
            MD5_Update(&mdb, buf+i, 1);
            // always add next byte to sum..
            sum += (unsigned int)buf[i];
            // grab old byte from buffer..
            int o = (int)(p%WINSIZ);
            unsigned char ob = old[o];
            // save new byte to buffer..
            old[o] = buf[i];
            // increment file position
            p += 1;
            // in first block? we're done.
            if (p<WINSIZ)
                continue;
            // past first block, roll sum by subtracting old byte
            else if (p>WINSIZ)
                sum -= (unsigned int)ob;
            // at or past first block, check boundary
            unsigned long len = p-l;
            if ( (len > MINBLK) &&
                 (!(sum & MSKBLK) || (len > MAXBLK)) ) {
                // got one, emit size and MD5 hash
                emit(len, &mdb);
                // update last emit position
                l=p;
                // clear dity bit
                d=0;
                // re-init MD5
                MD5_Init(&mdb);
            }
        }
        // add I/O buffer to whole file MD5
        MD5_Update(&mdt, buf, n);
    }
    // print last block if dirty
    if (d)
        emit(p-l, &mdb);
    // print MD5 of whole file
    emit(0, &mdt);
    return 0;
}
