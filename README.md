These are still early days; consider this marketing material until the
first Python prototype is actually done.

# TinFoil Hat FileSystem #

This is a layered, user space (for now) distributed filesystem.

## Why new filesystem? ##

* I want either local copy, or transparent remote access to all of my data
  on all of my machines, with almost real-time synchronization (but also
  support disconnected operation some of the time when I am in transit).

* Unison ( http://www.cis.upenn.edu/~bcpierce/unison/ ) or SyncThing (
  https://syncthing.net ) could not deal with my # of files / large files
  efficiently (mainly VMs), nor did they aim for ~real-time
  synchronization.

* Cloud services are not trustworthy, cheap, nor fast enough for
  'large-scale' personal data collections (terabytes, millions of files).

* Some of the filesystems and hardware I use are not robust enough
  (e.g. HFS+ on OS X, no RAID on laptops) to ensure data never gets
  corrupted.

## What does it consist of? ##

It consists of following layers:

* Reference-counted hash-identified set of variable-length blocks in
  **storage layer**

* N-ary hash **forest layer** with strong self-verification properties

* Tree **synchronization layer** over network


## Storage layer ##

The forest layer operates on reference-counted hash-identified
variable-length blocks of data. The storage layer provides storage of those
blocks.

A stored block contains:

1. 32 bit header, which identifies transformation(s) applied to the block

2. (**TBD** transform set-related extra data, if any; e.g. encryption IV,
   uncompressed length #)

3. (possibly transformed) portion which contains the variable-length
   block data

It also has two properties not encoded within the block itself:

* The 32-byte 'name' of a block which is SHA256 hash of the non-transformed
  variable-length block data. If desired, it may optionally have some
  further confidentiality-preserving transformation applied to it as well.

* Reference count (**TBD** # of bits guaranteed? <= 16?) is the number of
  references to that particular block.

Storage layer is responsible only for retrieving/storing the named blocks,
and handling reference counted deletion of these named blocks, as requested
by the forest layer.

A simple implementation of storage layer would be a normal filesystem
directory, where e.g. block 1234567890ABCDEF1234567890ABCDEF with reference
count of 1 is stored within 12/34/56/7890ABCDEF1234567890ABCDEF.1
file. This scheme scales comfortably to around 10e9 files. (**TBD**: On a
raw device, you would really want minimal filesystem semantics _here_ to do
that, but to cover correct flash rewrite semantics offloading that
logic elsewhere seems like a sane thing to do in any case. I have to think
about this more.)

Another feature required from the storage layer is ability to atomically
give a human-readable name to a particular hash-identified data block. This
can be implemented using a symlink and it is used to keep track of tree
roots in the forest layer.

**NOTE**: It is possible to determine both the name of the block, and the
current reference count of the block based on the content of blocks in
the filesystem, in a recovery scenario. This is because the forest layer
stores the type in the variable-length data as well, and therefore
superblock can be found, and reference counts can be counted from there.

## Forest layer ##

Forest layer consists of a forest of nested B+ trees. As the storage is
left out of scope, single nested B+ tree is enough to represent the state
of a filesystem. Individual B+ trees are single directories, with:

* header:
	* last modified, last create, last delete timestamp (UTC)

* key = 32-bit MurmurHash3 of filename concatenated with the actual
  filename, and

* value = metadata (usually associated with UNIX inodes) + reference to:

	* another directory,
	* block of file data, or
	* B+tree of pointers to file data

In addition to the 'content' nested B+ tree noted above, there is probably
need for others (to store snapshots, remote synchronization state and
on-demand remotely accessed blocks' use).

### Constraints of the design ###

* Hard-links with more than 1 link cannot be supported; 1 link, that is
normal files, are obviously fine.

* Storage efficiency for small files and directories is suboptimal (B+ tree
per subdirectory; block per file).

## Synchronization layer ##

The merging of remote state and local concurrent writes works the same way
(**TBD** think if local concurrent writes are actually sane design). A
nested B+ tree (=filesystem state) is compared recursively, and merge done
using last-writer-wins semantics.

Given operations + (add), u (update), - (delete), conflicts are handled as
follows:

* (+/u, +/u): LWW.
* (+/u, -): LWW based on entry mtime timestamp <> directory delete timestamp.
* (-, -): Delete.

Writes are atomic as far as finished writes to a file are concerned; given
two writers, _one of the writers'_ perspective wins and the other one's
disappears. Also, as long as the file is kept open, the view to a file
stays consistent.

Typically only a (subset of) content nested B+ tree is synchronized; the
other (nested) B+ trees are used for local bookkeeping.

## Notes on 'tinfoil hat' aspects ##

Design:

* Derive encryption key using PBKDF2 and 'large' number of rounds from
  password.

* Use SHA256 _plaintext_ block data *and* encryption key to acquire block
  identifiers (With this combination, we can do de-duplication yet without
  encryption key, even hashes of the plaintext data are not known).

* AES GCM all data in the blocks with per-block IV using the encryption
  key. Optionally compress/pad them as well. We will opportunistically try
  to compress everything as well as LZ4 is virtually free, and if size
  decreases, we use it for stored block.

These are the security characteristics of the current design:

* Without key:

	* Data content is opaque.

	* Data cannot be tampered with, with the exception of history replay
      attacks; as tree node roots pointers are not encrypted, a more
      historic root MAY be presented. But obviously blocks can be also
      deleted and mount made read-only or something, so the I do not
      consider the attack significant.

	* Size of blocks however IS visible. As an option, stored data blocks
    are rounded up arbitrarily much. in size (optionally) to prevent
    fingerprinting by file size (mainly applicable to small files).
