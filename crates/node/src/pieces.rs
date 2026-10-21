//! Pieces (see GLOSSARY.md, Content): the piece length rule, hashing a
//! file into its content hash and piece hashes, reading a piece from disk
//! only while the file still matches its entry, and the staging file a fetch
//! fills, from what an earlier try left and from this node's own files.

use std::collections::{HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{self, Read};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use iroh_blobs::Hash;

use crate::index::{FileMeta, Stat, TEMP_PREFIX};

/// The smallest piece length.
pub const MIN_PIECE_LENGTH: u32 = 128 << 10;

/// The largest piece length.
pub const MAX_PIECE_LENGTH: u32 = 16 << 20;

/// Files are split into at most this many pieces, up to the largest piece
/// size.
pub const MAX_PIECES: u64 = 2048;

/// The piece length for a file of `size` bytes: the smallest power of two from
/// [`MIN_PIECE_LENGTH`] to [`MAX_PIECE_LENGTH`] that splits it into at most
/// [`MAX_PIECES`] pieces. 0 for an empty file, which has no pieces. Depends
/// on the size alone, so the same content always has the same piece hashes.
pub fn piece_length(size: u64) -> u32 {
    if size == 0 {
        return 0;
    }
    let mut length = MIN_PIECE_LENGTH;
    while length < MAX_PIECE_LENGTH && size.div_ceil(u64::from(length)) > MAX_PIECES {
        length *= 2;
    }
    length
}

/// A file's content hash and piece hashes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hashed {
    /// BLAKE3 of the whole content, as `b3sum` prints it.
    pub content: Hash,
    pub piece_length: u32,
    /// BLAKE3 of each piece, in order. The last piece may be short.
    pub pieces: Vec<Hash>,
}

/// Hashes content fed to it in order into its whole hash and piece hashes, in
/// one pass.
#[derive(Debug)]
pub struct Hasher {
    whole: blake3::Hasher,
    piece: blake3::Hasher,
    piece_length: u32,
    /// Bytes in the current piece so far.
    filled: u64,
    pieces: Vec<Hash>,
}

impl Hasher {
    /// A hasher for content of `size` bytes, which sets the piece length.
    pub fn new(size: u64) -> Self {
        Self {
            whole: blake3::Hasher::new(),
            piece: blake3::Hasher::new(),
            piece_length: piece_length(size),
            filled: 0,
            pieces: Vec::new(),
        }
    }

    pub fn update(&mut self, mut data: &[u8]) {
        self.whole.update(data);
        while !data.is_empty() {
            let room = u64::from(self.piece_length) - self.filled;
            let take = data.len().min(usize::try_from(room).unwrap_or(usize::MAX));
            self.piece.update(&data[..take]);
            self.filled += take as u64;
            if self.filled == u64::from(self.piece_length) {
                self.pieces.push(self.piece.finalize().into());
                self.piece.reset();
                self.filled = 0;
            }
            data = &data[take..];
        }
    }

    pub fn finish(mut self) -> Hashed {
        if self.filled > 0 {
            self.pieces.push(self.piece.finalize().into());
        }
        Hashed {
            content: self.whole.finalize().into(),
            piece_length: self.piece_length,
            pieces: self.pieces,
        }
    }
}

/// Hashes everything `reader` yields, which the caller expects to be `size`
/// bytes (it sets the piece length). Returns the hash and the number of bytes
/// read, which differs from `size` if the content changed meanwhile.
pub fn hash_reader(size: u64, mut reader: impl Read) -> io::Result<(Hashed, u64)> {
    let mut hasher = Hasher::new(size);
    let mut buf = vec![0; 64 << 10];
    let mut read = 0;
    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                hasher.update(&buf[..n]);
                read += n as u64;
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok((hasher.finish(), read))
}

/// Opens the file at `rel` under `root` if it is a regular file reached
/// through real directories only (no symlink on the way), and the same file
/// after opening as before (nothing swapped in). `None` if not.
fn open_regular(root: &Path, rel: &str) -> io::Result<Option<(File, fs::Metadata)>> {
    let parts: Vec<&str> = rel.split('/').collect();
    let mut dir = PathBuf::from(root);
    for part in &parts[..parts.len() - 1] {
        dir.push(part);
        if !fs::symlink_metadata(&dir)?.is_dir() {
            return Ok(None);
        }
    }
    let abs = root.join(rel);
    let before = fs::symlink_metadata(&abs)?;
    if !before.is_file() {
        return Ok(None);
    }
    let file = File::open(&abs)?;
    let meta = file.metadata()?;
    if (meta.dev(), meta.ino()) != (before.dev(), before.ino()) {
        return Ok(None);
    }
    Ok(Some((file, meta)))
}

/// Where piece `index` of the file `meta` describes starts, and its length.
pub(crate) fn span(meta: &FileMeta, index: u32) -> (u64, usize) {
    let offset = u64::from(index) * u64::from(meta.piece_length);
    let len = u64::from(meta.piece_length).min(meta.size.saturating_sub(offset));
    (offset, len as usize)
}

/// Reads piece `index` of the file at `rel` under `root`, if the file is
/// still as `entry` describes it (see [`open_regular`]): with the entry's
/// size and modification time, and a piece that hashes to the entry's.
/// `None` if not.
pub(crate) fn read_piece(
    root: &Path,
    rel: &str,
    entry: &FileMeta,
    index: u32,
) -> io::Result<Option<Vec<u8>>> {
    let Some((file, meta)) = open_regular(root, rel)? else {
        return Ok(None);
    };
    if Stat::of(&meta) != entry.stat() {
        return Ok(None);
    }
    let (offset, len) = span(entry, index);
    let mut piece = vec![0; len];
    file.read_exact_at(&mut piece, offset)?;
    if Hash::new(&piece) != entry.pieces[index as usize] {
        return Ok(None);
    }
    Ok(Some(piece))
}

/// The temporary file version `content` of `path` is fetched into: at the
/// folder root (so nothing is written below it before the index task has
/// checked the way), named after the path and version, so a fetch that
/// stopped finds it again.
pub fn staging_path(root: &Path, path: &str, content: Hash) -> PathBuf {
    let mut hasher = blake3::Hasher::new();
    hasher.update(path.as_bytes());
    hasher.update(&[0]);
    hasher.update(content.as_bytes());
    root.join(format!(
        "{TEMP_PREFIX}{}",
        &hasher.finalize().to_hex()[..16]
    ))
}

/// One of this node's files that pieces can be copied from: its path under
/// the folder root and its entry.
pub type Source = (String, Arc<FileMeta>);

/// A staging file being filled (see [`staging_path`]), and the pieces it
/// still lacks.
#[derive(Debug)]
pub struct Staged {
    pub(crate) file: Arc<File>,
    pub(crate) missing: VecDeque<u32>,
    /// Bytes copied from this node's own files.
    pub copied: u64,
    /// Bytes still to fetch.
    pub to_fetch: u64,
}

impl Staged {
    /// Opens the staging file `temp` for version `meta` (or creates it at
    /// full length), keeps the pieces already there that match the piece
    /// list, and copies missing pieces from this node's files under `root`,
    /// trying `sources` in order. Every copied piece is checked against the
    /// piece hashes before it is written. Blocking.
    pub fn open(temp: &Path, meta: &FileMeta, root: &Path, sources: &[Source]) -> io::Result<Self> {
        let (file, have) = open_staged(temp, meta)?;
        let mut missing: VecDeque<u32> = (0..meta.pieces.len() as u32)
            .filter(|i| !have[*i as usize])
            .collect();
        let copied = copy_local(&file, meta, root, sources, &mut missing)?;
        let to_fetch = missing.iter().map(|&i| span(meta, i).1 as u64).sum();
        Ok(Self {
            file: Arc::new(file),
            missing,
            copied,
            to_fetch,
        })
    }

    /// Whether every piece is there.
    pub fn is_complete(&self) -> bool {
        self.missing.is_empty()
    }

    /// Syncs the staging file to disk. Blocking.
    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_all()
    }
}

/// Whether `name` (a file name at a folder root) is a staging file's (see
/// [`staging_path`]), not some other temporary file of this node's.
pub fn is_staging_name(name: &str) -> bool {
    name.strip_prefix(TEMP_PREFIX)
        .is_some_and(|rest| rest.len() == 16 && rest.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// Copies the `missing` pieces of `meta` that `sources` (files under `root`)
/// hold into `file`, taking them out of `missing`; a piece that doesn't
/// hash to the piece hashes is left missing. Returns the bytes copied.
fn copy_local(
    file: &File,
    meta: &FileMeta,
    root: &Path,
    sources: &[Source],
    missing: &mut VecDeque<u32>,
) -> io::Result<u64> {
    if missing.is_empty() || sources.is_empty() {
        return Ok(0);
    }
    // For each wanted hash, the first source holding it and where.
    let mut found: HashMap<Hash, Option<(usize, u32)>> = missing
        .iter()
        .map(|&i| (meta.pieces[i as usize], None))
        .collect();
    for (s, (_, source)) in sources.iter().enumerate() {
        for (i, hash) in source.pieces.iter().enumerate() {
            if let Some(slot @ None) = found.get_mut(hash) {
                *slot = Some((s, i as u32));
            }
        }
    }
    let mut opened: HashMap<usize, Option<File>> = HashMap::new();
    let mut piece = Vec::new();
    let mut copied = 0;
    let mut left = VecDeque::new();
    for index in missing.drain(..) {
        let expected = meta.pieces[index as usize];
        let Some((s, at)) = found[&expected] else {
            left.push_back(index);
            continue;
        };
        let (rel, source) = &sources[s];
        let src = opened
            .entry(s)
            .or_insert_with(|| open_regular(root, rel).ok().flatten().map(|(file, _)| file));
        let (to, len) = span(meta, index);
        piece.resize(len, 0);
        let read = src
            .as_ref()
            .is_some_and(|src| src.read_exact_at(&mut piece, span(source, at).0).is_ok());
        if !read || Hash::new(&piece) != expected {
            left.push_back(index);
            continue;
        }
        file.write_all_at(&piece, to)?;
        copied += len as u64;
    }
    *missing = left;
    Ok(copied)
}

/// Opens (or creates) the temporary file for `meta`'s content at its full
/// length, and finds which of its pieces already match the piece hashes.
fn open_staged(temp: &Path, meta: &FileMeta) -> io::Result<(File, Vec<bool>)> {
    let mut have = vec![false; meta.pieces.len()];
    match fs::symlink_metadata(temp) {
        Ok(found) if !found.is_file() => {
            return Err(io::Error::other(format!(
                "{} is not a regular file",
                temp.display()
            )))
        }
        _ => {}
    }
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(temp)?;
    if file.metadata()?.len() == meta.size {
        let mut piece = vec![0; meta.piece_length as usize];
        for (i, expected) in meta.pieces.iter().enumerate() {
            let (offset, len) = span(meta, i as u32);
            file.read_exact_at(&mut piece[..len], offset)?;
            have[i] = Hash::new(&piece[..len]) == *expected;
        }
    } else {
        file.set_len(0)?;
        file.set_len(meta.size)?;
    }
    Ok((file, have))
}
