/// string_index contains `StringIndex`, a two-way String to index mapping
/// for dictionary encoding.
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};

use ahash::{AHasher, RandomState};
use enum_dispatch::enum_dispatch;
use nested::{Iter, Nested};
use num::{PrimInt, ToPrimitive, Unsigned};

use crate::error::*;


pub fn calculate_hash(str: &str) -> u64 {
    let mut s = AHasher::default();
    str.hash(&mut s);
    s.finish()
}

/// A StringIndex combines Nested<String> for efficient string storage
/// with a HashMap to allow O(1) lookup of the index given a string.
/// It allows for space and time efficient two-way string <-> index lookups
/// for dictionary encoding and decoding.
/// A null string is pushed as an initial null value so code 0 always == ""
/// How can we be fast AND really space efficient?
/// We only store the string once, in Nested.  To look up index given
/// a string, we use `raw_entry_mut.from_hash` to custom calculate a hash
/// given a string input, and use hash of the string stored in Nested
/// for comparisons.  This requires the custom lookup plus a custom Hasher...
/// Only the index of the string is stored.
/// However above is WAY too complex, so instead we use HashMap<u64, u32>
/// to map u64 hash of string value to the index.  Much simpler.
#[derive(Debug, Clone)]
pub struct StringIndex {
    // Lookup a string by index
    idx_strs: Nested<String>,
    // Lookup index by string
    strs_idx: HashMap<u64, u32, RandomState>,
}

impl StringIndex {
    pub fn new() -> Self {
        let mut si = Self { idx_strs: Nested::new(), strs_idx: HashMap::default() };
        si.lookup_maybe_insert("");
        si
    }

    /// Creates a FrozenStringIndex from this
    pub fn to_frozen(&self) -> StringIndexType {
        let num_str_bytes = self.idx_strs.data().len();
        if num_str_bytes < 64 * 1024 {
            // <= 254 elements, < 64KB str size
            if self.len() <= 254 {
                FrozenStringIndex::<u8, u16>::from_nested(&self.idx_strs).unwrap().into()
            } else {
                // Must be < 64K elements since size < 64K
                FrozenStringIndex::<u16, u16>::from_nested(&self.idx_strs).unwrap().into()
            }
        } else {
            // For now try u16, u32
            if self.len() <= 65534 {
                FrozenStringIndex::<u16, u32>::from_nested(&self.idx_strs).unwrap().into()
            } else {
                unimplemented!("Sorry we don't support > 64K elements right now")
            }
        }
    }

    /// Number of elements in the index, including the initial null value
    pub fn len(&self) -> usize {
        self.idx_strs.len()
    }

    /// Inserts a String into the index, if it doesn't exist, and looks up the index.
    /// Takes ownership of the string.
    pub fn lookup_maybe_insert(&mut self, s: &str) -> u32 {
        let h = calculate_hash(s);
        let next_index = self.idx_strs.len() as u32;
        let mut push_s = false;
        let index = self.strs_idx.entry(h)
            .or_insert_with(|| {
                push_s = true;
                next_index
            });
        if push_s {
            self.idx_strs.push(s);
        }
        *index
    }

    /// Looks up index given a string.  O(1), must be fast as used in hot query path
    pub fn lookup(&self, s: &str) -> Option<u32> {
        self.strs_idx.get(&calculate_hash(s)).map(|&i| i)
    }

    /// Returns an iterator over all the strings present
    pub fn strings_iter(&self) -> Iter<String> {
        self.idx_strs.iter()
    }

    /// Shrinks data structures to use as little memory as possible
    pub fn shrink(&mut self) {
        self.strs_idx.shrink_to_fit();
    }

    pub fn reset(&mut self) {
        self.idx_strs.clear();
        self.strs_idx.clear();
        self.lookup_maybe_insert("");
    }

    pub fn bytes_estimate(&self) -> usize {
        self.idx_strs.data().len() + self.idx_strs.indices().len() * std::mem::size_of::<usize>()
            + 24 * self.strs_idx.capacity()
    }
}

/// Common traits for a String <-> Index/Code map
#[enum_dispatch]
pub trait StringIndexMap {
    /// Number of strings or entries in the map
    fn len(&self) -> usize;

    /// O(1) retrieve a string given an index
    fn get_str(&self, i: usize) -> Option<&str>;

    /// Approx O(1) retrieve an index, given a string
    fn get_index(&self, s: &str) -> Option<usize>;

    /// Gets number of bytes of memory used by this map
    fn bytes_estimate(&self) -> usize;
}

#[enum_dispatch(StringIndexMap)]
#[derive(Debug)]
pub enum StringIndexType {
    FrozenStringIndex1x2(FrozenStringIndex<u8, u16>),   // Up to 254 elements, < 64KB total strings
    FrozenStringIndex2x2(FrozenStringIndex<u16, u16>),  // Up to 64K elements, < 64KB total strings
    FrozenStringIndex2x4(FrozenStringIndex<u16, u32>),  // Up to 64K elements, < 2GB total strings
}

/// Load factor for FrozenStringIndex open addressing hashing.
const LOAD_FACTOR: f32 = 0.75;

/// Very space-efficient two-way String <-> index, immutable hash map.  Approx O(1) lookups both ways.
///
/// This is a MUCH more space efficient, two-way String <-> index map, with parameterized index size
/// for more efficient storage of smaller maps, compared to StringIndex, which uses > 40 bytes overhead for each entry.
/// The main savings comes from using u16 etc instead of an entire HashMap Entry, with customized hash
/// and open address, linear probing, plus a Nest-style byte storage for consecutive strings.
/// For u16, u16 FrozenStringIndex, overhead is only 4 bytes per entry.
/// Key difference is this is NOT a growable map, it is built from a StringIndex.
///
/// There are two type params: I = uint type used for the hash index, O = uint offset type used for string offset
/// tracking.  I is sized to number of strings;  O is sized to total size of all strings.
// TODO: consider using smallvec for inline vecs.  Might not be worth it, it doesn't reduce space that much.
pub struct FrozenStringIndex<I, O>
where I: Unsigned + PrimInt + ToPrimitive + Clone,
      O: Unsigned + PrimInt + ToPrimitive {
    hash_index: Vec<I>,
    str_bytes: String,
    offsets: Vec<O>,
}

impl<I, O> FrozenStringIndex<I, O>
where I: Unsigned + PrimInt + ToPrimitive + Clone,
      O: Unsigned + PrimInt + ToPrimitive {
    /// Returns Ok(FrozenStringIndex) or Err(OutOfBufferSpace)
    pub fn from_nested(nested: &Nested<String>) -> Result<Self, DataError> {
        // validate size of I/O are OK
        let offsets = nested.indices();
        let bytes = nested.data();
        if O::max_value().to_usize().unwrap() < bytes.len() { return Err(DataError::OutOfBufferSpace) }
        if I::max_value().to_usize().unwrap() < offsets.len() { return Err(DataError::OutOfBufferSpace) }

        // Size of hash_index is number of elements divided by load factor, rounded up
        let hash_size = (offsets.len() as f32 / LOAD_FACTOR).ceil() as usize;

        let mut s = Self {
            hash_index: Vec::with_capacity(hash_size),
            str_bytes:  bytes.clone(),
            offsets:    Vec::with_capacity(offsets.len() - 1),
        };
        // Fill hash with max_value, which is marker for empty (for now)
        s.hash_index.resize(hash_size, I::max_value());
        // Skip first offset as it's always 0 (start of first string is always at 0)
        offsets.iter().skip(1).for_each(|&o| { s.offsets.push(O::from(o).unwrap()); });

        // One by one, populate the hash index
        for (i, key) in nested.iter().enumerate() {
            s.add_str_to_hash(key, I::from(i).unwrap());
        }
        Ok(s)
    }

    fn find_initial_hash_slot(&self, s: &str) -> usize {
        let hash = calculate_hash(s);
        hash as usize % self.hash_index.len()
    }

    fn add_str_to_hash(&mut self, s: &str, index: I) {
        let init_slot = self.find_initial_hash_slot(s);
        let mut slot = init_slot;

        // Loop until we find an unoccupied slot and write it
        // The index is always larger than number of entries, so this should never infinite loop
        while self.hash_index[slot] != index {
            // If it is unoccupied, then occupy it and return  :)
            if self.hash_index[slot] == I::max_value() {
                self.hash_index[slot] = index;
                break;
            }

            // Occupied by something else already, keep going
            slot = (slot + 1) % self.hash_index.len();
            assert!(slot != init_slot);
        }
    }
}

impl<I, O> StringIndexMap for FrozenStringIndex<I, O>
where I: Unsigned + PrimInt + ToPrimitive + Clone + Debug,
      O: Unsigned + PrimInt + ToPrimitive + Debug {
    /// Number of strings or entries in the map
    #[inline]
    fn len(&self) -> usize {
        self.offsets.len()
    }

    /// O(1) retrieve a string given an index
    #[inline]
    fn get_str(&self, i: usize) -> Option<&str> {
        if i < self.offsets.len() {
            let start_off = if i == 0 { 0 } else { self.offsets[i - 1].to_usize().unwrap() };
            let end_off = self.offsets[i].to_usize().unwrap();
            Some(&self.str_bytes[start_off..end_off])
        } else {
            None
        }
    }

    /// Approx O(1) retrieve an index, given a string
    #[inline]
    fn get_index(&self, s: &str) -> Option<usize> {
        let initial_slot = self.find_initial_hash_slot(s);
        let mut slot = initial_slot;

        // Given this is not a resizable map and always has 1/LOAD_FACTOR free slots, should always terminate
        loop {
            // Get string index from hash slot
            let index = self.hash_index[slot];

            // If we've reached an empty slot, then just return None (if it was equal we would've found it)
            if index == I::max_value() { break None }

            // Compare strings to determine match
            let index_usize = index.to_usize().unwrap();
            if self.get_str(index_usize).unwrap() == s { break Some(index_usize) }

            slot = (slot + 1) % self.hash_index.len();
            assert!(slot != initial_slot);  // should never happen
        }
    }

    fn bytes_estimate(&self) -> usize {
        self.hash_index.len() * std::mem::size_of::<I>() +
        self.str_bytes.len() +
        self.offsets.len() * std::mem::size_of::<O>() +
        std::mem::size_of::<Self>()
    }
}

impl<I, O> fmt::Debug for FrozenStringIndex<I, O>
where I: Unsigned + PrimInt + ToPrimitive + Clone + Debug,
      O: Unsigned + PrimInt + ToPrimitive + Debug {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let elems_to_display = 10.min(self.len());
        f.debug_struct("FrozenStringIndex")
         .field("len", &self.len())
         .field("hashes", &&self.hash_index[..elems_to_display])
         .field("str_bytes", &&self.str_bytes[..30.min(self.str_bytes.len())])
         .field("offsets", &&self.offsets[..elems_to_display])
         .field("num_bytes", &self.bytes_estimate())
         .finish()
    }
}

// TODO: can't figure out how to integrate this into FrozenStringIndex yet.
// Returning iterators remains a major pain in Rust
pub struct FrozenIndexIter<'a, M>
where M: StringIndexMap {
    index: &'a M,
    i: usize,
}

impl<'a, M> FrozenIndexIter<'a, M>
where M: StringIndexMap {
    pub fn new(index: &'a M) -> Self {
        Self { index, i: 0 }
    }
}

impl<'a, M> Iterator for FrozenIndexIter<'a, M>
where M: StringIndexMap {
    type Item = &'a str;
    fn next(&mut self) -> Option<Self::Item> {
        let s = self.index.get_str(self.i);
        if s.is_some() { self.i += 1 }
        s
    }
}

#[test]
fn test_small_frozen_index() {
    let mut si = StringIndex::new();
    si.lookup_maybe_insert("apple");    // 1
    si.lookup_maybe_insert("bananas");  // 2
    si.lookup_maybe_insert("peach");
    si.lookup_maybe_insert("orange");
    si.lookup_maybe_insert("guava");
    assert_eq!(si.lookup("apple").unwrap(), 1);

    assert_eq!(si.len(), 6);  // above 5 plus null

    let frozen = si.to_frozen();
    dbg!(&frozen);
    dbg!(std::mem::size_of::<FrozenStringIndex<u8, u16>>());

    assert_eq!(frozen.len(), 6);
    assert_eq!(frozen.get_index("apple").unwrap(), 1);
    assert_eq!(frozen.get_index("peach").unwrap(), 3);
    assert_eq!(frozen.get_index("guava").unwrap(), 5);

    // Not in there
    assert_eq!(frozen.get_index("fuji apple"), None);
    assert_eq!(frozen.get_index("orangey"), None);

    assert_eq!(frozen.get_str(2).unwrap(), "bananas");
}
