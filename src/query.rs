/// Filters and query primitives
///

use crate::string_index::StringIndexMap;
use crate::table::BatchColumn;

use compressed_vec::filter::*;
use enum_dispatch::enum_dispatch;
use regex::Regex;


/// Result of translating a high level filter to a low-level column-based SectionFilter
pub enum FilterTranslateResult {
    WrongColType(u32),   // ex string queries cannot be executed on number columns with no dictionary. col ID.
    NoMatch,         // No item could possibly match (no entry in dictionary)
    Equals {
        col_id: u32,
        filt: EqualsSink<u32>,
    },
}

/// All high level ColumnFilters operate on a column ID and must be able to translate to low level filters
pub trait ColumnFilter {
    fn col_id(&self) -> u32;

    // Translate a high level query to a BatchColumn-level result
    fn translate(&self, batch_col: &BatchColumn) -> FilterTranslateResult;
}

/// String equality
#[derive(Debug, Clone)]
pub struct StringEq {
    col_id: u32,
    pred: String,
}

impl StringEq {
    pub fn new(col_id: u32, pred: String) -> Self {
        Self { col_id, pred }
    }
}

impl ColumnFilter for StringEq {
    fn col_id(&self) -> u32 {
        self.col_id
    }

    #[inline]
    fn translate(&self, batch_col: &BatchColumn) -> FilterTranslateResult {
        // If column has no dict, then we can't do anything
        match &batch_col.dict {
            None => FilterTranslateResult::WrongColType(batch_col.col_id),
            Some(index) => {
                match index.get_index(&self.pred) {
                    None => FilterTranslateResult::NoMatch,
                    Some(i) => FilterTranslateResult::Equals {
                        col_id: batch_col.col_id,
                        filt: EqualsSink::new(&(i as u32)),
                    },
                }
            }
        }
    }
}

/// String RegEx match using RegEx crate
#[derive(Debug)]
pub struct StringRegex {
    col_id: u32,
    regex: Regex,
}

impl StringRegex {
    pub fn new(col_id: u32, pattern: &str) -> Self {
        Self {
            col_id,
            regex: Regex::new(pattern).unwrap(),   // Compile only at creation time!
        }
    }
}

impl ColumnFilter for StringRegex {
    fn col_id(&self) -> u32 {
        self.col_id
    }

    #[inline]
    fn translate(&self, batch_col: &BatchColumn) -> FilterTranslateResult {
        // If column has no dict, then we can't do anything
        match &batch_col.dict {
            None => FilterTranslateResult::WrongColType(batch_col.col_id),
            Some(index) => {
                // Remember this is an Iterator and thus Lazy...  also skip null string
                let mut matches = (1..index.len()).filter(|&i| {
                    index.get_str(i).map(|s| self.regex.is_match(s)).unwrap_or(false)
                });
                // For now we will just take the FIRST value
                // TODO: In the future we'd have to collect all of them which might be $$....  sigh
                match matches.next() {
                    None => FilterTranslateResult::NoMatch,
                    Some(i) => FilterTranslateResult::Equals {
                        col_id: batch_col.col_id,
                        filt: EqualsSink::new(&(i as u32)),
                    },
                }
            }
        }
    }
}


/// Query operators for each batch based on filter iterators
/// NOTE: enum_dispatch allows us to call methods on different implementations of a trait without boxing
/// or allocations, but using static dispatch for speed.
#[enum_dispatch]
pub trait BatchOperator {
    /// Counts filter matches
    fn count_matches(&mut self) -> usize;

    /// Prints out debug info on filtered records?
    fn debug_print(&self);
}

#[enum_dispatch(BatchOperator)]
pub enum Operator<'a> {
    EmptyFilter,
    MultiVectorEquals(MultiVectorFilter<'a, EqualsSink<u32>, u32>),
}

impl BatchOperator for EmptyFilter {
    fn count_matches(&mut self) -> usize { 0 }

    fn debug_print(&self) {
        println!("Empty filter!");
    }
}

impl<'a> BatchOperator for MultiVectorFilter<'a, EqualsSink<u32>, u32> {
    fn count_matches(&mut self) -> usize {
        count_hits(self) as usize
    }

    fn debug_print(&self) {
        // TODO: print out details of each filter.  TODO: add debugging
        println!("MultiVectorFilter");
    }
}
