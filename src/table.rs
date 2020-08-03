/// Defines tables, schemas, batches, columns.
///
/// A table is a set of batches with a schema (numbered list of columns with info for each column).
/// The schema has column name, type, and ID, and is the authoritative source for column information.
///
/// A batch consists of one vector for each populated column.  Each batch may contain any subset of the table schema.
/// This is to handle sparse data well.
///
use std::collections::HashMap;

use crate::error::DataError;
use crate::string_index::{StringIndexMap, StringIndexType};
use crate::query::{ColumnFilter, FilterTranslateResult, Operator};

use ahash::RandomState;
use compressed_vec::filter::*;
use compressed_vec::vector;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    U64Column,
    DictStringColumn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub id: u32,
    pub name: String,
    pub typ: ColumnType,
}

impl ColumnInfo {
    pub fn new(id: u32, name: String, typ: ColumnType) -> Self {
        Self { id, name, typ }
    }
}

/// A schema correlates column name, ID, and other info.  It allows only appends of new columns.
/// It supports the following mappings:
/// 1. Given column name, reveal ColumnInfo
/// 2. Given column ID, reveal ColumnInfo
///
/// Only one copy is meant to exist for the entire table, so it's not designed to be space efficient or anything.
#[derive(Debug, Clone)]
pub struct Schema {
    name_to_info: HashMap<String, ColumnInfo, RandomState>,
    id_to_info: Vec<ColumnInfo>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            name_to_info: HashMap::default(),
            id_to_info: Vec::new(),
        }
    }

    pub fn add_column(&mut self, name: String, typ: ColumnType) {
        if !self.name_to_info.contains_key(&name) {
            let info = ColumnInfo::new(self.id_to_info.len() as u32, name, typ);
            self.id_to_info.push(info.clone());
            self.name_to_info.insert(info.name.clone(), info);
        }
    }

    pub fn info_from_name(&self, name: &str) -> Option<&ColumnInfo> {
        self.name_to_info.get(name)
    }

    pub fn info_from_id(&self, id: usize) -> Option<&ColumnInfo> {
        self.id_to_info.get(id)
    }

    /// Returns the ColumnInfos in ID order
    pub fn infos(&self) -> &Vec<ColumnInfo> {
        &self.id_to_info
    }
}

/// Holds all the metadata incld dictionary for a column/vector in a batch
pub struct BatchColumn {
    pub col_id: u32,
    col_type: ColumnType,
    pub dict: Option<StringIndexType>,
    vect_bytes: Vec<u8>
}

impl BatchColumn {
    // TODO: take D: Into<Option<StringIndexType>> instead, so users don't need to add Some()
    pub fn new(col_id: u32, col_type: ColumnType, dict: Option<StringIndexType>, vect_bytes: Vec<u8>) -> Self {
        Self { col_id, col_type, dict, vect_bytes }
    }

    /// Size of dictionary alone without encoded vector
    pub fn dict_size(&self) -> usize {
        self.dict.as_ref().map(|si| si.bytes_estimate()).unwrap_or(0)
    }

    /// Size of dictionary if any plus encoded vector
    pub fn bytes_estimate(&self) -> usize {
        self.dict_size() + self.vect_bytes.len()
    }

    /// Number of entries in dictionary including null entry
    pub fn num_dict_entries(&self) -> usize {
        self.dict.as_ref().map(|si| si.len()).unwrap_or(0)
    }

    pub fn debug_string(&self) -> String {
        format!("BatchColumn(id={:?} col_type={:?} dict: {:?} entries, {:?} bytes  total bytes: {:?})",
                self.col_id, self.col_type, self.num_dict_entries(), self.dict_size(), self.bytes_estimate())
    }

    pub fn vector_bytes(&self) -> &[u8] {
        &self.vect_bytes[..]
    }
}

/// A batch is a bunch of columns each of which has one vector and maybe one dictionary.
/// Data in it should be immutable.
/// TODO: Store Vec<BatchColumn> of only non-null cols and binary search for right column.
/// Saves a lot of space for large schemas like CK
pub struct Batch {
    // For now this is OK, but we probably need a more sparse representation for really large schemas like CloudKit.
    // One position per column, None if it is empty
    columns: Vec<Option<BatchColumn>>
}

impl Batch {
    pub fn new() -> Self {
        Self { columns: Vec::new() }
    }

    pub fn add_empty_column(&mut self) {
        self.columns.push(None);
    }

    pub fn add_col(&mut self, bc: BatchColumn) {
        // ID being added should be same as number of columns... for now
        assert_eq!(bc.col_id as usize, self.columns.len());
        self.columns.push(Some(bc));
    }

    pub fn num_columns(&self) -> usize {
        self.columns.iter().filter(|bc| bc.is_some()).count()
    }

    /// Estimate of total bytes used by this batch
    pub fn bytes_estimate(&self) -> usize {
        self.columns.len() * std::mem::size_of::<Option<BatchColumn>>() +
        self.columns.iter()
            .map(|bc| bc.as_ref().map(|b| b.bytes_estimate()).unwrap_or(0))
            .sum::<usize>()
    }

    pub fn batch_col_from_id(&self, id: u32) -> Option<&BatchColumn> {
        match self.columns.get(id as usize) {
            Some(opt_bc) => opt_bc.as_ref(),
            None         => None,
        }
    }

    /// Translates a set of high level filters to an Operator that operates on a filter iterator.
    /// This is typically the first step in query processing of a batch.
    pub fn get_filter_operator(&self, filters: &Vec<Box<dyn ColumnFilter>>) -> Result<Operator, DataError> {
        // First, translate each ColumnFilter to low level SectionFilter
        let translated: Vec<FilterTranslateResult> = filters.iter().map(|cf| {
            self.batch_col_from_id(cf.col_id())
                .map(|bc_ref| cf.translate(bc_ref))
                .unwrap_or(FilterTranslateResult::NoMatch)
        }).collect();

        // Error out if wrong column type detected
        // Return empty operator if NoMatch found or we don't have column in this batch for the ID
        // Otherwise, convert to VectorFilters
        // For now just handle all Equals
        let mut vect_filters = Vec::<VectorFilter<'_, EqualsSink<u32>, u32>>::new();
        for t in translated {
            match t {
                FilterTranslateResult::WrongColType(id) =>
                    return Err(DataError::WrongColumnType(id)),
                FilterTranslateResult::NoMatch =>
                    return Ok(EMPTY_FILTER.into()),
                FilterTranslateResult::Equals { col_id, filt } => {
                    // At this point we know there is a real column
                    let bc = self.batch_col_from_id(col_id)
                                 .expect(format!("Cannot get column {:?}", col_id).as_str());

                    // Validate this is a string column.  TODO: support other col types
                    assert_eq!(bc.col_type, ColumnType::DictStringColumn);

                    // Create VectorFilter
                    let reader = vector::VectorReader::<u32>::try_new(bc.vector_bytes())?;
                    let vect_filt = reader.filter_iter(filt);

                    // TODO: if column is sparse, append to front of filters?  or sort it later?
                    vect_filters.push(vect_filt);
                },
                // If anything else is left, throw an exception
            }
        }

        Ok(MultiVectorFilter::new(vect_filters).into())
    }

    pub fn print_batch_details(&self, schema: &Schema) {
        println!("Batch details:");
        println!("\tLen of columns Vec: {:?}  non-null columns: {:?}   bytes: {:?}",
                 self.columns.len(), self.num_columns(), self.columns.len() * std::mem::size_of::<BatchColumn>());
        // Get size of all nonnull columns, sorted in ascending order by size
        let mut size_and_id: Vec<(u32, usize)> =
            self.columns.iter().enumerate()
                .filter(|(_i, bc)| bc.is_some())
                .map(|(i, bc)| (i as u32, bc.as_ref().unwrap().bytes_estimate()))
                .collect();
        size_and_id.sort_by_key(|&(_, bytes)| bytes);

        // Report on last 10 (biggest) columns
        let num_cols = size_and_id.len();
        let mut top10_bytes = 0;
        println!("\nTop ten columns by size:");
        size_and_id[(num_cols - 10)..].iter().for_each(|(id, _size)| {
            let col_name = &schema.info_from_id(*id as usize).unwrap().name;
            let col = self.columns[*id as usize].as_ref().unwrap();
            println!("{}\t{}", col_name, col.debug_string());
            top10_bytes += col.bytes_estimate();
        });
        println!("\tTotal size of top 10 columns: {:?}", top10_bytes);

        // Middle ten and smallest ten
        println!("\nBottom ten columns by size:");
        size_and_id[..10].iter().for_each(|(id, _size)| {
            let col_name = &schema.info_from_id(*id as usize).unwrap().name;
            println!("{}\t{}", col_name, self.columns[*id as usize].as_ref().unwrap().debug_string());
        });
    }
}

pub struct Table {
    pub name: String,
    schema: Schema,
    batches: Vec<Batch>,
}

impl Table {
    /// Creates a new table. TODO: have ingester reference table and add columns here instead
    pub fn new(name: String) -> Self {
        Self { name, schema: Schema::new(), batches: Vec::new() }
    }

    pub fn add_column(&mut self, name: String, typ: ColumnType) {
        self.schema.add_column(name, typ);
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn get_id_from_col_name(&self, col_name: &str) -> Option<u32> {
        self.schema.info_from_name(col_name).map(|info| info.id)
    }

    pub fn add_batch(&mut self, batch: Batch) {
        self.batches.push(batch);
    }

    /// Calls get_filter_operator and calls your closure for each operator/batch.
    /// Starting point for filtering the whole table.
    pub fn filter<F>(&self, filters: &Vec<Box<dyn ColumnFilter>>,
                     mut op_func: F)
    where F: FnMut(&mut Operator) {
        for batch in &self.batches {
            let mut op = batch.get_filter_operator(filters).expect("Error getting filter operator");
            op_func(&mut op);
        }
    }

    /// Calls your closure for each batch, subject to a batch limit
    pub fn each_batch<F>(&self, batch_limit: usize, batch_func: F) where F: Fn(&Batch) {
        self.batches.iter().take(batch_limit).for_each(|b| batch_func(b));
    }
}