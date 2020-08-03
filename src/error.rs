use compressed_vec::error::CodingError;

#[derive(Debug)]
pub enum DataError {
    OutOfBufferSpace,
    MaybeDouble,               // Could not parse as integer, try double
    NeedBiggerDictionaryVal,   // dictionary encoding value type is too small
    WrongColumnType(u32),
    CodingError(CodingError),
}

impl From<CodingError> for DataError {
    fn from(err: CodingError) -> Self {
        match err {
            CodingError::NotEnoughSpace => DataError::OutOfBufferSpace,
            _ => DataError::CodingError(err),
        }
    }
}
