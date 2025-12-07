// Copyright 2020-2023 Alibaba Group Holding Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::mem::ManuallyDrop;

use arrow_array::{make_array, ArrayRef as ArrowArrayRef};
use arrow_array::ffi::from_ffi as arrow_from_ffi;
use arrow_data::ffi::FFI_ArrowArray;
use arrow_schema::{self, DataType as ArrowDataType};
use arrow_schema::ffi::FFI_ArrowSchema;
use itertools::izip;
use polars_arrow::array::ArrayRef as PolarsArrayRef;
use polars_arrow::datatypes::{ArrowDataType as PolarsArrowDataType, Field as PolarsField};
use polars_core::frame::column::Column;
use polars_core::prelude as polars;
use polars_core::prelude::CompatLevel;
use polars_core::series::Series;
use serde_json::Value;

use vineyard::client::*;
use vineyard::ds::arrow::{Table, TableBuilder};
use vineyard::ds::dataframe::DataFrame as VineyardDataFrame;

/// Convert a Polars error to a Vineyard error, as orphan impls are not allowed
/// in Rust
///
/// Usage:
///
/// ```ignore
/// let x = polars::DataFrame::new(...).map_err(error)?;
/// ```
pub fn error(error: polars::PolarsError) -> VineyardError {
    VineyardError::invalid(format!("{}", error))
}

fn arrow_schema_to_polars(schema: FFI_ArrowSchema) -> polars_arrow::ffi::ArrowSchema {
    let schema = ManuallyDrop::new(schema);
    unsafe { std::ptr::read(&*schema as *const _ as *const polars_arrow::ffi::ArrowSchema) }
}

fn arrow_array_to_polars(array: FFI_ArrowArray) -> polars_arrow::ffi::ArrowArray {
    let array = ManuallyDrop::new(array);
    unsafe { std::ptr::read(&*array as *const _ as *const polars_arrow::ffi::ArrowArray) }
}

fn polars_schema_to_arrow(schema: polars_arrow::ffi::ArrowSchema) -> FFI_ArrowSchema {
    let schema = ManuallyDrop::new(schema);
    unsafe { std::ptr::read(&*schema as *const _ as *const FFI_ArrowSchema) }
}

fn polars_array_to_arrow(array: polars_arrow::ffi::ArrowArray) -> FFI_ArrowArray {
    let array = ManuallyDrop::new(array);
    unsafe { std::ptr::read(&*array as *const _ as *const FFI_ArrowArray) }
}

fn normalize_polars_arrow_dtype(dtype: &PolarsArrowDataType) -> PolarsArrowDataType {
    use polars_arrow::datatypes::ArrowDataType::*;
    match dtype {
        BinaryView => Binary,
        Utf8View => Utf8,
        other => other.clone(),
    }
}

fn polars_arrow_dtype_to_arrow(dtype: &PolarsArrowDataType) -> Result<ArrowDataType> {
    let field = PolarsField::new("field".into(), normalize_polars_arrow_dtype(dtype), true);
    let schema = polars_arrow::ffi::export_field_to_c(&field);
    let schema = polars_schema_to_arrow(schema);
    let field = arrow_schema::Field::try_from(&schema)
        .map_err(|e| VineyardError::invalid(format!("{}", e)))?;
    Ok(field.data_type().clone())
}

fn arrow_array_to_polars_array(array: ArrowArrayRef) -> Result<(PolarsArrayRef, polars::DataType)> {
    let data = array.to_data();
    let schema = FFI_ArrowSchema::try_from(data.data_type())
        .map_err(|e| VineyardError::invalid(format!("{}", e)))?;
    let c_array = FFI_ArrowArray::new(&data);

    let schema = arrow_schema_to_polars(schema);
    let field = unsafe { polars_arrow::ffi::import_field_from_c(&schema) }
        .map_err(|e| VineyardError::invalid(format!("{}", e)))?;
    let polars_array = unsafe {
        polars_arrow::ffi::import_array_from_c(arrow_array_to_polars(c_array), field.dtype.clone())
    }
    .map_err(|e| VineyardError::invalid(format!("{}", e)))?;

    let dtype = polars::DataType::from_arrow_dtype(polars_array.dtype());

    Ok((polars_array.into(), dtype))
}

fn polars_array_to_arrow_array(array: PolarsArrayRef) -> Result<ArrowArrayRef> {
    let dtype = normalize_polars_arrow_dtype(array.dtype());
    let field = PolarsField::new("field".into(), dtype, true);
    let schema = polars_arrow::ffi::export_field_to_c(&field);
    let c_array = polars_arrow::ffi::export_array_to_c(array.as_ref().to_boxed());

    let schema = polars_schema_to_arrow(schema);
    let array = polars_array_to_arrow(c_array);
    let data =
        unsafe { arrow_from_ffi(array, &schema) }.map_err(|e| VineyardError::invalid(format!("{}", e)))?;

    Ok(make_array(data))
}

#[derive(Debug, Default)]
pub struct DataFrame {
    meta: ObjectMeta,
    dataframe: polars::DataFrame,
}

impl_typename!(DataFrame, "vineyard::Table");

impl Object for DataFrame {
    fn construct(&mut self, meta: ObjectMeta) -> Result<()> {
        let ty = meta.get_typename()?;
        if ty == typename::<VineyardDataFrame>() {
            return self.construct_from_pandas_dataframe(meta);
        } else if ty == typename::<Table>() {
            return self.construct_from_arrow_table(meta);
        } else {
            return Err(VineyardError::type_error(format!(
                "cannot construct DataFrame from this metadata: {}",
                ty
            )));
        }
    }
}

register_vineyard_object!(DataFrame);

impl DataFrame {
    pub fn new_boxed(meta: ObjectMeta) -> Result<Box<dyn Object>> {
        let mut object = Box::<Self>::default();
        object.construct(meta)?;
        Ok(object)
    }

    fn construct_from_pandas_dataframe(&mut self, meta: ObjectMeta) -> Result<()> {
        vineyard_assert_typename(typename::<VineyardDataFrame>(), meta.get_typename()?)?;
        let dataframe = downcast_object::<VineyardDataFrame>(VineyardDataFrame::new_boxed(meta)?)?;
        let names = dataframe.names().to_vec();
        let columns: Vec<(PolarsArrayRef, polars::DataType)> = dataframe
            .columns()
            .iter()
            .map(|c| arrow_array_to_polars_array(c.array()))
            .collect::<Result<_>>()?;

        let series: Vec<Series> = names
            .iter()
            .zip(columns)
            .map(|(name, (column, _dtype))| {
                let chunks: Vec<PolarsArrayRef> = vec![column];
                let field =
                    polars_arrow::datatypes::Field::new(name.clone().into(), chunks[0].dtype().clone(), true);
                Series::try_from((&field, chunks)).map_err(error)
            })
            .collect::<Result<Vec<_>>>()?;
        self.meta = dataframe.metadata();
        let columns: Vec<Column> = series.into_iter().map(Column::from).collect();
        self.dataframe = polars::DataFrame::new(columns).map_err(error)?;
        Ok(())
    }

    fn construct_from_arrow_table(&mut self, meta: ObjectMeta) -> Result<()> {
        vineyard_assert_typename(typename::<Table>(), meta.get_typename()?)?;
        let table = downcast_object::<Table>(Table::new_boxed(meta)?)?;
        let schema = table.schema();
        let names = schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let mut columns: Vec<Vec<PolarsArrayRef>> = Vec::with_capacity(table.num_columns());
        for index in 0..table.num_columns() {
            let mut chunks = Vec::with_capacity(table.num_batches());
            for batch in table.batches() {
                let batch = batch.as_ref().as_ref();
                let chunk = batch.column(index);
                let (array, _dtype) = arrow_array_to_polars_array(chunk.clone())?;
                chunks.push(array);
            }
            columns.push(chunks);
        }
        let series: Vec<Series> = izip!(&names, columns)
            .map(|(name, chunks)| {
                let chunks: Vec<PolarsArrayRef> = chunks;
                let field =
                    polars_arrow::datatypes::Field::new(name.clone().into(), chunks[0].dtype().clone(), true);
                Series::try_from((&field, chunks)).map_err(error)
            })
            .collect::<Result<Vec<_>>>()?;
        self.meta = table.metadata();
        let columns: Vec<Column> = series.into_iter().map(Column::from).collect();
        self.dataframe = polars::DataFrame::new(columns).map_err(error)?;
        Ok(())
    }
}

impl AsRef<polars::DataFrame> for DataFrame {
    fn as_ref(&self) -> &polars::DataFrame {
        &self.dataframe
    }
}

/// Building a polars dataframe into a pandas-compatible dataframe.
pub struct PandasDataFrameBuilder {
    sealed: bool,
    names: Vec<String>,
    columns: Vec<Box<dyn Object>>,
}

impl ObjectBuilder for PandasDataFrameBuilder {
    fn sealed(&self) -> bool {
        self.sealed
    }

    fn set_sealed(&mut self, sealed: bool) {
        self.sealed = sealed;
    }
}

impl ObjectBase for PandasDataFrameBuilder {
    fn build(&mut self, _client: &mut IPCClient) -> Result<()> {
        if self.sealed {
            return Ok(());
        }
        self.set_sealed(true);
        Ok(())
    }

    fn seal(mut self, client: &mut IPCClient) -> Result<Box<dyn Object>> {
        self.build(client)?;
        let mut meta = ObjectMeta::new_from_typename(typename::<DataFrame>());
        meta.add_usize("__values_-size", self.names.len());
        meta.add_isize("partition_index_row_", -1);
        meta.add_isize("partition_index_column_", -1);
        meta.add_isize("row_batch_index_", -1);
        for (index, (name, column)) in self.names.iter().zip(self.columns).enumerate() {
            meta.add_value(
                &format!("__values_-key-{}", index),
                Value::String(name.into()),
            );
            meta.add_member(&format!("__values_-value-{}", index), column)?;
        }
        let metadata = client.create_metadata(&meta)?;
        DataFrame::new_boxed(metadata)
    }
}

impl PandasDataFrameBuilder {
    pub fn new(client: &mut IPCClient, dataframe: &polars::DataFrame) -> Result<Self> {
        let mut names = Vec::with_capacity(dataframe.width());
        let mut columns = Vec::with_capacity(dataframe.width());
        for column in dataframe.get_columns() {
            let series = column.as_materialized_series().rechunk(); // ensure a single chunk
            names.push(series.name().to_string());
            let arrow_array =
                polars_array_to_arrow_array(series.to_arrow(0, CompatLevel::oldest()))?;
            columns.push(arrow_array);
        }
        Self::new_from_arrays(client, names, columns)
    }

    pub fn new_from_columns(names: Vec<String>, columns: Vec<Box<dyn Object>>) -> Result<Self> {
        Ok(PandasDataFrameBuilder {
            sealed: false,
            names,
            columns,
        })
    }

    pub fn new_from_arrays(
        client: &mut IPCClient,
        names: Vec<String>,
        arrays: Vec<ArrowArrayRef>,
    ) -> Result<Self> {
        use vineyard::ds::tensor::build_tensor;

        let mut columns = Vec::with_capacity(arrays.len());
        for array in arrays {
            columns.push(build_tensor(client, array)?);
        }
        Ok(PandasDataFrameBuilder {
            sealed: false,
            names,
            columns,
        })
    }
}

/// Building a polars dataframe into a arrow's table-compatible dataframe.
pub struct ArrowDataFrameBuilder(pub TableBuilder);

impl ObjectBuilder for ArrowDataFrameBuilder {
    fn sealed(&self) -> bool {
        self.0.sealed()
    }

    fn set_sealed(&mut self, sealed: bool) {
        self.0.set_sealed(sealed)
    }
}

impl ObjectBase for ArrowDataFrameBuilder {
    fn build(&mut self, client: &mut IPCClient) -> Result<()> {
        self.0.build(client)
    }

    fn seal(self, client: &mut IPCClient) -> Result<Box<dyn Object>> {
        let table = downcast_object::<Table>(self.0.seal(client)?)?;
        DataFrame::new_boxed(table.metadata())
    }
}

impl ArrowDataFrameBuilder {
    pub fn new(client: &mut IPCClient, dataframe: &polars::DataFrame) -> Result<Self> {
        let mut names = Vec::with_capacity(dataframe.width());
        let mut datatypes = Vec::with_capacity(dataframe.width());
        let mut columns = Vec::with_capacity(dataframe.width());
        for column in dataframe.get_columns() {
            let series = column.as_materialized_series();
            names.push(series.name().to_string());
            let arrow_dtype =
                polars_arrow_dtype_to_arrow(&series.dtype().to_arrow(CompatLevel::oldest()))?;
            datatypes.push(arrow_dtype);
            let mut chunks = Vec::with_capacity(series.chunks().len());
            for (idx, _chunk) in series.chunks().iter().enumerate() {
                let arr = series.to_arrow(idx, CompatLevel::oldest());
                chunks.push(polars_array_to_arrow_array(arr)?);
            }
            columns.push(chunks);
        }
        Self::new_from_columns(client, names, datatypes, columns)
    }

    /// batches[0]: the first record batch
    /// batches[0][0]: the first column of the first record batch
    pub fn new_from_batch_columns(
        client: &mut IPCClient,
        names: Vec<String>,
        datatypes: Vec<ArrowDataType>,
        num_rows: Vec<usize>,
        num_columns: usize,
        batches: Vec<Vec<Box<dyn Object>>>,
    ) -> Result<Self> {
        let schema = arrow_schema::Schema::new(
            izip!(names, datatypes)
                .map(|(name, datatype)| arrow_schema::Field::new(name, datatype, false))
                .collect::<Vec<_>>(),
        );
        Ok(ArrowDataFrameBuilder(TableBuilder::new_from_batch_columns(
            client,
            &schema,
            num_rows,
            num_columns,
            batches,
        )?))
    }

    /// batches[0]: the first record batch
    /// batches[0][0]: the first column of the first record batch
    pub fn new_from_batches(
        client: &mut IPCClient,
        names: Vec<String>,
        datatypes: Vec<ArrowDataType>,
        batches: Vec<Vec<ArrowArrayRef>>,
    ) -> Result<Self> {
        use vineyard::ds::arrow::build_array;

        let mut num_rows = Vec::with_capacity(batches.len());
        let num_columns = batches.first().map(|b| b.len()).unwrap_or(0);
        let mut chunks = Vec::with_capacity(batches.len());
        for batch in batches {
            let mut columns = Vec::with_capacity(batch.len());
            num_rows.push(batch.get(0).map(|a| a.len()).unwrap_or(0));
            for array in batch {
                columns.push(build_array(client, array)?);
            }
            chunks.push(columns);
        }
        Self::new_from_batch_columns(
            client,
            names,
            datatypes,
            num_rows,
            num_columns,
            chunks,
        )
    }

    /// columns[0]: the first column
    /// columns[0][0]: the first chunk of the first column
    pub fn new_from_columns(
        client: &mut IPCClient,
        names: Vec<String>,
        datatypes: Vec<ArrowDataType>,
        columns: Vec<Vec<ArrowArrayRef>>,
    ) -> Result<Self> {
        use vineyard::ds::arrow::build_array;

        let mut num_rows = Vec::new();
        let num_columns = columns.len();
        let mut chunks = Vec::new();
        for (column_index, column) in columns.into_iter().enumerate() {
            for (chunk_index, chunk) in column.into_iter().enumerate() {
                if column_index == 0 {
                    chunks.push(Vec::new());
                    num_rows.push(chunk.len());
                }
                chunks[chunk_index].push(build_array(client, chunk)?);
            }
        }
        Self::new_from_batch_columns(
            client,
            names,
            datatypes,
            num_rows,
            num_columns,
            chunks,
        )
    }
}
