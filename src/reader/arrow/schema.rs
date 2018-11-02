use std::collections::HashSet;
use std::rc::Rc;

use schema::types::SchemaDescPtr;
use schema::types::Type;
use schema::types::TypePtr;
use basic::LogicalType;
use basic::Type as PhysicalType;
use basic::Repetition;

use arrow::datatypes::Schema;
use arrow::datatypes::Field;
use arrow::datatypes::DataType;

use errors::ParquetError;
use errors::Result;

struct ParquetTypeConverter {
  schema: TypePtr,
  leaves: Rc<HashSet<*const Type>>
}

impl ParquetTypeConverter {
  fn new(schema: TypePtr, leaves: Rc<HashSet<*const Type>>) -> Self {
    Self {
      schema,
      leaves
    }
  }
  
  fn clone_with_schema(&self, other: TypePtr) -> Self {
    Self {
      schema: other,
      leaves: self.leaves.clone()
    }
  }
}

impl ParquetTypeConverter {
  
  /// Interfaces.
  fn to_data_type(&self) -> Result<Option<DataType>> {
    match &*self.schema {
      Type::PrimitiveType {..} => self.to_primitive_type(),
      Type::GroupType {..} => self.to_group_type()
    }
  }
  
  fn to_field(&self) -> Result<Option<Field>> {
    self.to_data_type().map(|opt| opt.map(|dt| {
      Field::new(self.schema.name(), dt, self.is_nullable())
    }))
  }
  
  /// Implementation details
  
  fn is_nullable(&self) -> bool {
    let basic_info = self.schema.get_basic_info();
    if basic_info.has_repetition() {
      match basic_info.repetition() {
        Repetition::OPTIONAL => true,
        Repetition::REPEATED => true,
        Repetition::REQUIRED => false
      }
    } else {
      false
    }
  }
  
  fn is_repeated(&self) -> bool {
    let basic_info = self.schema.get_basic_info();
    
    basic_info.has_repetition() && basic_info.repetition()==Repetition::REPEATED
  }
  
  fn is_self_included(&self) -> bool {
    self.leaves.contains(&Rc::into_raw(self.schema.clone()))
  }
  
  fn to_primitive_type(&self) -> Result<Option<DataType>> {
    if self.is_self_included() {
      match self.schema.get_physical_type() {
        PhysicalType::BOOLEAN => Ok(DataType::Boolean),
        PhysicalType::INT32 => self.to_int32(),
        PhysicalType::INT64 => self.to_int64(),
        PhysicalType::FLOAT => Ok(DataType::Float32),
        PhysicalType::DOUBLE => Ok(DataType::Float64),
        PhysicalType::BYTE_ARRAY => self.to_byte_array(),
        other => Err(ParquetError::ArrowError(format!("Unable to convert parquet type {}", other)))
      }.map(|dt| Some(dt))
    } else {
      Ok(None)
    }
  }
  
  fn to_group_type(&self) -> Result<Option<DataType>> {
    if self.is_repeated() {
      self.to_struct().map(|opt| opt.map(|dt| DataType::List(Box::new(dt))))
    } else {
      match self.schema.get_basic_info().logical_type() {
        LogicalType::LIST => self.to_list(),
        _ => self.to_struct()
      }
    }
  }
  
  
  /// primitive types
  fn to_int32(&self) -> Result<DataType> {
    match self.schema.get_basic_info().logical_type() {
      LogicalType::NONE => Ok(DataType::Int32),
      LogicalType::UINT_8 => Ok(DataType::UInt8),
      LogicalType::UINT_16 => Ok(DataType::UInt16),
      LogicalType::UINT_32 => Ok(DataType::UInt32),
      LogicalType::INT_8 => Ok(DataType::Int8),
      LogicalType::INT_16 => Ok(DataType::Int16),
      LogicalType::INT_32 => Ok(DataType::Int32),
      other => Err(ParquetError::ArrowError(format!("Unable to convert parquet logical type {}", other)))
    }
  }
  
  fn to_int64(&self) -> Result<DataType> {
    match self.schema.get_basic_info().logical_type() {
      LogicalType::NONE => Ok(DataType::Int64),
      LogicalType::INT_64 => Ok(DataType::Int64),
      LogicalType::UINT_64 => Ok(DataType::UInt64),
      other => Err(ParquetError::ArrowError(format!("Unable to convert parquet logical type {}", other)))
    }
  }
  
  fn to_byte_array(&self) -> Result<DataType> {
    match self.schema.get_basic_info().logical_type() {
      LogicalType::UTF8 => Ok(DataType::Utf8),
      other => Err(ParquetError::ArrowError(format!("Unable to convert parquet logical type {}", other)))
    }
  }
  
  /// group types
  ///
  ///
  
  fn to_struct(&self) -> Result<Option<DataType>> {
    match &*self.schema {
      Type::PrimitiveType {..} => panic!("This should not happen."),
      Type::GroupType {
        basic_info: _,
        fields
      } => {
        fields.iter()
          .map(|field_ptr| self.clone_with_schema(field_ptr.clone()).to_field())
          .collect::<Result<Vec<Option<Field>>>>()
          .map(|result| result.into_iter()
            .filter_map(|f| f)
            .collect::<Vec<Field>>())
          .map(|fields| {
            if fields.is_empty() {
              None
            } else {
              Some(DataType::Struct(fields))
            }
          })
      }
    }
  }
  
  fn to_list(&self) -> Result<Option<DataType>> {
    match &*self.schema {
      Type::PrimitiveType {..} => panic!("This should not happen."),
      Type::GroupType {
        basic_info: _,
        fields
      } if fields.len() == 1 => {
        let list_item = fields.first().unwrap();
        let item_converter = self.clone_with_schema(list_item.clone());
        
        let item_type = match &**list_item {
          Type::PrimitiveType {
            ..
          }  => {
            if item_converter.is_repeated() {
              item_converter.to_data_type()
            } else {
              Err(ParquetError::ArrowError("Unrecognized list type.".to_string()))
            }
          },
          Type::GroupType {
            basic_info: _,
            fields
          } if fields.len()>1 => {
            item_converter.to_data_type()
          },
          Type::GroupType {
            basic_info: _,
            fields
          } if fields.len()==1 && list_item.name()!="array" &&
            list_item.name()!=format!("{}_tuple", self.schema.name()) => {
            let nested_item = fields.first().unwrap();
            let nested_item_converter = self.clone_with_schema(nested_item.clone());
            
            nested_item_converter.to_data_type()
          },
          Type::GroupType {
            basic_info: _,
            fields: _
          } => {
            item_converter.to_data_type()
          }
        };
        
        item_type.map(|opt| opt.map(|dt| DataType::List(Box::new(dt))))
      },
      _ => Err(ParquetError::ArrowError("Unrecognized list type.".to_string()))
    }
  }
}


pub fn parquet_to_arrow_schema(parquet_schema: SchemaDescPtr) -> Result<Schema> {
  parquet_to_arrow_schema_by_columns(parquet_schema.clone(), 0..parquet_schema.columns().len())
}

pub fn parquet_to_arrow_schema_by_columns<T>(
  parquet_schema: SchemaDescPtr, column_indices: T)
  -> Result<Schema>
  where T: IntoIterator<Item=usize> {
  let leaves = Rc::new(column_indices
    .into_iter()
    .map(|c| parquet_schema.column(c).self_type() as *const Type)
    .collect::<HashSet<*const Type>>());
  
  parquet_schema.root_schema().get_fields()
    .iter()
    .map(|t| ParquetTypeConverter::new(t.clone(), leaves.clone()).to_field())
    .collect::<Result<Vec<Option<Field>>>>()
    .map(|result| result.into_iter()
      .filter_map(|f| f)
      .collect::<Vec<Field>>())
    .map(|fields| Schema::new(fields))
}


#[cfg(test)]
mod tests {
  use std::rc::Rc;
  
  use schema::types::GroupTypeBuilder;
  use schema::types::SchemaDescriptor;
  use basic::Type as PhysicalType;
  use basic::LogicalType;
  use basic::Repetition;
  use schema::types::PrimitiveTypeBuilder;
  
  use arrow::datatypes::Field;
  use arrow::datatypes::DataType;
  use arrow::datatypes::Schema;
  
  use super::parquet_to_arrow_schema;
  use super::parquet_to_arrow_schema_by_columns;
  
  macro_rules! make_parquet_type {
    ($name: expr, $physical_type: expr,  $($attr: ident : $value: expr),*) => {{
      let mut builder = PrimitiveTypeBuilder::new($name, $physical_type);

      $(
        builder = builder.$attr($value);
      )*

      Rc::new(builder.build().unwrap())
    }}
  }
  
  #[test]
  fn test_flat_primitives() {
    let mut parquet_types = vec![
      make_parquet_type!("boolean", PhysicalType::BOOLEAN, with_repetition: Repetition::REQUIRED),
      make_parquet_type!("int8", PhysicalType::INT32, with_repetition: Repetition::REQUIRED,
        with_logical_type: LogicalType::INT_8),
      make_parquet_type!("int16", PhysicalType::INT32, with_repetition: Repetition::REQUIRED,
        with_logical_type: LogicalType::INT_16),
      make_parquet_type!("int32", PhysicalType::INT32, with_repetition: Repetition::REQUIRED),
      make_parquet_type!("int64", PhysicalType::INT64, with_repetition: Repetition::REQUIRED),
      make_parquet_type!("double", PhysicalType::DOUBLE, with_repetition: Repetition::OPTIONAL),
      make_parquet_type!("float", PhysicalType::FLOAT, with_repetition: Repetition::OPTIONAL),
      make_parquet_type!("string", PhysicalType::BYTE_ARRAY,
        with_repetition: Repetition::OPTIONAL, with_logical_type: LogicalType::UTF8),
    ];
    
    let parquet_group_type = GroupTypeBuilder::new("")
      .with_fields(&mut parquet_types)
      .build()
      .unwrap();
    
    let parquet_schema = SchemaDescriptor::new(Rc::new(parquet_group_type));
    let converted_arrow_schema = parquet_to_arrow_schema(Rc::new(parquet_schema)).unwrap();
    
    let arrow_fields = vec![
      Field::new("boolean", DataType::Boolean, false),
      Field::new("int8", DataType::Int8, false),
      Field::new("int16", DataType::Int16, false),
      Field::new("int32", DataType::Int32, false),
      Field::new("int64", DataType::Int64, false),
      Field::new("double", DataType::Float64, true),
      Field::new("float", DataType::Float32, true),
      Field::new("string", DataType::Utf8, true),
    ];
    
    assert_eq!(&arrow_fields, converted_arrow_schema.fields());
  }
  
  #[test]
  fn test_duplicate_fields() {
    let mut parquet_types = vec![
      make_parquet_type!("boolean", PhysicalType::BOOLEAN, with_repetition: Repetition::REQUIRED),
      make_parquet_type!("int8", PhysicalType::INT32, with_repetition: Repetition::REQUIRED,
        with_logical_type: LogicalType::INT_8)
    ];
    
    let parquet_group_type = GroupTypeBuilder::new("")
      .with_fields(&mut parquet_types)
      .build()
      .unwrap();
    
    let parquet_schema = Rc::new(SchemaDescriptor::new(Rc::new(parquet_group_type)));
    let converted_arrow_schema = parquet_to_arrow_schema(parquet_schema.clone()).unwrap();
    
    let arrow_fields = vec![
      Field::new("boolean", DataType::Boolean, false),
      Field::new("int8", DataType::Int8, false),
    ];
    assert_eq!(&arrow_fields, converted_arrow_schema.fields());
    
    let converted_arrow_schema = parquet_to_arrow_schema_by_columns(
      parquet_schema.clone(), vec![0usize, 1usize])
      .unwrap();
    assert_eq!(&arrow_fields, converted_arrow_schema.fields());
  }
  
  #[test]
  fn test_parquet_lists() {
    let arrow_fields = vec![
      Field::new("my_list", DataType::List(Box::new(DataType::Utf8)), false)
    ];
    
    let mut parquet_types = Vec::new();
  
    // LIST encoding example taken from parquet-format/LogicalTypes.md
  
    // // List<String> (list non-null, elements nullable)
    // required group my_list (LIST) {
    //   repeated group list {
    //     optional binary element (UTF8);
    //   }
    // }
    {
      let element = PrimitiveTypeBuilder::new("string", PhysicalType::BYTE_ARRAY)
        .with_logical_type(LogicalType::UTF8)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap();
      
      let list = GroupTypeBuilder::new("list")
        .with_repetition(Repetition::REPEATED)
        .with_fields(&mut vec![Rc::new(element)])
        .build()
        .unwrap();
      
      let my_list = GroupTypeBuilder::new("my_list")
        .with_repetition(Repetition::REQUIRED)
        .with_logical_type(LogicalType::LIST)
        .with_fields(&mut vec![Rc::new(list)])
        .build()
        .unwrap();
      
      parquet_types.push(Rc::new(my_list));
    }
  
    // // List<String> (list nullable, elements non-null)
    // optional group my_list (LIST) {
    //   repeated group list {
    //     required binary element (UTF8);
    //   }
    // }
    {
      let element = PrimitiveTypeBuilder::new("string", PhysicalType::BYTE_ARRAY)
        .with_repetition(Repetition::REQUIRED)
        .with_logical_type(LogicalType::UTF8)
        .build()
        .unwrap();
      
      let list = GroupTypeBuilder::new("list")
        .with_repetition(Repetition::REPEATED)
        .with_fields(&mut vec![Rc::new(element)])
        .build()
        .unwrap();
  
      let my_list = GroupTypeBuilder::new("my_list")
        .with_repetition(Repetition::REQUIRED)
        .with_logical_type(LogicalType::LIST)
        .with_fields(&mut vec![Rc::new(list)])
        .build()
        .unwrap();
  
      parquet_types.push(Rc::new(my_list));
    }
  
    // Element types can be nested structures. For example, a list of lists:
    //
    // // List<List<Integer>>
    // optional group array_of_arrays (LIST) {
    //   repeated group list {
    //     required group element (LIST) {
    //       repeated group list {
    //         required int32 element;
    //       }
    //     }
    //   }
    // }
    {
      let inner_element = PrimitiveTypeBuilder::new("element", PhysicalType::INT32)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();
      
      let inner_list = GroupTypeBuilder::new("list")
        .with_repetition(Repetition::REPEATED)
        .with_fields(&mut vec![Rc::new(inner_element)])
        .build()
        .unwrap();
      
      let element = GroupTypeBuilder::new("element")
        .with_logical_type(LogicalType::LIST)
        .with_repetition(Repetition::REQUIRED)
        .with_fields(&mut vec![Rc::new(inner_list)])
        .build()
        .unwrap();
      
      let list = GroupTypeBuilder::new("list")
        .with_repetition(Repetition::REPEATED)
        .with_fields(&mut vec![Rc::new(element)])
        .build()
        .unwrap();
      
      let array_of_arrays = GroupTypeBuilder::new("array_of_arrays")
        .with_repetition(Repetition::OPTIONAL)
        .with_logical_type(LogicalType::LIST)
        .with_fields(&mut vec![Rc::new(lsit)])
        .build()
        .unwrap();
  
      parquet_types.push(Rc::new(array_of_arrays));
    }
  
  
    let parquet_group_type = GroupTypeBuilder::new("")
      .with_fields(&mut parquet_types)
      .build()
      .unwrap();
  
    let parquet_schema = Rc::new(SchemaDescriptor::new(Rc::new(parquet_group_type)));
    let converted_arrow_schema = parquet_to_arrow_schema(parquet_schema.clone()).unwrap();
  
    assert_eq!(&arrow_fields, converted_arrow_schema.fields());
  }
}
