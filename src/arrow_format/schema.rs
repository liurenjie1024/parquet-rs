// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Provides API for converting parquet schema to arrow schema and vice versa.
//!
//! The main interfaces for converting parquet schema to arrow schema  are
//! `parquet_to_arrow_schema` and `parquet_to_arrow_schema_by_columns`.
//!
//! The interfaces for converting arrow schema to parquet schema is coming.

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

/// Convert parquet schema to arrow schema.
pub fn parquet_to_arrow_schema(parquet_schema: SchemaDescPtr) -> Result<Schema> {
  parquet_to_arrow_schema_by_columns(parquet_schema.clone(), 0..parquet_schema.columns().len())
}

/// Convert parquet schema to arrow schema, only preserving some leaf columns.
pub fn parquet_to_arrow_schema_by_columns<T>(
  parquet_schema: SchemaDescPtr, column_indices: T) -> Result<Schema>
  where T: IntoIterator<Item=usize> {
  let mut base_nodes = Vec::new();
  let mut base_nodes_set = HashSet::new();
  let mut leaves = HashSet::new();
  
  for c in column_indices {
    let column = parquet_schema.column(c).self_type() as *const Type;
    let root = parquet_schema.get_column_root_ptr(c);
    let root_raw_ptr = Rc::into_raw(root.clone());
    
    leaves.insert(column);
    if !base_nodes_set.contains(&root_raw_ptr) {
      base_nodes.push(root);
      base_nodes_set.insert(root_raw_ptr);
    }
  }
  
  let leaves = Rc::new(leaves);
  base_nodes.into_iter()
    .map(|t| ParquetTypeConverter::new(t, leaves.clone()).to_field())
    .collect::<Result<Vec<Option<Field>>>>()
    .map(|result| result.into_iter()
      .filter_map(|f| f)
      .collect::<Vec<Field>>())
    .map(|fields| Schema::new(fields))
}

/// This struct is used to group methods and data structures used to convert parquet schema
/// together.
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
  // Interfaces.
  
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
  
  // Utility functions.
  
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
  
  // Functions for primitive types.
  
  fn to_primitive_type(&self) -> Result<Option<DataType>> {
    if self.is_self_included() {
      self.to_primitive_type_inner().map(|dt| {
        if self.is_repeated() {
          Some(DataType::List(Box::new(dt)))
        } else {
          Some(dt)
        }
      })
    } else {
      Ok(None)
    }
  }
  
  fn to_primitive_type_inner(&self) -> Result<DataType> {
    match self.schema.get_physical_type() {
      PhysicalType::BOOLEAN => Ok(DataType::Boolean),
      PhysicalType::INT32 => self.to_int32(),
      PhysicalType::INT64 => self.to_int64(),
      PhysicalType::FLOAT => Ok(DataType::Float32),
      PhysicalType::DOUBLE => Ok(DataType::Float64),
      PhysicalType::BYTE_ARRAY => self.to_byte_array(),
      other => Err(ParquetError::ArrowError(format!("Unable to convert parquet type {}", other)))
    }
  }
  
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
  
  // Functions for group types.
  
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
              item_converter.to_primitive_type_inner().map(|dt| Some(dt))
            } else {
              Err(ParquetError::ArrowError("Unrecognized list type.".to_string()))
            }
          },
          Type::GroupType {
            basic_info: _,
            fields
          } if fields.len()>1 => {
            item_converter.to_struct()
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
            item_converter.to_struct()
          }
        };
        
        item_type.map(|opt| opt.map(|dt| DataType::List(Box::new(dt))))
      },
      _ => Err(ParquetError::ArrowError("Unrecognized list type.".to_string()))
    }
  }
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
    let mut arrow_fields = Vec::new();
    
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

      arrow_fields.push(Field::new("my_list", DataType::List(Box::new(DataType::Utf8)), false));
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
        .with_repetition(Repetition::OPTIONAL)
        .with_logical_type(LogicalType::LIST)
        .with_fields(&mut vec![Rc::new(list)])
        .build()
        .unwrap();
  
      parquet_types.push(Rc::new(my_list));

      arrow_fields.push(Field::new("my_list", DataType::List(Box::new(DataType::Utf8)), true));
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
        .with_fields(&mut vec![Rc::new(list)])
        .build()
        .unwrap();
  
      parquet_types.push(Rc::new(array_of_arrays));

      let arrow_inner_list = DataType::List(Box::new(DataType::Int32));
      arrow_fields.push(Field::new("array_of_arrays", DataType::List(Box::new(arrow_inner_list)), true));
    }


    // // List<String> (list nullable, elements non-null)
    // optional group my_list (LIST) {
    //   repeated group element {
    //     required binary str (UTF8);
    //   };
    // }
    {
      let str_ = PrimitiveTypeBuilder::new("str", PhysicalType::BYTE_ARRAY)
        .with_logical_type(LogicalType::UTF8)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

      let element = GroupTypeBuilder::new("element")
        .with_repetition(Repetition::REPEATED)
        .with_fields(&mut vec![Rc::new(str_)])
        .build()
        .unwrap();

      let my_list = GroupTypeBuilder::new("my_list")
        .with_repetition(Repetition::OPTIONAL)
        .with_logical_type(LogicalType::LIST)
        .with_fields(&mut vec![Rc::new(element)])
        .build()
        .unwrap();

      parquet_types.push(Rc::new(my_list));
      arrow_fields.push(Field::new("my_list", DataType::List(Box::new(DataType::Utf8)), true));
    }

    // // List<Integer> (nullable list, non-null elements)
    // optional group my_list (LIST) {
    //   repeated int32 element;
    // }
    {
      let element = PrimitiveTypeBuilder::new("element", PhysicalType::INT32)
        .with_repetition(Repetition::REPEATED)
        .build()
        .unwrap();

      let my_list = GroupTypeBuilder::new("my_list")
        .with_logical_type(LogicalType::LIST)
        .with_repetition(Repetition::OPTIONAL)
        .with_fields(&mut vec![Rc::new(element)])
        .build()
        .unwrap();

      parquet_types.push(Rc::new(my_list));

      arrow_fields.push(Field::new("my_list", DataType::List(Box::new(DataType::Int32)), true));
    }

    // // List<Tuple<String, Integer>> (nullable list, non-null elements)
    // optional group my_list (LIST) {
    //   repeated group element {
    //     required binary str (UTF8);
    //     required int32 num;
    //   };
    // }
    {
      let mut primitives = vec![
        make_parquet_type!("str", PhysicalType::BYTE_ARRAY, with_logical_type: LogicalType::UTF8,
         with_repetition: Repetition::REQUIRED),
        make_parquet_type!("num", PhysicalType::INT32, with_repetition: Repetition::REQUIRED)
      ];

      let element = GroupTypeBuilder::new("element")
        .with_repetition(Repetition::REPEATED)
        .with_fields(&mut primitives)
        .build()
        .unwrap();

      let my_list = GroupTypeBuilder::new("my_list")
        .with_repetition(Repetition::OPTIONAL)
        .with_logical_type(LogicalType::LIST)
        .with_fields(&mut vec![Rc::new(element)])
        .build()
        .unwrap();

      parquet_types.push(Rc::new(my_list));

      let arrow_struct = DataType::Struct(vec![
        Field::new("str", DataType::Utf8, false),
        Field::new("num", DataType::Int32, false),
      ]);
      arrow_fields.push(Field::new("my_list", DataType::List(Box::new(arrow_struct)), true));
    }

    // // List<OneTuple<String>> (nullable list, non-null elements)
    // optional group my_list (LIST) {
    //   repeated group array {
    //     required binary str (UTF8);
    //   };
    // }
    // Special case: group is named array
    {
      let mut primitives = vec![
        make_parquet_type!("str", PhysicalType::BYTE_ARRAY, with_logical_type: LogicalType::UTF8,
          with_repetition: Repetition::REQUIRED)
      ];

      let array = GroupTypeBuilder::new("array")
        .with_repetition(Repetition::REPEATED)
        .with_fields(&mut primitives)
        .build()
        .unwrap();

      let my_list = GroupTypeBuilder::new("my_list")
        .with_repetition(Repetition::OPTIONAL)
        .with_logical_type(LogicalType::LIST)
        .with_fields(&mut vec![Rc::new(array)])
        .build()
        .unwrap();

      parquet_types.push(Rc::new(my_list));

      let arrow_struct = DataType::Struct(vec![
        Field::new("str", DataType::Utf8, false)
      ]);
      arrow_fields.push(Field::new("my_list", DataType::List(Box::new(arrow_struct)), true));
    }

    // // List<OneTuple<String>> (nullable list, non-null elements)
    // optional group my_list (LIST) {
    //   repeated group my_list_tuple {
    //     required binary str (UTF8);
    //   };
    // }
    // Special case: group named ends in _tuple
    {
      let mut primitives = vec![
        make_parquet_type!("str", PhysicalType::BYTE_ARRAY, with_logical_type: LogicalType::UTF8,
          with_repetition: Repetition::REQUIRED)
      ];

      let array = GroupTypeBuilder::new("my_list_tuple")
        .with_repetition(Repetition::REPEATED)
        .with_fields(&mut primitives)
        .build()
        .unwrap();

      let my_list = GroupTypeBuilder::new("my_list")
        .with_repetition(Repetition::OPTIONAL)
        .with_logical_type(LogicalType::LIST)
        .with_fields(&mut vec![Rc::new(array)])
        .build()
        .unwrap();

      parquet_types.push(Rc::new(my_list));

      let arrow_struct = DataType::Struct(vec![
        Field::new("str", DataType::Utf8, false)
      ]);
      arrow_fields.push(Field::new("my_list", DataType::List(Box::new(arrow_struct)), true));
    }

    // One-level encoding: Only allows required lists with required cells
    //   repeated value_type name
    {
      parquet_types.push(make_parquet_type!("name", PhysicalType::INT32,
        with_repetition: Repetition::REPEATED));
      arrow_fields.push(Field::new("name", DataType::List(Box::new(DataType::Int32)), true));
    }
  
  
    let parquet_group_type = GroupTypeBuilder::new("")
      .with_fields(&mut parquet_types)
      .build()
      .unwrap();
  
    let parquet_schema = Rc::new(SchemaDescriptor::new(Rc::new(parquet_group_type)));
    let converted_arrow_schema = parquet_to_arrow_schema(parquet_schema.clone()).unwrap();
    let converted_fields = converted_arrow_schema.fields();

    assert_eq!(arrow_fields.len(), converted_fields.len());
    for i in 0..arrow_fields.len() {
      assert_eq!(arrow_fields[i], converted_fields[i]);
    }
  }

  #[test]
  fn test_nested_schema() {
    let mut arrow_fields = Vec::new();
    let mut parquet_types = Vec::new();

    // required group group1 {
    //   required bool leaf1;
    //   required int32 leaf2;
    // }
    // required int64 leaf3;
    {
      let group1 = GroupTypeBuilder::new("group1")
        .with_repetition(Repetition::REQUIRED)
        .with_fields(&mut vec![
          make_parquet_type!("leaf1", PhysicalType::BOOLEAN, with_repetition: Repetition::REQUIRED),
          make_parquet_type!("leaf2", PhysicalType::INT32, with_repetition: Repetition::REQUIRED)
        ]).build()
        .unwrap();
      parquet_types.push(Rc::new(group1));

      let leaf3 = make_parquet_type!("leaf3", PhysicalType::INT64,
        with_repetition: Repetition::REQUIRED);
      parquet_types.push(leaf3);


      let group1_fields = vec![
        Field::new("leaf1", DataType::Boolean, false),
        Field::new("leaf2", DataType::Int32, false),
      ];
      let group1_struct = Field::new("group1", DataType::Struct(group1_fields), false);
      arrow_fields.push(group1_struct);

      let leaf3_field = Field::new("leaf3", DataType::Int64, false);
      arrow_fields.push(leaf3_field);
    }

    let parquet_group_type = GroupTypeBuilder::new("")
      .with_fields(&mut parquet_types)
      .build()
      .unwrap();

    let parquet_schema = Rc::new(SchemaDescriptor::new(Rc::new(parquet_group_type)));
    let converted_arrow_schema = parquet_to_arrow_schema(parquet_schema.clone()).unwrap();
    let converted_fields = converted_arrow_schema.fields();

    assert_eq!(arrow_fields.len(), converted_fields.len());
    for i in 0..arrow_fields.len() {
      assert_eq!(arrow_fields[i], converted_fields[i]);
    }
  }

  #[test]
  fn test_nested_schema_partial() {
    let mut arrow_fields = Vec::new();
    let mut parquet_types = Vec::new();

    // Full Parquet Schema:
    // required group group1 {
    //   required int64 leaf1;
    //   required int64 leaf2;
    // }
    // required group group2 {
    //   required int64 leaf3;
    //   required int64 leaf4;
    // }
    // required int64 leaf5;
    //
    // Expected partial arrow schema (columns 0, 3, 4):
    // required group group1 {
    //   required int64 leaf1;
    // }
    // required group group2 {
    //   required int64 leaf4;
    // }
    // required int64 leaf5;
    {
      let group1 = GroupTypeBuilder::new("group1")
        .with_repetition(Repetition::REQUIRED)
        .with_fields(&mut vec![
          make_parquet_type!("leaf1", PhysicalType::INT64, with_repetition: Repetition::REQUIRED),
          make_parquet_type!("leaf2", PhysicalType::INT64, with_repetition: Repetition::REQUIRED),
        ]).build()
        .unwrap();
      parquet_types.push(Rc::new(group1));

      let group2 = GroupTypeBuilder::new("group2")
        .with_repetition(Repetition::REQUIRED)
        .with_fields(&mut vec![
          make_parquet_type!("leaf3", PhysicalType::INT64, with_repetition: Repetition::REQUIRED),
          make_parquet_type!("leaf4", PhysicalType::INT64, with_repetition: Repetition::REQUIRED),
        ]).build()
        .unwrap();
      parquet_types.push(Rc::new(group2));

      let leaf5 = make_parquet_type!("leaf5", PhysicalType::INT64,
        with_repetition: Repetition::REQUIRED);
      parquet_types.push(leaf5);
    }

    {
      let group1_fields = vec![
        Field::new("leaf1", DataType::Int64, false)
      ];
      let group1 = Field::new("group1", DataType::Struct(group1_fields), false);
      arrow_fields.push(group1);

      let group2_fields = vec![
        Field::new("leaf4", DataType::Int64, false)
      ];
      let group2 = Field::new("group2", DataType::Struct(group2_fields), false);
      arrow_fields.push(group2);

      arrow_fields.push(Field::new("leaf5", DataType::Int64, false));
    }

    let parquet_group_type = GroupTypeBuilder::new("")
      .with_fields(&mut parquet_types)
      .build()
      .unwrap();

    let parquet_schema = Rc::new(SchemaDescriptor::new(Rc::new(parquet_group_type)));
    let converted_arrow_schema = parquet_to_arrow_schema_by_columns(parquet_schema.clone(),
      vec![0, 3, 4]).unwrap();
    let converted_fields = converted_arrow_schema.fields();

    assert_eq!(arrow_fields.len(), converted_fields.len());
    for i in 0..arrow_fields.len() {
      assert_eq!(arrow_fields[i], converted_fields[i]);
    }
  }

  #[test]
  fn test_nested_schema_partial_ordering() {
    let mut arrow_fields = Vec::new();
    let mut parquet_types = Vec::new();

    // Full Parquet Schema:
    // required group group1 {
    //   required int64 leaf1;
    //   required int64 leaf2;
    // }
    // required group group2 {
    //   required int64 leaf3;
    //   required int64 leaf4;
    // }
    // required int64 leaf5;
    //
    // Expected partial arrow schema (columns 3, 4, 0):
    // required group group1 {
    //   required int64 leaf1;
    // }
    // required group group2 {
    //   required int64 leaf4;
    // }
    // required int64 leaf5;
    {
      let group1 = GroupTypeBuilder::new("group1")
        .with_repetition(Repetition::REQUIRED)
        .with_fields(&mut vec![
          make_parquet_type!("leaf1", PhysicalType::INT64, with_repetition: Repetition::REQUIRED),
          make_parquet_type!("leaf2", PhysicalType::INT64, with_repetition: Repetition::REQUIRED),
        ]).build()
        .unwrap();
      parquet_types.push(Rc::new(group1));

      let group2 = GroupTypeBuilder::new("group2")
        .with_repetition(Repetition::REQUIRED)
        .with_fields(&mut vec![
          make_parquet_type!("leaf3", PhysicalType::INT64, with_repetition: Repetition::REQUIRED),
          make_parquet_type!("leaf4", PhysicalType::INT64, with_repetition: Repetition::REQUIRED),
        ]).build()
        .unwrap();
      parquet_types.push(Rc::new(group2));

      let leaf5 = make_parquet_type!("leaf5", PhysicalType::INT64,
        with_repetition: Repetition::REQUIRED);
      parquet_types.push(leaf5);
    }

    {
      let group2_fields = vec![
        Field::new("leaf4", DataType::Int64, false)
      ];
      let group2 = Field::new("group2", DataType::Struct(group2_fields), false);
      arrow_fields.push(group2);

      arrow_fields.push(Field::new("leaf5", DataType::Int64, false));

      let group1_fields = vec![
        Field::new("leaf1", DataType::Int64, false)
      ];
      let group1 = Field::new("group1", DataType::Struct(group1_fields), false);
      arrow_fields.push(group1);
    }

    let parquet_group_type = GroupTypeBuilder::new("")
      .with_fields(&mut parquet_types)
      .build()
      .unwrap();

    let parquet_schema = Rc::new(SchemaDescriptor::new(Rc::new(parquet_group_type)));
    let converted_arrow_schema = parquet_to_arrow_schema_by_columns(parquet_schema.clone(),
                                                                    vec![3, 4, 0]).unwrap();
    let converted_fields = converted_arrow_schema.fields();

    assert_eq!(arrow_fields.len(), converted_fields.len());
    for i in 0..arrow_fields.len() {
      assert_eq!(arrow_fields[i], converted_fields[i]);
    }
  }

  #[test]
  fn test_repeated_nested_schema() {
    let mut arrow_fields = Vec::new();
    let mut parquet_types = Vec::new();

    //   optional int32 leaf1;
    //   repeated group outerGroup {
    //     optional int32 leaf2;
    //     repeated group innerGroup {
    //       optional int32 leaf3;
    //     }
    //   }
    {
      parquet_types.push(make_parquet_type!("leaf1", PhysicalType::INT32, with_repetition:
        Repetition::OPTIONAL));

      let leaf2 = make_parquet_type!("leaf2", PhysicalType::INT32, with_repetition:
        Repetition::OPTIONAL);

      let leaf3 = make_parquet_type!("leaf3", PhysicalType::INT32, with_repetition:
        Repetition::OPTIONAL);
      let inner_group = GroupTypeBuilder::new("innerGroup")
        .with_fields(&mut vec![leaf3])
        .with_repetition(Repetition::REPEATED)
        .build()
        .unwrap();

      let outer_group = GroupTypeBuilder::new("outerGroup")
        .with_fields(&mut vec![leaf2, Rc::new(inner_group)])
        .with_repetition(Repetition::REPEATED)
        .build()
        .unwrap();

      parquet_types.push(Rc::new(outer_group));
    }

    {
      arrow_fields.push(Field::new("leaf1", DataType::Int32, true));

      let inner_group_list = Field::new("innerGroup",
        DataType::List(Box::new(DataType::Struct(
          vec![Field::new("leaf3", DataType::Int32, true)]))),
        true
      );

      let outer_group_list = Field::new("outerGroup",
        DataType::List(Box::new(DataType::Struct(
          vec![Field::new("leaf2", DataType::Int32, true), inner_group_list]
        ))),
        true);
      arrow_fields.push(outer_group_list);
    }

    let parquet_group_type = GroupTypeBuilder::new("")
      .with_fields(&mut parquet_types)
      .build()
      .unwrap();

    let parquet_schema = Rc::new(SchemaDescriptor::new(Rc::new(parquet_group_type)));
    let converted_arrow_schema = parquet_to_arrow_schema(parquet_schema.clone())
      .unwrap();
    let converted_fields = converted_arrow_schema.fields();

    assert_eq!(arrow_fields.len(), converted_fields.len());
    for i in 0..arrow_fields.len() {
      assert_eq!(arrow_fields[i], converted_fields[i]);
    }
  }
}
