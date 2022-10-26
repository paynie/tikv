// This file is generated by rust-protobuf 3.1.0. Do not edit
// .proto file is parsed by protoc --rust-out=...
// @generated

// https://github.com/rust-lang/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![allow(unused_attributes)]
#![cfg_attr(rustfmt, rustfmt::skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unused_results)]
#![allow(unused_mut)]

//! Generated file from `foo.proto`

/// Generated files are compatible only with the same version
/// of protobuf runtime.
const _PROTOBUF_VERSION_CHECK: () = ::protobuf::VERSION_3_1_0;

#[derive(PartialEq,Clone,Default,Debug)]
// @@protoc_insertion_point(message:com.tencent.easygraph.model.Value)
pub struct Value {
    // message fields
    // @@protoc_insertion_point(field:com.tencent.easygraph.model.Value.value_type)
    pub value_type: ::protobuf::EnumOrUnknown<ValueType>,
    // message oneof groups
    pub oneof_value: ::std::option::Option<value::Oneof_value>,
    // special fields
    // @@protoc_insertion_point(special_field:com.tencent.easygraph.model.Value.special_fields)
    pub special_fields: ::protobuf::SpecialFields,
}

impl<'a> ::std::default::Default for &'a Value {
    fn default() -> &'a Value {
        <Value as ::protobuf::Message>::default_instance()
    }
}

impl Value {
    pub fn new() -> Value {
        ::std::default::Default::default()
    }

    // int32 int_value = 1;

    pub fn int_value(&self) -> i32 {
        match self.oneof_value {
            ::std::option::Option::Some(value::Oneof_value::IntValue(v)) => v,
            _ => 0,
        }
    }

    pub fn clear_int_value(&mut self) {
        self.oneof_value = ::std::option::Option::None;
    }

    pub fn has_int_value(&self) -> bool {
        match self.oneof_value {
            ::std::option::Option::Some(value::Oneof_value::IntValue(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_int_value(&mut self, v: i32) {
        self.oneof_value = ::std::option::Option::Some(value::Oneof_value::IntValue(v))
    }

    // int64 long_value = 2;

    pub fn long_value(&self) -> i64 {
        match self.oneof_value {
            ::std::option::Option::Some(value::Oneof_value::LongValue(v)) => v,
            _ => 0,
        }
    }

    pub fn clear_long_value(&mut self) {
        self.oneof_value = ::std::option::Option::None;
    }

    pub fn has_long_value(&self) -> bool {
        match self.oneof_value {
            ::std::option::Option::Some(value::Oneof_value::LongValue(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_long_value(&mut self, v: i64) {
        self.oneof_value = ::std::option::Option::Some(value::Oneof_value::LongValue(v))
    }

    // string string_value = 3;

    pub fn string_value(&self) -> &str {
        match self.oneof_value {
            ::std::option::Option::Some(value::Oneof_value::StringValue(ref v)) => v,
            _ => "",
        }
    }

    pub fn clear_string_value(&mut self) {
        self.oneof_value = ::std::option::Option::None;
    }

    pub fn has_string_value(&self) -> bool {
        match self.oneof_value {
            ::std::option::Option::Some(value::Oneof_value::StringValue(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_string_value(&mut self, v: ::std::string::String) {
        self.oneof_value = ::std::option::Option::Some(value::Oneof_value::StringValue(v))
    }

    // Mutable pointer to the field.
    pub fn mut_string_value(&mut self) -> &mut ::std::string::String {
        if let ::std::option::Option::Some(value::Oneof_value::StringValue(_)) = self.oneof_value {
        } else {
            self.oneof_value = ::std::option::Option::Some(value::Oneof_value::StringValue(::std::string::String::new()));
        }
        match self.oneof_value {
            ::std::option::Option::Some(value::Oneof_value::StringValue(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_string_value(&mut self) -> ::std::string::String {
        if self.has_string_value() {
            match self.oneof_value.take() {
                ::std::option::Option::Some(value::Oneof_value::StringValue(v)) => v,
                _ => panic!(),
            }
        } else {
            ::std::string::String::new()
        }
    }

    // double double_value = 4;

    pub fn double_value(&self) -> f64 {
        match self.oneof_value {
            ::std::option::Option::Some(value::Oneof_value::DoubleValue(v)) => v,
            _ => 0.,
        }
    }

    pub fn clear_double_value(&mut self) {
        self.oneof_value = ::std::option::Option::None;
    }

    pub fn has_double_value(&self) -> bool {
        match self.oneof_value {
            ::std::option::Option::Some(value::Oneof_value::DoubleValue(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_double_value(&mut self, v: f64) {
        self.oneof_value = ::std::option::Option::Some(value::Oneof_value::DoubleValue(v))
    }

    fn generated_message_descriptor_data() -> ::protobuf::reflect::GeneratedMessageDescriptorData {
        let mut fields = ::std::vec::Vec::with_capacity(5);
        let mut oneofs = ::std::vec::Vec::with_capacity(1);
        fields.push(::protobuf::reflect::rt::v2::make_oneof_copy_has_get_set_simpler_accessors::<_, _>(
            "int_value",
            Value::has_int_value,
            Value::int_value,
            Value::set_int_value,
        ));
        fields.push(::protobuf::reflect::rt::v2::make_oneof_copy_has_get_set_simpler_accessors::<_, _>(
            "long_value",
            Value::has_long_value,
            Value::long_value,
            Value::set_long_value,
        ));
        fields.push(::protobuf::reflect::rt::v2::make_oneof_deref_has_get_set_simpler_accessor::<_, _>(
            "string_value",
            Value::has_string_value,
            Value::string_value,
            Value::set_string_value,
        ));
        fields.push(::protobuf::reflect::rt::v2::make_oneof_copy_has_get_set_simpler_accessors::<_, _>(
            "double_value",
            Value::has_double_value,
            Value::double_value,
            Value::set_double_value,
        ));
        fields.push(::protobuf::reflect::rt::v2::make_simpler_field_accessor::<_, _>(
            "value_type",
            |m: &Value| { &m.value_type },
            |m: &mut Value| { &mut m.value_type },
        ));
        oneofs.push(value::Oneof_value::generated_oneof_descriptor_data());
        ::protobuf::reflect::GeneratedMessageDescriptorData::new_2::<Value>(
            "Value",
            fields,
            oneofs,
        )
    }
}

impl ::protobuf::Message for Value {
    const NAME: &'static str = "Value";

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::Result<()> {
        while let Some(tag) = is.read_raw_tag_or_eof()? {
            match tag {
                8 => {
                    self.oneof_value = ::std::option::Option::Some(value::Oneof_value::IntValue(is.read_int32()?));
                },
                16 => {
                    self.oneof_value = ::std::option::Option::Some(value::Oneof_value::LongValue(is.read_int64()?));
                },
                26 => {
                    self.oneof_value = ::std::option::Option::Some(value::Oneof_value::StringValue(is.read_string()?));
                },
                33 => {
                    self.oneof_value = ::std::option::Option::Some(value::Oneof_value::DoubleValue(is.read_double()?));
                },
                40 => {
                    self.value_type = is.read_enum_or_unknown()?;
                },
                tag => {
                    ::protobuf::rt::read_unknown_or_skip_group(tag, is, self.special_fields.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u64 {
        let mut my_size = 0;
        if self.value_type != ::protobuf::EnumOrUnknown::new(ValueType::INT) {
            my_size += ::protobuf::rt::int32_size(5, self.value_type.value());
        }
        if let ::std::option::Option::Some(ref v) = self.oneof_value {
            match v {
                &value::Oneof_value::IntValue(v) => {
                    my_size += ::protobuf::rt::int32_size(1, v);
                },
                &value::Oneof_value::LongValue(v) => {
                    my_size += ::protobuf::rt::int64_size(2, v);
                },
                &value::Oneof_value::StringValue(ref v) => {
                    my_size += ::protobuf::rt::string_size(3, &v);
                },
                &value::Oneof_value::DoubleValue(v) => {
                    my_size += 1 + 8;
                },
            };
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.special_fields.unknown_fields());
        self.special_fields.cached_size().set(my_size as u32);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::Result<()> {
        if self.value_type != ::protobuf::EnumOrUnknown::new(ValueType::INT) {
            os.write_enum(5, ::protobuf::EnumOrUnknown::value(&self.value_type))?;
        }
        if let ::std::option::Option::Some(ref v) = self.oneof_value {
            match v {
                &value::Oneof_value::IntValue(v) => {
                    os.write_int32(1, v)?;
                },
                &value::Oneof_value::LongValue(v) => {
                    os.write_int64(2, v)?;
                },
                &value::Oneof_value::StringValue(ref v) => {
                    os.write_string(3, v)?;
                },
                &value::Oneof_value::DoubleValue(v) => {
                    os.write_double(4, v)?;
                },
            };
        }
        os.write_unknown_fields(self.special_fields.unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn special_fields(&self) -> &::protobuf::SpecialFields {
        &self.special_fields
    }

    fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {
        &mut self.special_fields
    }

    fn new() -> Value {
        Value::new()
    }

    fn clear(&mut self) {
        self.oneof_value = ::std::option::Option::None;
        self.oneof_value = ::std::option::Option::None;
        self.oneof_value = ::std::option::Option::None;
        self.oneof_value = ::std::option::Option::None;
        self.value_type = ::protobuf::EnumOrUnknown::new(ValueType::INT);
        self.special_fields.clear();
    }

    fn default_instance() -> &'static Value {
        static instance: Value = Value {
            value_type: ::protobuf::EnumOrUnknown::from_i32(0),
            oneof_value: ::std::option::Option::None,
            special_fields: ::protobuf::SpecialFields::new(),
        };
        &instance
    }
}

impl ::protobuf::MessageFull for Value {
    fn descriptor() -> ::protobuf::reflect::MessageDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().message_by_package_relative_name("Value").unwrap()).clone()
    }
}

impl ::std::fmt::Display for Value {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Value {
    type RuntimeType = ::protobuf::reflect::rt::RuntimeTypeMessage<Self>;
}

/// Nested message and enums of message `Value`
pub mod value {

    #[derive(Clone,PartialEq,Debug)]
    #[non_exhaustive]
    // @@protoc_insertion_point(oneof:com.tencent.easygraph.model.Value.oneof_value)
    pub enum Oneof_value {
        // @@protoc_insertion_point(oneof_field:com.tencent.easygraph.model.Value.int_value)
        IntValue(i32),
        // @@protoc_insertion_point(oneof_field:com.tencent.easygraph.model.Value.long_value)
        LongValue(i64),
        // @@protoc_insertion_point(oneof_field:com.tencent.easygraph.model.Value.string_value)
        StringValue(::std::string::String),
        // @@protoc_insertion_point(oneof_field:com.tencent.easygraph.model.Value.double_value)
        DoubleValue(f64),
    }

    impl ::protobuf::Oneof for Oneof_value {
    }

    impl ::protobuf::OneofFull for Oneof_value {
        fn descriptor() -> ::protobuf::reflect::OneofDescriptor {
            static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::OneofDescriptor> = ::protobuf::rt::Lazy::new();
            descriptor.get(|| <super::Value as ::protobuf::MessageFull>::descriptor().oneof_by_name("oneof_value").unwrap()).clone()
        }
    }

    impl Oneof_value {
        pub(in super) fn generated_oneof_descriptor_data() -> ::protobuf::reflect::GeneratedOneofDescriptorData {
            ::protobuf::reflect::GeneratedOneofDescriptorData::new::<Oneof_value>("oneof_value")
        }
    }
}

#[derive(Clone,Copy,PartialEq,Eq,Debug,Hash)]
// @@protoc_insertion_point(enum:com.tencent.easygraph.model.ValueType)
pub enum ValueType {
    // @@protoc_insertion_point(enum_value:com.tencent.easygraph.model.ValueType.INT)
    INT = 0,
    // @@protoc_insertion_point(enum_value:com.tencent.easygraph.model.ValueType.LONG)
    LONG = 1,
    // @@protoc_insertion_point(enum_value:com.tencent.easygraph.model.ValueType.STRING)
    STRING = 2,
    // @@protoc_insertion_point(enum_value:com.tencent.easygraph.model.ValueType.DOUBLE)
    DOUBLE = 3,
}

impl ::protobuf::Enum for ValueType {
    const NAME: &'static str = "ValueType";

    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<ValueType> {
        match value {
            0 => ::std::option::Option::Some(ValueType::INT),
            1 => ::std::option::Option::Some(ValueType::LONG),
            2 => ::std::option::Option::Some(ValueType::STRING),
            3 => ::std::option::Option::Some(ValueType::DOUBLE),
            _ => ::std::option::Option::None
        }
    }

    const VALUES: &'static [ValueType] = &[
        ValueType::INT,
        ValueType::LONG,
        ValueType::STRING,
        ValueType::DOUBLE,
    ];
}

impl ::protobuf::EnumFull for ValueType {
    fn enum_descriptor() -> ::protobuf::reflect::EnumDescriptor {
        static descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::rt::Lazy::new();
        descriptor.get(|| file_descriptor().enum_by_package_relative_name("ValueType").unwrap()).clone()
    }

    fn descriptor(&self) -> ::protobuf::reflect::EnumValueDescriptor {
        let index = *self as usize;
        Self::enum_descriptor().value_by_index(index)
    }
}

impl ::std::default::Default for ValueType {
    fn default() -> Self {
        ValueType::INT
    }
}

impl ValueType {
    fn generated_enum_descriptor_data() -> ::protobuf::reflect::GeneratedEnumDescriptorData {
        ::protobuf::reflect::GeneratedEnumDescriptorData::new::<ValueType>("ValueType")
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\tfoo.proto\x12\x1bcom.tencent.easygraph.model\"\xe7\x01\n\x05Value\
    \x12\x1d\n\tint_value\x18\x01\x20\x01(\x05H\0R\x08intValue\x12\x1f\n\nlo\
    ng_value\x18\x02\x20\x01(\x03H\0R\tlongValue\x12#\n\x0cstring_value\x18\
    \x03\x20\x01(\tH\0R\x0bstringValue\x12#\n\x0cdouble_value\x18\x04\x20\
    \x01(\x01H\0R\x0bdoubleValue\x12E\n\nvalue_type\x18\x05\x20\x01(\x0e2&.c\
    om.tencent.easygraph.model.ValueTypeR\tvalueTypeB\r\n\x0boneof_value*6\n\
    \tValueType\x12\x07\n\x03INT\x10\0\x12\x08\n\x04LONG\x10\x01\x12\n\n\x06\
    STRING\x10\x02\x12\n\n\x06DOUBLE\x10\x03J\xae\x04\n\x06\x12\x04\0\0\x12\
    \x01\n\x08\n\x01\x0c\x12\x03\0\0\x12\n\x08\n\x01\x02\x12\x03\x01\0$\n\n\
    \n\x02\x04\0\x12\x04\x03\0\x0b\x01\n\n\n\x03\x04\0\x01\x12\x03\x03\x08\r\
    \n\x0c\n\x04\x04\0\x08\0\x12\x04\x04\x02\t\x03\n\x0c\n\x05\x04\0\x08\0\
    \x01\x12\x03\x04\x08\x13\n\x0b\n\x04\x04\0\x02\0\x12\x03\x05\x04\x1e\n\
    \x0c\n\x05\x04\0\x02\0\x05\x12\x03\x05\x04\t\n\x0c\n\x05\x04\0\x02\0\x01\
    \x12\x03\x05\n\x13\n\x0c\n\x05\x04\0\x02\0\x03\x12\x03\x05\x1c\x1d\n\x0b\
    \n\x04\x04\0\x02\x01\x12\x03\x06\x04\x1e\n\x0c\n\x05\x04\0\x02\x01\x05\
    \x12\x03\x06\x04\t\n\x0c\n\x05\x04\0\x02\x01\x01\x12\x03\x06\n\x14\n\x0c\
    \n\x05\x04\0\x02\x01\x03\x12\x03\x06\x1c\x1d\n\x0b\n\x04\x04\0\x02\x02\
    \x12\x03\x07\x04\x1e\n\x0c\n\x05\x04\0\x02\x02\x05\x12\x03\x07\x04\n\n\
    \x0c\n\x05\x04\0\x02\x02\x01\x12\x03\x07\x0b\x17\n\x0c\n\x05\x04\0\x02\
    \x02\x03\x12\x03\x07\x1c\x1d\n\x0b\n\x04\x04\0\x02\x03\x12\x03\x08\x04\
    \x1e\n\x0c\n\x05\x04\0\x02\x03\x05\x12\x03\x08\x04\n\n\x0c\n\x05\x04\0\
    \x02\x03\x01\x12\x03\x08\x0b\x17\n\x0c\n\x05\x04\0\x02\x03\x03\x12\x03\
    \x08\x1c\x1d\n\x0b\n\x04\x04\0\x02\x04\x12\x03\n\x02\x1b\n\r\n\x05\x04\0\
    \x02\x04\x04\x12\x04\n\x02\t\x03\n\x0c\n\x05\x04\0\x02\x04\x06\x12\x03\n\
    \x02\x0b\n\x0c\n\x05\x04\0\x02\x04\x01\x12\x03\n\x0c\x16\n\x0c\n\x05\x04\
    \0\x02\x04\x03\x12\x03\n\x19\x1a\n\n\n\x02\x05\0\x12\x04\r\0\x12\x01\n\n\
    \n\x03\x05\0\x01\x12\x03\r\x05\x0e\n\x0b\n\x04\x05\0\x02\0\x12\x03\x0e\
    \x02\n\n\x0c\n\x05\x05\0\x02\0\x01\x12\x03\x0e\x02\x05\n\x0c\n\x05\x05\0\
    \x02\0\x02\x12\x03\x0e\x08\t\n\x0b\n\x04\x05\0\x02\x01\x12\x03\x0f\x02\
    \x0b\n\x0c\n\x05\x05\0\x02\x01\x01\x12\x03\x0f\x02\x06\n\x0c\n\x05\x05\0\
    \x02\x01\x02\x12\x03\x0f\t\n\n\x0b\n\x04\x05\0\x02\x02\x12\x03\x10\x02\r\
    \n\x0c\n\x05\x05\0\x02\x02\x01\x12\x03\x10\x02\x08\n\x0c\n\x05\x05\0\x02\
    \x02\x02\x12\x03\x10\x0b\x0c\n\x0b\n\x04\x05\0\x02\x03\x12\x03\x11\x02\r\
    \n\x0c\n\x05\x05\0\x02\x03\x01\x12\x03\x11\x02\x08\n\x0c\n\x05\x05\0\x02\
    \x03\x02\x12\x03\x11\x0b\x0cb\x06proto3\
";

/// `FileDescriptorProto` object which was a source for this generated file
fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    static file_descriptor_proto_lazy: ::protobuf::rt::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::rt::Lazy::new();
    file_descriptor_proto_lazy.get(|| {
        ::protobuf::Message::parse_from_bytes(file_descriptor_proto_data).unwrap()
    })
}

/// `FileDescriptor` object which allows dynamic access to files
pub fn file_descriptor() -> &'static ::protobuf::reflect::FileDescriptor {
    static generated_file_descriptor_lazy: ::protobuf::rt::Lazy<::protobuf::reflect::GeneratedFileDescriptor> = ::protobuf::rt::Lazy::new();
    static file_descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::FileDescriptor> = ::protobuf::rt::Lazy::new();
    file_descriptor.get(|| {
        let generated_file_descriptor = generated_file_descriptor_lazy.get(|| {
            let mut deps = ::std::vec::Vec::with_capacity(0);
            let mut messages = ::std::vec::Vec::with_capacity(1);
            messages.push(Value::generated_message_descriptor_data());
            let mut enums = ::std::vec::Vec::with_capacity(1);
            enums.push(ValueType::generated_enum_descriptor_data());
            ::protobuf::reflect::GeneratedFileDescriptor::new_generated(
                file_descriptor_proto(),
                deps,
                messages,
                enums,
            )
        });
        ::protobuf::reflect::FileDescriptor::new_generated_2(generated_file_descriptor)
    })
}
