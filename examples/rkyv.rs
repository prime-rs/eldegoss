use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

fn main() {
    #[derive(Archive, RkyvDeserialize, RkyvSerialize, Deserialize, Serialize, Debug, PartialEq)]
    #[archive(compare(PartialEq))]
    #[archive_attr(derive(Debug))]
    struct Test {
        int: u8,
        string: String,
        option: Option<Vec<i32>>,
    }

    #[derive(Archive, RkyvDeserialize, RkyvSerialize, Deserialize, Serialize, Debug, PartialEq)]
    #[archive(compare(PartialEq))]
    #[archive_attr(derive(Debug))]
    struct Test2 {
        test: Test,
    }

    common_x::log::init_log_filter("info");
    let mut stats = eldegoss::util::Stats::new(1000);

    let test1 = Test {
        int: 42,
        string: "hello world".to_string(),
        option: Some(vec![1; 1024]),
    };
    let value = Test2 { test: test1 };

    use rkyv::ser::{serializers::AllocSerializer, Serializer};
    for _ in 0..10000 {
        let mut serializer = AllocSerializer::<32>::default();
        serializer.serialize_value(&value).unwrap();
        let _bytes = serializer.into_serializer().into_inner();
        stats.increment();
    }
    // let bytes = bincode::serialize(&value).unwrap();
    // println!("bytes len: {}", bytes.len());

    // let bytes = rkyv::to_bytes::<_, 0>(&value).unwrap();
    // println!("bytes len: {}", bytes.len());

    // Serializing is as easy as a single function call
    // let bytes = rkyv::to_bytes::<_, 256>(&value).unwrap();
    // println!("bytes len: {}", bytes.len());

    // Or you can customize your serialization for better performance
    // and compatibility with #![no_std] environments
    // use rkyv::ser::{serializers::AllocSerializer, Serializer};

    // let mut serializer = AllocSerializer::<32>::default();
    // serializer.serialize_value(&value).unwrap();
    // let bytes = serializer.into_serializer().into_inner();
    // println!("bytes len: {}", bytes.len());

    // Or you can use the unsafe API for maximum performance
    // let archived = unsafe { rkyv::archived_root::<Test2>(&bytes[..]) };
    // assert_eq!(archived, &value);

    // And you can always deserialize back to the original type
    // let deserialized: Test2 = archived.deserialize(&mut rkyv::Infallible).unwrap();
    // assert_eq!(deserialized, value);
}
