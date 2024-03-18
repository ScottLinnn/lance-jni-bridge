use std::mem::ManuallyDrop;
use std::vec;

use jni::objects::{JClass, JIntArray, JObject, JString, JValue};
use jni::sys::{jint, jintArray, jobject, jstring};
use jni::JNIEnv;
use lance::arrow::schema;
use lance::Dataset;
use arrow::array::{Array, ArrayData, Int32Array, RecordBatch};
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};

#[no_mangle]
pub extern "system" fn Java_LanceReader_readRange<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
    start: jint,
    end: jint,
) -> jobject {
    // Get the path string from the Java side
    let path_str: String = env.get_string(&path).unwrap().into();

    // Your implementation for readRange goes here
    // You can use the path_str, start, and end parameters to read the data
    // and return an instance of org.apache.arrow.vector.ipc.message.ArrowRecordBatch

    // For now, we'll just return a null object
    jobject::null()
}

#[no_mangle]
pub async extern "system" fn Java_LanceReader_readIndex <'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
    indices: JIntArray<'local>,
) -> jobject {
    // Get the path string from the Java side
    let path_str: String = env.get_string(&path).unwrap().into();

    // Get the indices array from the Java side
    let indices_vec: Vec<u32> = unsafe {
        let ae = env.get_array_elements(&indices, jni::sys::ReleaseMode::NoCopyBack).unwrap();
        let len = ae.len();
        let mut vec = Vec::with_capacity(len as usize);
        let mut iter = ae.iter();
        for i in 0..len {
            let val = iter.next().unwrap();
            vec.push(val.clone() as u32);
        }
        vec
    };

    // Now this assumes the dataset at path_str exists, and the first fragment contains the indices!
    let dataset = Dataset::open(&path_str).await.unwrap();
    let schema = dataset.schema();
    let fragment = &dataset.get_fragments()[0];
    let res_len = indices_vec.len();
    // let mut vec = Vec::with_capacity(res_len as usize);
    
    let record_batch = fragment.take(&indices_vec[..],schema).await.unwrap();


    jobject::
}

pub fn export_array(rb : RecordBatch) -> Vec<[i64; 2]> {
    // Export it
    let mut vec = Vec::new();
    for i in 0..rb.num_columns() {
        let array = rb.column(i);
        let data = array.into_data();
        let out_array = FFI_ArrowArray::new(&data);
        let out_schema = FFI_ArrowSchema::try_from(data.data_type()).unwrap();

        let schema = Box::new(out_schema);
        let array = Box::new(out_array);
        let schema_addr = Box::into_raw(schema) as i64;
        let array_addr = Box::into_raw(array) as i64;

        vec.push([schema_addr, array_addr]);
    }
    vec 
    //https://docs.rs/arrow/33.0.0/arrow/ffi/index.html
    //https://arrow.apache.org/docs/java/cdata.html#java-to-c
    //https://github.com/apache/arrow-rs/blob/3761ac53cab55c269b06d9a13825dd81b03e0c11/arrow/src/ffi.rs#L579-L580
}