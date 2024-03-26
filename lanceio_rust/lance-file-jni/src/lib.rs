use arrow::array::{Array, ArrayData, Int32Array, RecordBatch};
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use jni::objects::{JClass, JIntArray, JObject, JObjectArray, JString, JValue};
use jni::sys::{jint, jintArray, jlongArray, jobject, jobjectArray, jsize, jstring};
use jni::JNIEnv;
use lance::Dataset;
use tokio::runtime::Runtime;

pub mod create_file;

#[no_mangle]
pub extern "system" fn Java_jni_LanceReader_hello<'local>(
    mut env: JNIEnv<'local>,
    // This is the class that owns our static method. It's not going to be used,
    // but still must be present to match the expected signature of a static
    // native method.
    class: JClass<'local>,
    input: JString<'local>,
) -> jstring {
    // First, we have to get the string out of Java. Check out the `strings`
    // module for more info on how this works.
    let input: String = env
        .get_string(&input)
        .expect("Couldn't get java string!")
        .into();
    let new_input = input + "hello from Rust!";
    // Then we have to create a new Java string to return. Again, more info
    // in the `strings` module.
    let output = env
        .new_string(format!("Hello, {}!", new_input))
        .expect("Couldn't create java string!");

    // Finally, extract the raw pointer to return.
    output.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_jni_LanceReader_readRangeJni<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
    start: jint,
    end: jint,
) -> jlongArray {
    // Get the path string from the Java side
    let path_str: String = env.get_string(&path).unwrap().into();

    let mut dataset = None;
    // Use a runtime to open the dataset
    let rt = Runtime::new().unwrap();

    // Block the current thread until the asynchronous function completes

    rt.block_on(async {
        // Open the dataset asynchronously
        dataset = Some(Dataset::open(&path_str).await.unwrap());
    });

    let schema = dataset.as_ref().unwrap().schema();
    let fragment = &dataset.as_ref().unwrap().get_fragments()[0];
    let mut record_batch = None;
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let reader = fragment.open(schema, false).await.unwrap();
        // Build range from start and end
        let range = start as usize..end as usize;
        record_batch = Some(reader.read_range(range).await.unwrap());
    });

    // Convert the record batch to a list of two-size array
    let array_list = export_array(record_batch.unwrap());

    let len = array_list.len();
    let arr_size = 2 * len;

    // init a i64 array with size arr_size
    let arr = env.new_long_array(arr_size as jsize).unwrap();

    // set the elements of the array
    let mut start = 0;
    for pair in array_list {
        env.set_long_array_region(&arr, start, &pair).unwrap();
        start += 2;
    }

    arr.as_raw()
}

#[no_mangle]
pub extern "system" fn Java_jni_LanceReader_readIndexJni<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
    indices: JIntArray<'local>,
) -> jlongArray {
    // Get the path string from the Java side
    let path_str: String = env.get_string(&path).unwrap().into();

    // Get the indices array from the Java side
    let indices_vec: Vec<u32> = unsafe {
        let ae = env
            .get_array_elements(&indices, jni::objects::ReleaseMode::NoCopyBack)
            .unwrap();
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

    let mut dataset = None;
    // Use a runtime to open the dataset
    let rt = Runtime::new().unwrap();

    // Block the current thread until the asynchronous function completes

    rt.block_on(async {
        // Open the dataset asynchronously
        dataset = Some(Dataset::open(&path_str).await.unwrap());
    });

    let schema = dataset.as_ref().unwrap().schema();
    let fragment = &dataset.as_ref().unwrap().get_fragments()[1];
    let mut record_batch: Option<RecordBatch> = None;
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        record_batch = Some(fragment.take(&indices_vec[..], schema).await.unwrap());
    });

    // Convert the record batch to a list of two-size array
    let array_list = export_array(record_batch.unwrap());

    let len = array_list.len();
    let arr_size = 2 * len;

    // init a i64 array with size arr_size
    let arr = env.new_long_array(arr_size as jsize).unwrap();

    // set the elements of the array
    let mut start = 0;
    for pair in array_list {
        env.set_long_array_region(&arr, start, &pair).unwrap();
        start += 2;
    }

    arr.as_raw()
}

pub fn export_array(rb: RecordBatch) -> Vec<[i64; 2]> {
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
