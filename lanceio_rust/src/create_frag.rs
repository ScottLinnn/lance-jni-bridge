// use arrow::array::{Int32Array, StringArray};
// use arrow::array::{RecordBatch, RecordBatchIterator};
// use arrow::datatypes::{DataType, Field, Schema};
// use lance::dataset::fragment::FileFragment;
// use lance::dataset::WriteParams;
// use std::sync::Arc;

// async fn create_frag() {
//     let test_uri = "test_frag";

//     let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));

//     let in_memory_batch = 1024;
//     let batches: Vec<RecordBatch> = (0..10)
//         .map(|i| {
//             RecordBatch::try_new(
//                 schema.clone(),
//                 vec![Arc::new(Int32Array::from_iter_values(
//                     i * in_memory_batch..(i + 1) * in_memory_batch,
//                 ))],
//             )
//             .unwrap()
//         })
//         .collect();

//     let batch_iter = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

//     FileFragment::create(
//         test_uri,
//         10,
//         batch_iter,
//         Some(WriteParams {
//             max_rows_per_group: 100,
//             ..Default::default()
//         }),
//     )
//     .await
//     .unwrap();
// }

// fn main() {
//     let rt = tokio::runtime::Runtime::new().unwrap();
//     rt.block_on(create_frag());
// }
