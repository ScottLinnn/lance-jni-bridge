use arrow::array::{Int32Array, StringArray};
use arrow::array::{RecordBatch, RecordBatchIterator};
use arrow::datatypes::{DataType, Field, Schema};
use lance::dataset::fragment::FileFragment;
use lance::dataset::WriteParams;
use lance::Dataset;
use std::sync::Arc;

async fn create_frag() {
    let test_uri = "/home/scott/lance-jni-bridge/test_frag";

    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));

    let in_memory_batch = 1024;
    let batches: Vec<RecordBatch> = (0..10)
        .map(|i| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(
                    i * in_memory_batch..(i + 1) * in_memory_batch,
                ))],
            )
            .unwrap()
        })
        .collect();

    let batch_iter = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

    FileFragment::create(
        test_uri,
        10,
        batch_iter,
        Some(WriteParams {
            max_rows_per_group: 100,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
}

async fn create_dataset(test_uri: &str) -> Dataset {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int32, true),
        Field::new("s", DataType::Utf8, true),
    ]));

    let batches: Vec<RecordBatch> = (0..10)
        .map(|i| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(i * 20..(i + 1) * 20)),
                    Arc::new(StringArray::from_iter_values(
                        (i * 20..(i + 1) * 20).map(|v| format!("s-{}", v)),
                    )),
                ],
            )
            .unwrap()
        })
        .collect();

    let write_params = WriteParams {
        max_rows_per_file: 40,
        max_rows_per_group: 10,
        ..Default::default()
    };
    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    Dataset::write(batches, test_uri, Some(write_params))
        .await
        .unwrap();

    Dataset::open(test_uri).await.unwrap()
}
fn main() {
    let test_uri = "/home/scott/lance-jni-bridge/test_dataset";
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(create_dataset(test_uri));
}
