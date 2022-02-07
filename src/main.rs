use std::{cmp, process};
use std::fmt::Debug;
use std::io::{BufRead, Read, SeekFrom};
use std::io::BufReader;
use std::path::Path;
use std::time::Instant;

use async_std::fs::File;
use async_std::prelude::*;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{ByteStream, Client, Error, PKG_VERSION, Region};
use aws_sdk_s3::Endpoint;
use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart};
use bytes::Bytes;
use futures_util::future::join_all;
use futures_util::FutureExt;
use http::Uri;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Opt {
    /// The AWS Region.
    #[structopt(short, long)]
    region: Option<String>,

    /// The name of the bucket.
    #[structopt(short, long)]
    bucket: String,

    /// The name of the file to upload.
    #[structopt(short, long)]
    filename: String,

    /// The name of the object in the bucket.
    #[structopt(short, long)]
    key: String,

    /// Whether to display additional information.
    #[structopt(short, long)]
    verbose: bool,
}

const CHUNK_SIZE: usize = 10242880;

async fn upload_part(
    part_id: i32,
    upload_id: &str,
    client: &Client,
    bucket: &str,
    key: &str,
    chunk: Vec<u8>,
) -> CompletedPart {

    //println!("part: {}, chunk length: {}", part_id, chunk.len());
    let stream = ByteStream::from(chunk);

    let resp = client
        .upload_part()
        .upload_id(upload_id)
        .body(stream)
        .part_number(part_id)
        .bucket(bucket)
        .key(key)
        .send()
        .await.unwrap();


    return CompletedPart::builder().set_e_tag(resp.e_tag).part_number(part_id).build();
}


// Upload a file to a bucket.
// snippet-start:[s3.rust.s3-helloworld]
async fn upload_object(
    client: &Client,
    bucket: &str,
    filename: &str,
    key: &str,
) -> Result<(), Error> {
    let multipart_upload = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key).send().await?;

    let mut file = File::open(filename).await.unwrap();
    let mut data = Vec::new();
    file.read_to_end(&mut data).await.unwrap();

    let mut futures = Vec::new();
    let mut part_id = 1;

    for chunk in data.chunks(CHUNK_SIZE) {
        futures.push(
            upload_part(
                part_id as i32,
                multipart_upload.upload_id.as_ref().unwrap(),
                &client,
                bucket,
                key,
                chunk.to_vec(),
            )
        );
        part_id += 1;
    }

    let joined_futures = join_all(futures);
    let mut parts: Vec<CompletedPart> = joined_futures.await;
    // Sort in ascending order
    parts.sort_by_key(|a| a.part_number);
    //println!("Parts: {:?}", parts);
    let completed_parts = CompletedMultipartUpload::builder()
        .set_parts(Option::Some(parts))
        .build();

    let _complete_multipart_upload_resp = client.complete_multipart_upload()
        .upload_id(multipart_upload.upload_id.as_ref().unwrap())
        .key(key)
        .bucket(bucket)
        .multipart_upload(completed_parts)
        .send().await;

    match _complete_multipart_upload_resp {
        Ok(response) => {
            println!("Success!");
        }
        Err(error) => {
            println!("Error occurred. Multipart upload will be aborted. ");
            client.abort_multipart_upload()
                .upload_id(multipart_upload.upload_id.as_ref().unwrap())
                .key(key)
                .bucket(bucket)
                .send()
                .await.unwrap();

            return Err(error.into());
        }
    }

    Ok(())
}
// snippet-end:[s3.rust.s3-helloworld]

/// Lists your buckets and uploads a file to a bucket.
/// # Arguments
///
/// * `-b BUCKET` - The bucket to which the file is uploaded.
/// * `-k KEY` - The name of the file to upload to the bucket.
/// * `[-r REGION]` - The Region in which the client is created.
///    If not supplied, uses the value of the **AWS_REGION** environment variable.
///    If the environment variable is not set, defaults to **us-west-2**.
/// * `[-v]` - Whether to display additional information.
#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();

    let Opt {
        bucket,
        filename,
        key,
        region,
        verbose,
    } = Opt::from_args();

    let region_provider = RegionProviderChain::first_try(region.map(Region::new))
        .or_default_provider()
        .or_else(Region::new("us-west-2"));

    println!();

    if verbose {
        println!("S3 client version: {}", PKG_VERSION);
        println!(
            "Region:            {}",
            region_provider.region().await.unwrap().as_ref()
        );
        println!("Bucket:            {}", &bucket);
        println!("Filename:          {}", &filename);
        println!("Key:               {}", &key);
        println!();
    }
    let shared_config = aws_config::from_env().load().await;
    let local_config = aws_sdk_s3::config::Builder::from(&shared_config).endpoint_resolver(Endpoint::immutable(Uri::from_static("http://localhost:9000")),
    )
        .build();

    let client = Client::from_conf(local_config);

    upload_object(&client, &bucket, &filename, &key).await
}