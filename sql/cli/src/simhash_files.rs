#![allow(unused)]
use std::cell::RefCell;
use std::io::Write as _;
use std::time::Instant;

use bytesize::ByteSize;
use indicatif::HumanCount;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::internal_error;
use jj_cli::ui::Ui;
use jj_lib::repo::Repo as _;
use jj_sql_lib::SimHash;
use jj_sql_lib::SqlBackend;
use rayon::prelude::*;
use zerocopy::IntoBytes as _;

const BATCH_SIZE: usize = 2048;

#[derive(clap::Args, Clone, Debug)]
pub struct SimhashFilesArgs {
    /// Recompute the simhash for all files, including those that already have
    /// one.
    #[arg(long)]
    pub rehash: bool,
}

pub async fn run(
    ui: &mut Ui,
    command_helper: &CommandHelper,
    args: &SimhashFilesArgs,
) -> Result<(), CommandError> {
    /*
    let workspace = command_helper.workspace_helper(ui).await?;
    let Some(backend) = workspace.repo().store().backend_impl::<SqlBackend>() else {
        return Err(internal_error("not a SQL-backed repository"));
    };

    let conn = SqlBackend::connect(backend.store_path(), false)
        .await
        .map_err(internal_error)?;
    let mut write_conn = SqlBackend::connect(backend.store_path(), false)
        .await
        .map_err(internal_error)?;

    let filter = if args.rehash {
        ""
    } else {
        "WHERE simhash IS NULL"
    };

    let total = conn
        .query_row(&format!("SELECT COUNT(*) FROM files {filter}"), (), |r| {
            r.get::<_, i64>(0)
        })
        .map_err(internal_error)? as u64;

    if total == 0 {
        writeln!(ui.stdout(), "All files already have a simhash.")?;
        return Ok(());
    }

    let pb = ProgressBar::new(total).with_style(
        ProgressStyle::default_bar()
            .template("Hashing files [{bar:40}] {pos}/{len} ({msg}, eta {eta})")
            .map_err(internal_error)?
            .progress_chars("=> "),
    );

    // Collect row_ids cheaply upfront — no content loaded yet.
    let row_ids: Vec<i64> = {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT row_id FROM files {filter} ORDER BY row_id"
            ))
            .map_err(internal_error)?;
        stmt.query_map((), |r| r.get(0))
            .map_err(internal_error)?
            .collect::<Result<_, rusqlite::Error>>()
            .map_err(internal_error)?
    };

    let mut fetch_stmt = conn
        .prepare(
            "SELECT row_id, content, uncompressed_size FROM files WHERE row_id IN unpack_i64s(?1)",
        )
        .map_err(internal_error)?;

    let start = Instant::now();
    let mut total_files: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut read_ms: u128 = 0;
    let mut hash_ms: u128 = 0;
    let mut write_ms: u128 = 0;

    for chunk in row_ids.chunks(BATCH_SIZE) {
        // Fetch compressed content and uncompressed size for this chunk only.
        let t = Instant::now();
        let batch: Vec<(i64, Vec<u8>, u64)> = fetch_stmt
            .query_map((chunk.as_bytes(),), |r| {
                Ok((r.get(0)?, r.get(1)?, r.get::<_, i64>(2)? as u64))
            })
            .map_err(internal_error)?
            .collect::<Result<_, rusqlite::Error>>()
            .map_err(internal_error)?;
        read_ms += t.elapsed().as_millis();

        // Decompress and hash in parallel with rayon.
        // Thread-locals reuse both the decompressor context and output buffer
        // across files, avoiding per-file heap allocation.
        thread_local! {
            static DECOMP: RefCell<zstd::bulk::Decompressor<'static>> =
                RefCell::new(zstd::bulk::Decompressor::new().unwrap());
            static BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
        }
        let t = Instant::now();
        let results: Vec<(i64, SimHash<8>)> = batch
            .par_iter()
            .map(|(row_id, compressed, uncompressed_size)| {
                DECOMP.with(|d| {
                    BUF.with(|buf| {
                        let mut d = d.borrow_mut();
                        let mut buf = buf.borrow_mut();
                        buf.resize(*uncompressed_size as usize, 0);
                        let n = d.decompress_to_buffer(compressed, &mut *buf)?;
                        let mut hasher = jj_sql_lib::SimHasher::<8>::new();
                        hasher.update(&buf[..n]);
                        Ok((*row_id, hasher.finish()))
                    })
                })
            })
            .collect::<std::io::Result<_>>()
            .map_err(internal_error)?;
        hash_ms += t.elapsed().as_millis();

        // Write results in a single transaction.
        let t = Instant::now();
        let tx = write_conn.transaction().map_err(internal_error)?;
        {
            let mut stmt = tx
                .prepare("UPDATE files SET simhash = ?1 WHERE row_id = ?2")
                .map_err(internal_error)?;
            for (row_id, hash) in &results {
                stmt.execute((*hash, *row_id)).map_err(internal_error)?;
            }
        }
        tx.commit().map_err(internal_error)?;
        write_ms += t.elapsed().as_millis();

        total_files += results.len() as u64;
        total_bytes += batch.iter().map(|(_, _, sz)| sz).sum::<u64>();
        let elapsed = start.elapsed().as_secs_f64().max(f64::EPSILON);
        pb.set_message(format!(
            "{:.0} files/s, {}/s",
            total_files as f64 / elapsed,
            ByteSize((total_bytes as f64 / elapsed) as u64),
        ));

        pb.inc(chunk.len() as u64);
    }

    pb.finish_with_message(format!("Hashed {} files  ", HumanCount(total)));
    eprintln!("read_ms={read_ms} -- hash_ms={hash_ms} -- write_ms={write_ms}");
    */
    Ok(())
}
