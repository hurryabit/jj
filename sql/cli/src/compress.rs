use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::Write as _;

use balsaq::ConnectionExt;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::internal_error;
use jj_cli::ui::Ui;
use jj_lib::repo::Repo as _;
use jj_sql_lib::SimHash;
use jj_sql_lib::SqlBackend;
use jj_sql_lib::SqlBackendError;
use jj_sql_lib::model;
use rusqlite::named_params;
use zstd::Encoder;

#[derive(clap::Args, Clone, Debug)]
pub struct CompressArgs {
    /// Re-evaluate files that already have a delta base.
    #[arg(long)]
    pub recompress: bool,
}

pub async fn run(
    ui: &mut Ui,
    command_helper: &CommandHelper,
    args: &CompressArgs,
) -> Result<(), CommandError> {
    let workspace = command_helper.workspace_helper(ui).await?;
    async {
        let Some(backend) = workspace.repo().store().backend_impl::<SqlBackend>() else {
            return Err(SqlBackendError::InternalError(String::from(
                "not a SQL-backed repository",
            )));
        };
        let conn = SqlBackend::connect_db(backend.store_path())?;

        let filter = if args.recompress {
            "WHERE simhash IS NOT NULL"
        } else {
            "WHERE simhash IS NOT NULL AND delta_base_id IS NULL"
        };

        // Collect row_ids cheaply upfront — no content loaded yet.
        let row_ids: Vec<model::FileRowId> = {
            let mut stmt = conn.prepare(&format!(
                "SELECT row_id FROM files {filter} ORDER BY row_id"
            ))?;
            stmt.query_map((), |r| r.get(0))?
                .collect::<Result<_, rusqlite::Error>>()?
        };

        let pb = ProgressBar::new(row_ids.len() as u64).with_style(
            ProgressStyle::default_bar()
                .template("Compressing files [{bar:40}] {pos}/{len} (eta {eta})")
                .map_err(|e| SqlBackendError::Other(e.into()))?
                .progress_chars("=> "),
        );

        let mut delta_bases: HashMap<SimHash<{ model::SIMHASH_WINDOW_SIZE }>, model::FileRowId> =
            HashMap::new();
        #[rustfmt::skip]
        let mut update_stmt =
            conn.prepare("\
                UPDATE files SET \
                    compression_mode = :compression_mode, \
                    compression_base_id = :compression_base_id, \
                    compressed_data = :compressed_data \
                WHERE row_id = :row_id \
            ")?;

        for row_id in row_ids {
            let file = conn.get::<model::File>(&row_id)?;
            let Some(simhash) = file.simhash else {
                continue;
            };
            match delta_bases.entry(simhash) {
                Entry::Vacant(vacant) => {
                    vacant.insert(row_id);
                }
                Entry::Occupied(occupied) => {
                    let base_row_id = occupied.get();
                    let base_file = conn.get::<model::File>(base_row_id)?;
                    assert!(base_file.compression_base_id.is_none());
                    let base_content = zstd::decode_all(base_file.compressed_data.as_slice())?;
                    let mut encoder = Encoder::with_dictionary(Vec::new(), 3, &base_content)?;
                    // TODO: Stream bytes from decoder into encoder.
                    let decompressed = zstd::decode_all(file.compressed_data.as_slice())?;
                    encoder.write_all(&decompressed)?;
                    let compressed_data = encoder.finish()?;
                    let old_len = file.compressed_data.len() as u64;
                    let new_len = compressed_data.len() as u64;
                    let ratio = new_len as f64 / old_len as f64;
                    if ratio <= 0.8 {
                        update_stmt.execute(named_params! {
                            ":row_id": row_id,
                            ":compression_mode": model::CompressionMode::ZSTD_SIMILAR,
                            ":compression_base_id": Some(*base_row_id),
                            ":compressed_data": compressed_data,
                        })?;
                        // eprintln!(
                        //     "{} saved ({} -> {}: {:.1}%)",
                        //     HumanBytes(old_len - new_len),
                        //     HumanBytes(old_len),
                        //     HumanBytes(new_len),
                        //     100.0 * (1.0 - ratio),
                        // );
                    }
                }
            }
            pb.inc(1);
        }

        Ok::<_, SqlBackendError>(())
    }
    .await
    .map_err(|e: SqlBackendError| internal_error(e))
}
