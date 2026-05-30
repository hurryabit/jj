mod import_git;
mod simhash_files;

use std::io::Write as _;
use std::path::PathBuf;

use bytesize::ByteSize;
use jj_cli::cli_util::CliRunner;
use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::command_error::internal_error;
use jj_cli::ui::Ui;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::Repo as _;
use jj_lib::repo::StoreFactories;
use jj_lib::signing::Signer;
use jj_lib::workspace::Workspace;
use jj_lib::workspace::WorkspaceInitError;
use jj_lib::workspace::default_working_copy_factory;
use jj_sql_lib::SqlBackend;
use jj_sql_lib::SqlOpHeadsStore;
use jj_sql_lib::SqlOpStore;

/// Top-level commands specific to the SQL-backed jj binary.
#[derive(clap::Subcommand, Clone, Debug)]
enum SqlCommand {
    /// SQL backend commands.
    Sql(SqlArgs),
}

#[derive(clap::Args, Clone, Debug)]
struct SqlArgs {
    #[command(subcommand)]
    command: SqlSubcommand,
}

#[derive(clap::Subcommand, Clone, Debug)]
enum SqlSubcommand {
    /// Initialize a new workspace backed by a SQL (SQLite) database.
    Init(InitArgs),
    /// Import all commits from a git repository into a new SQL-backed
    /// workspace.
    GitImport(import_git::ImportGitArgs),
    /// Print statistics about the current repository.
    Stats(StatsArgs),
    /// Compute and store the simhash for all files that do not have one yet.
    SimhashFiles(simhash_files::SimhashFilesArgs),
}

#[derive(clap::Args, Clone, Debug)]
struct StatsArgs {
    /// Also print per-table storage statistics from the SQLite dbstat table.
    #[arg(long)]
    db: bool,
}

#[derive(clap::Args, Clone, Debug)]
struct InitArgs {
    /// Directory to initialize (default: current directory).
    path: Option<PathBuf>,
}

fn create_store_factories() -> StoreFactories {
    let mut store_factories = StoreFactories::empty();
    // Register load factories so jj-sql can open existing SQL-backed repos.
    // Each name must match the corresponding `::name()` associated function,
    // which is what jj writes into the `type` file on init.
    store_factories.add_backend(
        SqlBackend::name(),
        Box::new(|settings, store_path| Ok(Box::new(SqlBackend::load(settings, store_path)?))),
    );
    store_factories.add_op_store(
        SqlOpStore::name(),
        Box::new(|_settings, store_path, root_data| {
            Ok(Box::new(SqlOpStore::load(store_path, root_data)?))
        }),
    );
    store_factories.add_op_heads_store(
        SqlOpHeadsStore::name(),
        Box::new(|_settings, store_path| Ok(Box::new(SqlOpHeadsStore::load(store_path)?))),
    );
    store_factories
}

async fn run_stats(
    ui: &mut Ui,
    command_helper: &CommandHelper,
    args: &StatsArgs,
) -> Result<(), CommandError> {
    let workspace = command_helper.workspace_helper(ui).await?;
    let Some(backend) = workspace.repo().store().backend_impl::<SqlBackend>() else {
        return Err(internal_error("not a SQL-backed repository"));
    };
    let stats = backend.stats().map_err(internal_error)?;
    writeln!(ui.stdout(), "Commits:              {:>12}", stats.commits)?;
    writeln!(ui.stdout(), "Trees:                {:>12}", stats.trees)?;
    writeln!(ui.stdout(), "Blobs:                {:>12}", stats.blobs)?;
    writeln!(
        ui.stdout(),
        "Blob compressed:      {:>12}",
        ByteSize(stats.blob_compressed_bytes as u64),
    )?;
    writeln!(
        ui.stdout(),
        "Blob uncompressed:    {:>12}",
        ByteSize(stats.blob_uncompressed_bytes as u64),
    )?;
    writeln!(
        ui.stdout(),
        "DB size on disk:      {:>12}",
        ByteSize(stats.db_size_bytes),
    )?;
    if args.db {
        let db_stats = backend.db_stats().map_err(internal_error)?;
        let name_width = db_stats
            .iter()
            .map(|r| r.name.len())
            .max()
            .unwrap_or(0)
            .max("Table".len());
        writeln!(ui.stdout())?;
        writeln!(
            ui.stdout(),
            "{:<name_width$} {:>12}  {:>10}  {:>10}",
            "Table", "Payload", "Rows", "Cells",
        )?;
        writeln!(ui.stdout(), "{}", "-".repeat(name_width + 40))?;
        for row in &db_stats {
            writeln!(
                ui.stdout(),
                "{:<name_width$} {:>12}  {:>10}  {:>10}",
                row.name,
                ByteSize(row.payload_bytes as u64),
                row.rows,
                row.cells,
            )?;
        }
    }
    Ok(())
}

async fn run_sql_command(
    ui: &mut Ui,
    command_helper: &CommandHelper,
    command: SqlCommand,
) -> Result<(), CommandError> {
    match command {
        SqlCommand::Sql(SqlArgs {
            command: SqlSubcommand::Stats(args),
        }) => run_stats(ui, command_helper, &args).await,
        SqlCommand::Sql(SqlArgs {
            command: SqlSubcommand::SimhashFiles(args),
        }) => simhash_files::run(ui, command_helper, &args).await,
        SqlCommand::Sql(SqlArgs {
            command: SqlSubcommand::GitImport(args),
        }) => import_git::run(command_helper.settings(), command_helper.cwd(), &args)
            .await
            .map_err(internal_error),
        SqlCommand::Sql(SqlArgs {
            command: SqlSubcommand::Init(args),
        }) => {
            let wc_path = match &args.path {
                Some(p) => p.as_path(),
                None => command_helper.cwd(),
            };
            std::fs::create_dir_all(wc_path)?;
            let settings = command_helper.settings_for_new_workspace(ui, wc_path)?.0;
            Workspace::init_with_factories(
                &settings,
                wc_path,
                &|settings, store_path| Ok(Box::new(SqlBackend::init(settings, store_path)?)),
                Signer::from_settings(&settings).map_err(WorkspaceInitError::SignInit)?,
                &|_settings, store_path, root_data| {
                    Ok(Box::new(SqlOpStore::init(store_path, root_data)?))
                },
                &|_settings, store_path, root_op_id| {
                    Ok(Box::new(SqlOpHeadsStore::init(store_path, root_op_id)?))
                },
                ReadonlyRepo::default_index_store_initializer(),
                ReadonlyRepo::default_submodule_store_initializer(),
                &*default_working_copy_factory(),
                WorkspaceName::DEFAULT.to_owned(),
            )
            .await?;
            Ok(())
        }
    }
}

fn main() -> std::process::ExitCode {
    CliRunner::init()
        .name("jj")
        .about("Jujitsu with experimental SQLite backend")
        .version("0.0.1")
        .add_store_factories(create_store_factories())
        .add_subcommand(run_sql_command)
        .run()
        .into()
}
