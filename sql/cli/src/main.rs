use jj_cli::cli_util::CliRunner;
use jj_cli::cli_util::CommandHelper;
use jj_cli::command_error::CommandError;
use jj_cli::ui::Ui;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::ReadonlyRepo;
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
    Init,
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

async fn run_sql_command(
    ui: &mut Ui,
    command_helper: &CommandHelper,
    command: SqlCommand,
) -> Result<(), CommandError> {
    match command {
        SqlCommand::Sql(SqlArgs {
            command: SqlSubcommand::Init,
        }) => {
            let wc_path = command_helper.cwd();
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
