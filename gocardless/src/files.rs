use std::{
    fs::Permissions,
    io::{BufWriter, Write},
    os::unix::fs::PermissionsExt,
    path::Path,
};

use color_eyre::{eyre::Context, Result};
use serde::Serialize;
use tokio::task::spawn_blocking;
use tracing::{debug, instrument, Span};

#[instrument(skip_all, fields(?path))]
pub(crate) async fn write_json_atomically<T: Serialize + Send + 'static>(
    path: &Path,
    state: T,
) -> Result<()> {
    write_file_atomically(path, move |f| {
        serde_json::to_writer_pretty(f, &state)?;
        Ok(())
    })
    .await
}

async fn write_file_atomically<
    F: FnOnce(&mut dyn std::io::Write) -> Result<()> + Send + 'static,
>(
    path: &Path,
    writer: F,
) -> std::result::Result<(), color_eyre::eyre::Error> {
    let span = Span::current();
    let path = path.to_owned();
    spawn_blocking(move || -> Result<()> {
        let _entered = span.enter();
        let parent = path.parent().unwrap_or(".".as_ref());
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Creating parent: {:?}", parent))?;

        let f = tempfile::Builder::new()
            .permissions(Permissions::from_mode(0o666))
            .tempfile_in(parent)?;
        let mut f = BufWriter::new(f);
        writer(&mut f)?;
        f.flush()?;

        let tmpf = f.into_inner()?;
        tmpf.persist(&path)?;

        debug!("Wrote data to file");

        Ok(())
    })
    .await??;

    Ok(())
}

#[instrument(skip_all, fields(?path))]
pub(crate) async fn write_json_lines(
    path: &Path,
    data: impl IntoIterator<Item = impl Serialize> + Send + 'static,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    write_file_atomically(path, move |mut f| {
        for datum in data {
            serde_json::to_writer(&mut f, &datum)?;
            f.write_all(b"\n")?;
        }
        Ok(())
    })
    .await?;

    Ok(())
}
