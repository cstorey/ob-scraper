use std::{fs::Permissions, io::Write, os::unix::fs::PermissionsExt, path::Path};

use color_eyre::Result;
use serde::Serialize;
use tokio::{io::AsyncWriteExt, task::spawn_blocking};
use tracing::{debug, instrument, Span};

#[instrument(skip_all, fields(?path))]
pub(crate) async fn write_atomically<T: Serialize + Send + 'static>(
    path: &Path,
    state: T,
) -> Result<()> {
    let span = Span::current();
    let path = path.to_owned();
    spawn_blocking(move || -> Result<()> {
        let _entered = span.enter();
        let parent = path.parent().unwrap_or(".".as_ref());

        let mut f = tempfile::Builder::new()
            .permissions(Permissions::from_mode(0o666))
            .tempfile_in(parent)?;
        serde_json::to_writer_pretty(&mut f, &state)?;
        f.flush()?;
        f.persist(&path)?;

        Ok(())
    })
    .await??;

    Ok(())
}

#[instrument(skip_all, fields(?path))]
pub(crate) async fn write_json_lines(path: &Path, data: &[impl Serialize]) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let of = tokio::fs::File::create(&path).await?;
    let mut of = tokio::io::BufWriter::new(of);

    let mut buf = Vec::new();
    for datum in data {
        serde_json::to_writer(&mut buf, datum)?;
        buf.push(b'\n');
        of.write_all(buf.as_ref()).await?;
        buf.clear();
    }
    of.flush().await?;

    debug!(size=%buf.len(), ?path, "Wrote data to file");

    Ok(())
}
