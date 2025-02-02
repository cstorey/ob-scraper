use std::{io::Write, path::Path};

use color_eyre::Result;
use serde::Serialize;
use tokio::task::spawn_blocking;
use tracing::{instrument, Span};

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
        let mut f = tempfile::NamedTempFile::new_in(parent)?;
        serde_json::to_writer_pretty(&mut f, &state)?;
        f.flush()?;
        f.persist(&path)?;

        Ok(())
    })
    .await??;

    Ok(())
}
