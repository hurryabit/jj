use rusqlite::Connection;
use rusqlite::types::ValueRef;
use rusqlite::vtab::Context;
use rusqlite::vtab::Filters;
use rusqlite::vtab::IndexInfo;
use rusqlite::vtab::VTab;
use rusqlite::vtab::VTabConfig;
use rusqlite::vtab::VTabCursor;
use rusqlite::vtab::eponymous_only_module;
use rusqlite::vtab::sqlite3_vtab;
use rusqlite::vtab::sqlite3_vtab_cursor;

/// Registers the `unpack_i64s` and `unpack_blobs` table-valued functions on `conn`.
///
/// `unpack_i64s(blob)` accepts a BLOB of native-endian `i64` values and yields
/// one `val INTEGER` row per value:
/// ```sql
/// SELECT row_id, id FROM files WHERE row_id IN (SELECT val FROM unpack_i64s(?1))
/// ```
///
/// `unpack_blobs(data, stride)` accepts a BLOB that is a concatenation of
/// fixed-size sub-blobs and yields one `val BLOB` row per chunk:
/// ```sql
/// SELECT id, row_id FROM files WHERE id IN (SELECT val FROM unpack_blobs(?1, 64))
/// ```
pub fn load_module(conn: &Connection) -> rusqlite::Result<()> {
    conn.create_module("unpack_i64s", eponymous_only_module::<UnpackI64sTab>(), None)?;
    conn.create_module("unpack_blobs", eponymous_only_module::<UnpackBlobsTab>(), None)?;
    Ok(())
}

#[repr(C)]
struct UnpackI64sTab {
    base: sqlite3_vtab,
}

// Column indices in the schema below.
const COL_VAL: i32 = 0;
const COL_BLOB: i32 = 1; // hidden input parameter

unsafe impl VTab<'_> for UnpackI64sTab {
    type Aux = ();
    type Cursor = UnpackI64sCursor;

    fn connect(
        db: &mut rusqlite::vtab::VTabConnection,
        _aux: Option<&()>,
        _args: &[&[u8]],
    ) -> rusqlite::Result<(String, Self)> {
        let vtab = Self {
            base: sqlite3_vtab::default(),
        };
        db.config(VTabConfig::Innocuous)?;
        Ok((
            "CREATE TABLE x(val INTEGER, blob BLOB HIDDEN)".to_owned(),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        for (i, c) in info.constraints().enumerate() {
            if c.column() == COL_BLOB && c.is_usable() {
                let mut usage = info.constraint_usage(i);
                usage.set_argv_index(1);
                usage.set_omit(true);
                info.set_idx_num(1);
                info.set_estimated_cost(1.0);
                return Ok(());
            }
        }
        Err(rusqlite::Error::ModuleError(
            "unpack_i64s requires a blob argument".to_owned(),
        ))
    }

    fn open(&mut self) -> rusqlite::Result<UnpackI64sCursor> {
        Ok(UnpackI64sCursor {
            base: sqlite3_vtab_cursor::default(),
            data: Vec::new(),
            pos: 0,
        })
    }
}

#[repr(C)]
struct UnpackI64sCursor {
    base: sqlite3_vtab_cursor,
    data: Vec<i64>,
    pos: usize,
}

unsafe impl VTabCursor for UnpackI64sCursor {
    fn filter(
        &mut self,
        idx_num: std::ffi::c_int,
        _idx_str: Option<&str>,
        args: &Filters<'_>,
    ) -> rusqlite::Result<()> {
        assert_eq!(idx_num, 1, "idx_num={idx_num} in despite after best_index");
        let blob = match args
            .iter()
            .next()
            .expect("not enough args in filter despite best_index")
        {
            ValueRef::Blob(blob) => blob,
            other => {
                return Err(rusqlite::Error::InvalidFilterParameterType(
                    0,
                    other.data_type(),
                ));
            }
        };
        self.data = blob
            .chunks_exact(8)
            .map(|c| i64::from_ne_bytes(c.try_into().unwrap()))
            .collect();
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        self.pos += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.data.len()
    }

    fn column(&self, ctx: &mut Context, col: std::ffi::c_int) -> rusqlite::Result<()> {
        if col == COL_VAL {
            ctx.set_result(&self.data[self.pos])?;
        }
        Ok(())
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.pos as i64)
    }
}

#[repr(C)]
struct UnpackBlobsTab {
    base: sqlite3_vtab,
}

const BLOBS_COL_VAL: i32 = 0;
const BLOBS_COL_DATA: i32 = 1;
const BLOBS_COL_STRIDE: i32 = 2;

unsafe impl VTab<'_> for UnpackBlobsTab {
    type Aux = ();
    type Cursor = UnpackBlobsCursor;

    fn connect(
        db: &mut rusqlite::vtab::VTabConnection,
        _aux: Option<&()>,
        _args: &[&[u8]],
    ) -> rusqlite::Result<(String, Self)> {
        db.config(VTabConfig::Innocuous)?;
        Ok((
            "CREATE TABLE x(val BLOB, data BLOB HIDDEN, stride INTEGER HIDDEN)".to_owned(),
            Self { base: sqlite3_vtab::default() },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        let mut data_idx = None;
        let mut stride_idx = None;
        for (i, c) in info.constraints().enumerate() {
            if !c.is_usable() {
                continue;
            }
            if c.column() == BLOBS_COL_DATA && data_idx.is_none() {
                data_idx = Some(i);
            } else if c.column() == BLOBS_COL_STRIDE && stride_idx.is_none() {
                stride_idx = Some(i);
            }
        }
        match (data_idx, stride_idx) {
            (Some(di), Some(si)) => {
                info.constraint_usage(di).set_argv_index(1);
                info.constraint_usage(di).set_omit(true);
                info.constraint_usage(si).set_argv_index(2);
                info.constraint_usage(si).set_omit(true);
                info.set_idx_num(1);
                info.set_estimated_cost(1.0);
                Ok(())
            }
            _ => Err(rusqlite::Error::ModuleError(
                "unpack_blobs requires both data and stride arguments".to_owned(),
            )),
        }
    }

    fn open(&mut self) -> rusqlite::Result<UnpackBlobsCursor> {
        Ok(UnpackBlobsCursor {
            base: sqlite3_vtab_cursor::default(),
            data: Vec::new(),
            stride: 1,
            pos: 0,
        })
    }
}

#[repr(C)]
struct UnpackBlobsCursor {
    base: sqlite3_vtab_cursor,
    data: Vec<u8>,
    stride: usize,
    pos: usize,
}

unsafe impl VTabCursor for UnpackBlobsCursor {
    fn filter(
        &mut self,
        idx_num: std::ffi::c_int,
        _idx_str: Option<&str>,
        args: &Filters<'_>,
    ) -> rusqlite::Result<()> {
        assert_eq!(idx_num, 1);
        let mut iter = args.iter();
        let data = match iter.next().expect("missing data arg") {
            ValueRef::Blob(b) => b,
            other => {
                return Err(rusqlite::Error::InvalidFilterParameterType(
                    0,
                    other.data_type(),
                ));
            }
        };
        let stride = match iter.next().expect("missing stride arg") {
            ValueRef::Integer(n) if n > 0 => n as usize,
            ValueRef::Integer(_) => {
                return Err(rusqlite::Error::ModuleError(
                    "unpack_blobs: stride must be positive".to_owned(),
                ));
            }
            other => {
                return Err(rusqlite::Error::InvalidFilterParameterType(
                    1,
                    other.data_type(),
                ));
            }
        };
        self.data = data.to_vec();
        self.stride = stride;
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        self.pos += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.pos >= self.data.len() / self.stride
    }

    fn column(&self, ctx: &mut Context, col: std::ffi::c_int) -> rusqlite::Result<()> {
        if col == BLOBS_COL_VAL {
            let start = self.pos * self.stride;
            ctx.set_result(&&self.data[start..start + self.stride])?;
        }
        Ok(())
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.pos as i64)
    }
}
