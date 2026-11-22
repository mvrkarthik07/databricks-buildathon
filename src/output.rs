use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use parquet::data_type::{ByteArray, ByteArrayType, DataType, Int32Type, Int64Type};
use parquet::errors::Result;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
use parquet::schema::parser::parse_message_type;

use crate::{CampaignTotals, Text};

const ROW_GROUP: usize = 64 * 1024;
const USER_SCHEMA: &str = "message user_daily {
    REQUIRED BINARY user_id (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 events;
    REQUIRED INT64 revenue_cents;
    OPTIONAL BINARY top_category (UTF8);
}";
const CAMPAIGN_SCHEMA: &str = "message campaign_daily {
    REQUIRED BINARY campaign_id (UTF8);
    REQUIRED BINARY channel (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 purchasing_sessions;
    REQUIRED INT64 revenue_cents;
    REQUIRED INT64 assisted_sessions;
}";

pub struct UserRow<'a> {
    pub name: Text<'a>,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top: Option<Text<'a>>,
}

impl<'a> UserRow<'a> {
    pub fn new(name: Text<'a>) -> Self {
        Self {
            name,
            day: 0,
            sessions: 0,
            events: 0,
            revenue: 0,
            top: None,
        }
    }
}

pub struct CampaignRow<'a> {
    pub id: Text<'a>,
    pub channel: Text<'a>,
    pub day: i32,
    pub totals: CampaignTotals,
}

/// A `ByteArray` that points at `text` instead of copying it (`ByteArray::from(&[u8])` makes two
/// allocations per value, which was most of the time spent writing names).
///
/// The value must be dropped before `text`'s mapping is: it is only ever collected into the batch
/// that `column` hands to `write_batch`, which copies the bytes into the page and returns.
fn borrowed(text: Text<'_>) -> ByteArray {
    // SAFETY: only the lifetime is changed; see above for why the value never outlives `text`.
    let text: &'static [u8] = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(text) };
    ByteArray::from(Bytes::from_static(text))
}

type Sink = BufWriter<File>;

fn open(path: &Path, schema: &str) -> Result<SerializedFileWriter<Sink>> {
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::None)
        .set_dictionary_enabled(false)
        .build();
    let file = BufWriter::with_capacity(256 * 1024, File::create(path)?);
    SerializedFileWriter::new(file, Arc::new(parse_message_type(schema)?), Arc::new(props))
}

fn column<T: DataType>(
    group: &mut SerializedRowGroupWriter<'_, Sink>,
    values: impl Iterator<Item = T::T>,
    definition: Option<&[i16]>,
) -> Result<()> {
    let values: Vec<_> = values.collect();
    let mut writer = group
        .next_column()?
        .expect("column described by output schema");
    writer.typed::<T>().write_batch(&values, definition, None)?;
    writer.close()?;
    Ok(())
}

pub fn users(path: &Path, rows: &[UserRow<'_>]) -> Result<()> {
    let mut file = open(path, USER_SCHEMA)?;
    for rows in rows.chunks(ROW_GROUP) {
        let mut group = file.next_row_group()?;
        column::<ByteArrayType>(
            &mut group,
            rows.iter().map(|row| borrowed(row.name)),
            None,
        )?;
        column::<Int32Type>(&mut group, rows.iter().map(|row| row.day), None)?;
        column::<Int64Type>(&mut group, rows.iter().map(|row| row.sessions), None)?;
        column::<Int64Type>(&mut group, rows.iter().map(|row| row.events), None)?;
        column::<Int64Type>(&mut group, rows.iter().map(|row| row.revenue), None)?;
        let levels: Vec<i16> = rows
            .iter()
            .map(|row| i16::from(row.top.is_some()))
            .collect();
        let categories = rows.iter().filter_map(|row| row.top.map(borrowed));
        column::<ByteArrayType>(&mut group, categories, Some(&levels))?;
        group.close()?;
    }
    file.close()?;
    Ok(())
}

pub fn campaigns(path: &Path, rows: &[CampaignRow<'_>]) -> Result<()> {
    let mut file = open(path, CAMPAIGN_SCHEMA)?;
    for rows in rows.chunks(ROW_GROUP) {
        let mut group = file.next_row_group()?;
        column::<ByteArrayType>(
            &mut group,
            rows.iter().map(|row| borrowed(row.id)),
            None,
        )?;
        column::<ByteArrayType>(
            &mut group,
            rows.iter().map(|row| borrowed(row.channel)),
            None,
        )?;
        column::<Int32Type>(&mut group, rows.iter().map(|row| row.day), None)?;
        column::<Int64Type>(&mut group, rows.iter().map(|row| row.totals.sessions), None)?;
        column::<Int64Type>(
            &mut group,
            rows.iter().map(|row| row.totals.purchasing),
            None,
        )?;
        column::<Int64Type>(&mut group, rows.iter().map(|row| row.totals.revenue), None)?;
        column::<Int64Type>(&mut group, rows.iter().map(|row| row.totals.assists), None)?;
        group.close()?;
    }
    file.close()?;
    Ok(())
}
