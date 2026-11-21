use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::ptr::NonNull;

use memchr::{memchr, memchr_iter};

pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;
pub const GAP: i64 = 30 * 60_000;

pub struct Mapped {
    ptr: NonNull<u8>,
    len: usize,
}

// The mapping is read-only and never mutated, so sharing it across threads is sound.
unsafe impl Send for Mapped {}
unsafe impl Sync for Mapped {}

impl Mapped {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let len = usize::try_from(file.metadata()?.len()).map_err(io::Error::other)?;
        if len == 0 {
            return Ok(Self {
                ptr: NonNull::dangling(),
                len,
            });
        }
        if len > isize::MAX as usize {
            return Err(io::Error::other("file exceeds maximum slice size"));
        }
        // Input files are mounted read-only and stay unchanged until the map is dropped.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let ptr = NonNull::new(ptr.cast()).expect("mmap returned the null address");
        Ok(Self { ptr, len })
    }

    pub fn bytes(&self) -> &[u8] {
        // The mapping is live for the lifetime of the borrow.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    pub fn body(&self) -> &[u8] {
        let bytes = self.bytes();
        memchr(b'\n', bytes).map_or(&[], |header| &bytes[header + 1..])
    }
}

impl Drop for Mapped {
    fn drop(&mut self) {
        if self.len != 0 {
            unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
        }
    }
}

pub fn csv_files(folder: &Path) -> io::Result<Vec<Mapped>> {
    let mut paths = Vec::new();
    match folder.read_dir() {
        Ok(entries) => {
            for entry in entries {
                let path = entry?.path();
                if path
                    .file_name()
                    .is_some_and(|name| name.as_encoded_bytes().ends_with(b".csv"))
                    && path.is_file()
                {
                    paths.push(path);
                }
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    paths.sort_unstable();
    paths.iter().map(|path| Mapped::open(path)).collect()
}

pub fn chunk_batches(files: &[Mapped], workers: usize) -> Vec<Vec<&[u8]>> {
    let total: usize = files.iter().map(|file| file.bytes().len()).sum();
    let size = (total / (workers * 8)).clamp(64 * 1024, 4 * 1024 * 1024);
    let mut chunks = Vec::new();
    for file in files {
        let mut body = file.body();
        while body.len() > size {
            let Some(newline) = memchr(b'\n', &body[size..]) else {
                break;
            };
            let (chunk, suffix) = body.split_at(size + newline + 1);
            chunks.push(chunk);
            body = suffix;
        }
        if !body.is_empty() {
            chunks.push(body);
        }
    }

    chunks.sort_unstable_by_key(|chunk| std::cmp::Reverse(chunk.len()));
    let mut batches = vec![Vec::new(); workers];
    let mut sizes = vec![0; workers];
    for chunk in chunks {
        let worker = (0..workers).min_by_key(|&i| sizes[i]).unwrap();
        batches[worker].push(chunk);
        sizes[worker] += chunk.len();
    }
    batches
}

pub fn lines(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut start = 0;
    memchr_iter(b'\n', bytes)
        .chain(std::iter::once(bytes.len()))
        .scan((), move |_, end| {
            let line = bytes.get(start..end)?;
            start = end + 1;
            Some(line)
        })
}

#[inline]
fn field(value: &[u8]) -> Option<&[u8]> {
    let value = if let [b'"', inner @ .., b'"'] = value {
        inner
    } else {
        value
    };
    memchr(b'"', value).is_none().then_some(value)
}

#[inline]
pub fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut out = [&[][..]; N];
    let mut start = 0;
    let mut commas = memchr_iter(b',', line);
    for value in &mut out[..N - 1] {
        let end = commas.next()?;
        *value = field(&line[start..end])?;
        start = end + 1;
    }
    if commas.next().is_some() {
        return None;
    }
    out[N - 1] = field(&line[start..])?;
    Some(out)
}

#[inline]
fn two(a: u8, b: u8) -> Option<i64> {
    let (a, b) = (a.wrapping_sub(b'0'), b.wrapping_sub(b'0'));
    (a <= 9 && b <= 9).then_some(i64::from(a) * 10 + i64::from(b))
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    // Howard Hinnant's civil calendar formula, with the year starting in March.
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year - 719_468
}

#[inline]
pub fn timestamp(raw: &[u8]) -> Option<i64> {
    if let [
        a,
        b,
        c,
        d,
        b'-',
        e,
        f,
        b'-',
        g,
        h,
        b'T',
        i,
        j,
        b':',
        k,
        l,
        b':',
        m,
        n,
        zone @ ..,
    ] = raw
    {
        let year = two(*a, *b)? * 100 + two(*c, *d)?;
        let (month, day) = (two(*e, *f)?, two(*g, *h)?);
        let (hour, minute, second) = (two(*i, *j)?, two(*k, *l)?, two(*m, *n)?);
        if year == 0 || !(1..=12).contains(&month) || hour >= 24 || minute >= 60 || second >= 60 {
            return None;
        }
        let max_day = match month {
            2 => 28 + i64::from(year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)),
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        if day == 0 || day > max_day {
            return None;
        }
        let offset = match zone {
            [b'Z'] => 0,
            [sign @ (b'+' | b'-'), a, b, b':', c, d] => {
                let (hours, minutes) = (two(*a, *b)?, two(*c, *d)?);
                if hours >= 24 || minutes >= 60 {
                    return None;
                }
                let offset = hours * 60 + minutes;
                if *sign == b'+' { offset } else { -offset }
            }
            _ => return None,
        };
        return Some(
            days_from_civil(year, month, day) * DAY
                + ((hour * 60 + minute - offset) * 60 + second) * 1000,
        );
    }
    // Any F3 value beyond i64::MAX lies outside all possible civil windows.
    decimal(raw, i64::MAX as u64).map(|value| value as i64)
}

#[inline]
pub fn decimal(raw: &[u8], max: u64) -> Option<u64> {
    if raw.is_empty() {
        return None;
    }
    let mut value = 0u64;
    for &byte in raw {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 || value > max / 10 {
            return None;
        }
        value = value * 10 + u64::from(digit);
    }
    (value <= max).then_some(value)
}
