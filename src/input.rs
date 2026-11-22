use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::ptr::NonNull;

use memchr::{memchr, memchr2_iter, memchr_iter};

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

/// Maps every `*.csv` in `folder`, sorted by path. Opening and mapping a file costs tens of
/// microseconds and a dataset has hundreds, so large folders are mapped by several threads.
pub fn csv_files(folder: &Path, workers: usize) -> io::Result<Vec<Mapped>> {
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
    if workers < 2 || paths.len() < 16 {
        return paths.iter().map(|path| Mapped::open(path)).collect();
    }
    let share = paths.len().div_ceil(workers);
    std::thread::scope(|scope| {
        let parts: Vec<_> = paths
            .chunks(share)
            .map(|part| scope.spawn(move || part.iter().map(|path| Mapped::open(path)).collect::<io::Result<Vec<_>>>()))
            .collect();
        let mut files = Vec::with_capacity(paths.len());
        for part in parts {
            files.extend(part.join().unwrap()?);
        }
        Ok(files)
    })
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
    if raw.len() <= 18 {
        // Eighteen digits cannot overflow u64, so one check at the end is enough.
        let mut value = 0u64;
        let mut bad = 0u8;
        for &byte in raw {
            let digit = byte.wrapping_sub(b'0');
            bad |= u8::from(digit > 9);
            value = value * 10 + u64::from(digit);
        }
        return (bad == 0 && value <= max).then_some(value);
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

/// Remembers the epoch milliseconds of recently seen `YYYY-MM-DD` prefixes, so the calendar
/// checks and day arithmetic run once per distinct date instead of once per timestamp.
pub struct Dates {
    slots: Vec<(u64, u16, i64)>,
}

impl Dates {
    pub fn new() -> Self {
        Self {
            slots: vec![(0, 0, i64::MIN); 256],
        }
    }
}

const BAD_DATE: i64 = i64::MIN + 1;

#[inline]
fn date_start(raw: &[u8; 10]) -> i64 {
    let [a, b, c, d, b'-', e, f, b'-', g, h] = *raw else {
        return BAD_DATE;
    };
    let (Some(hi), Some(lo), Some(month), Some(day)) =
        (two(a, b), two(c, d), two(e, f), two(g, h))
    else {
        return BAD_DATE;
    };
    let year = hi * 100 + lo;
    if year == 0 || !(1..=12).contains(&month) {
        return BAD_DATE;
    }
    let max_day = match month {
        2 => 28 + i64::from(year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)),
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if day == 0 || day > max_day {
        return BAD_DATE;
    }
    days_from_civil(year, month, day) * DAY
}

/// Same result as `timestamp`, using `dates` to skip repeated calendar work.
#[inline]
pub fn timestamp_cached(raw: &[u8], dates: &mut Dates) -> Option<i64> {
    if raw.len() >= 20 && raw[10] == b'T' {
        let prefix: &[u8; 10] = raw[..10].try_into().unwrap();
        let lead = u64::from_le_bytes(prefix[..8].try_into().unwrap());
        let tail = u16::from_le_bytes([prefix[8], prefix[9]]);
        let slot = ((lead ^ u64::from(tail).wrapping_mul(0x9E37_79B9_7F4A_7C15))
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            >> 56) as usize;
        let entry = &mut dates.slots[slot];
        let day = if entry.0 == lead && entry.1 == tail && entry.2 != i64::MIN {
            entry.2
        } else {
            let day = date_start(prefix);
            *entry = (lead, tail, day);
            day
        };
        if day == BAD_DATE {
            return None;
        }
        return time_of_day(&raw[11..], day);
    }
    timestamp(raw)
}

#[inline]
fn time_of_day(rest: &[u8], day: i64) -> Option<i64> {
    let [i, j, b':', k, l, b':', m, n, zone @ ..] = rest else {
        return None;
    };
    let (hour, minute, second) = (two(*i, *j)?, two(*k, *l)?, two(*m, *n)?);
    if hour >= 24 || minute >= 60 || second >= 60 {
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
    Some(day + ((hour * 60 + minute - offset) * 60 + second) * 1000)
}

/// Calls `f` with the N cleaned fields of every well-formed line, exactly as `fields::<N>` does
/// per `lines` item, but with a single scan for all separators.
#[inline]
pub fn rows<'a, const N: usize>(bytes: &'a [u8], f: impl FnMut([&'a [u8]; N])) {
    if bytes.len() > u32::MAX as usize - 1024 {
        return rows_scalar(bytes, f);
    }
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: NEON is part of the aarch64 baseline.
        return unsafe { rows_masked::<N, Neon, _>(bytes, f) };
    }
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("avx2")
        && std::is_x86_feature_detected!("bmi1")
        && std::is_x86_feature_detected!("popcnt")
    {
        // SAFETY: the CPU supports AVX2, BMI1 and POPCNT, checked just above.
        return unsafe { rows_avx2(bytes, f) };
    }
    #[cfg(not(target_arch = "aarch64"))]
    rows_scalar(bytes, f);
}

#[inline]
fn rows_scalar<'a, const N: usize>(bytes: &'a [u8], mut f: impl FnMut([&'a [u8]; N])) {
    let mut ends = [0usize; N];
    let mut count = 0usize;
    let mut line_start = 0usize;
    for p in memchr2_iter(b',', b'\n', bytes) {
        if bytes[p] == b',' {
            if count < N - 1 {
                ends[count] = p;
            }
            count += 1;
        } else {
            if count == N - 1 {
                ends[N - 1] = p;
                let quoted = memchr(b'"', &bytes[line_start..p]).is_some();
                emit(bytes, line_start, &ends, quoted, &mut f);
            }
            count = 0;
            line_start = p + 1;
        }
    }
    if count == N - 1 && line_start <= bytes.len() {
        ends[N - 1] = bytes.len();
        let quoted = memchr(b'"', &bytes[line_start..]).is_some();
        emit(bytes, line_start, &ends, quoted, &mut f);
    }
}

/// Comma, newline and quote bit masks (bit i = byte i) of 64 bytes.
trait Masker {
    /// # Safety
    /// `p` must be readable for 64 bytes and the CPU must support the implementation.
    unsafe fn masks(p: *const u8) -> [u64; 3];
}

#[cfg(target_arch = "aarch64")]
struct Neon;

#[cfg(target_arch = "aarch64")]
impl Masker for Neon {
    #[inline(always)]
    unsafe fn masks(p: *const u8) -> [u64; 3] {
        use std::arch::aarch64::*;
        unsafe {
            const BITS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
            let bits = vld1q_u8(BITS.as_ptr());
            let v = [vld1q_u8(p), vld1q_u8(p.add(16)), vld1q_u8(p.add(32)), vld1q_u8(p.add(48))];
            let mask = |byte: u8| {
                let c = vdupq_n_u8(byte);
                let t = |i: usize| vandq_u8(vceqq_u8(v[i], c), bits);
                // Three rounds of pairwise adds turn 64 weighted bytes into 8 mask bytes.
                let s = vpaddq_u8(vpaddq_u8(t(0), t(1)), vpaddq_u8(t(2), t(3)));
                vgetq_lane_u64::<0>(vreinterpretq_u64_u8(vpaddq_u8(s, s)))
            };
            [mask(b','), mask(b'\n'), mask(b'"')]
        }
    }
}

#[cfg(target_arch = "x86_64")]
struct Avx2;

#[cfg(target_arch = "x86_64")]
impl Masker for Avx2 {
    #[inline(always)]
    unsafe fn masks(p: *const u8) -> [u64; 3] {
        use std::arch::x86_64::*;
        unsafe {
            let lo = _mm256_loadu_si256(p.cast());
            let hi = _mm256_loadu_si256(p.add(32).cast());
            let mask = |byte: u8| {
                let c = _mm256_set1_epi8(byte as i8);
                let a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, c)) as u32 as u64;
                let b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, c)) as u32 as u64;
                a | b << 32
            };
            [mask(b','), mask(b'\n'), mask(b'"')]
        }
    }
}

/// Portable masks, used to test the splitter logic on any machine.
#[cfg(test)]
struct Plain;

#[cfg(test)]
impl Masker for Plain {
    unsafe fn masks(p: *const u8) -> [u64; 3] {
        let mut out = [0u64; 3];
        for i in 0..64 {
            let byte = unsafe { *p.add(i) };
            for (mask, c) in out.iter_mut().zip([b',', b'\n', b'"']) {
                *mask |= u64::from(byte == c) << i;
            }
        }
        out
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi1,bmi2,popcnt,lzcnt")]
unsafe fn rows_avx2<'a, const N: usize>(bytes: &'a [u8], f: impl FnMut([&'a [u8]; N])) {
    unsafe { rows_masked::<N, Avx2, _>(bytes, f) }
}

/// Writes the positions of the set bits of `m` (offset by `base`) to `out`, returns how many.
/// Writes at least 8 entries whatever the count, so `out` needs 16 entries of slack.
#[inline(always)]
unsafe fn flatten(mut m: u64, base: u32, out: *mut u32) -> usize {
    let count = m.count_ones() as usize;
    unsafe {
        for i in 0..8 {
            *out.add(i) = base + m.trailing_zeros();
            m &= m.wrapping_sub(1);
        }
        if count > 8 {
            for i in 8..16 {
                *out.add(i) = base + m.trailing_zeros();
                m &= m.wrapping_sub(1);
            }
            for i in 16..count {
                *out.add(i) = base + m.trailing_zeros();
                m &= m.wrapping_sub(1);
            }
        }
    }
    count
}

/// Bytes of input a segment aims for. A segment always ends after a newline.
const SEGMENT: usize = 16 * 1024;
const SLACK: usize = 24;

/// Same rows as `rows_scalar`. Each segment of whole lines is scanned once for comma, newline and
/// quote bit masks that are flattened into position lists; rows are then cut from the lists.
#[inline(always)]
unsafe fn rows_masked<'a, const N: usize, M: Masker, F: FnMut([&'a [u8]; N])>(bytes: &'a [u8], mut f: F) {
    let mut commas: Vec<u32> = Vec::new();
    let mut newlines: Vec<u32> = Vec::new();
    // Quotes seen in the segment before each newline.
    let mut quotes_before: Vec<u32> = Vec::new();
    let mut segment_start = 0usize;
    let mut line_start = 0usize;
    while segment_start < bytes.len() {
        let mut segment_end = bytes.len();
        if segment_end - segment_start > SEGMENT {
            segment_end = match memchr(b'\n', &bytes[segment_start + SEGMENT..]) {
                Some(newline) => segment_start + SEGMENT + newline + 1,
                None => bytes.len(),
            };
        }
        let room = segment_end - segment_start + SLACK;
        if commas.len() < room {
            commas.resize(room, 0);
            newlines.resize(room, 0);
            quotes_before.resize(room, 0);
        }
        let (mut comma_count, mut newline_count, mut quote_count) = (0usize, 0usize, 0u32);
        let mut offset = segment_start;
        while offset < segment_end {
            let block = if offset + 64 <= segment_end {
                unsafe { M::masks(bytes.as_ptr().add(offset)) }
            } else {
                // Zero padding holds no delimiter.
                let mut tail = [0u8; 64];
                tail[..segment_end - offset].copy_from_slice(&bytes[offset..segment_end]);
                unsafe { M::masks(tail.as_ptr()) }
            };
            let [comma_mask, newline_mask, quote_mask] = block;
            let base = offset as u32;
            unsafe {
                comma_count += flatten(comma_mask, base, commas.as_mut_ptr().add(comma_count));
                // Two newline entries are written whatever the count: most blocks hold 0 to 2.
                let n = newline_mask.count_ones() as usize;
                let mut m = newline_mask;
                for i in 0..n.max(2) {
                    let bit = m.trailing_zeros();
                    newlines[newline_count + i] = base + bit;
                    quotes_before[newline_count + i] =
                        quote_count + (quote_mask & (1u64 << (bit & 63)).wrapping_sub(1)).count_ones();
                    m &= m.wrapping_sub(1);
                }
                newline_count += n;
            }
            quote_count += quote_mask.count_ones();
            offset += 64;
        }
        if segment_end == bytes.len() && bytes[segment_end - 1] != b'\n' {
            // An unterminated last line ends at the end of the input.
            newlines[newline_count] = segment_end as u32;
            quotes_before[newline_count] = quote_count;
            newline_count += 1;
        }
        // Sentinels: a comma list that runs out never matches a newline.
        commas[comma_count..comma_count + 8].fill(u32::MAX);
        let (mut next_comma, mut previous_quotes) = (0usize, 0u32);
        for k in 0..newline_count {
            let newline = newlines[k];
            let quotes = quotes_before[k] - previous_quotes;
            previous_quotes = quotes_before[k];
            if commas[next_comma + N - 2] < newline && commas[next_comma + N - 1] > newline {
                let mut ends = [0usize; N];
                for (end, &comma) in ends.iter_mut().zip(&commas[next_comma..next_comma + N - 1]) {
                    *end = comma as usize;
                }
                ends[N - 1] = newline as usize;
                emit_counted(bytes, line_start, &ends, quotes, &mut f);
                next_comma += N - 1;
            } else {
                while commas[next_comma] < newline {
                    next_comma += 1;
                }
            }
            line_start = newline as usize + 1;
        }
        segment_start = segment_end;
    }
}

/// Like `emit`, but the caller already counted the quotes in the line. The line is well formed
/// exactly when every quote is the first or last byte of a field that has both.
/// `line_start <= ends[0] < ends[1] < ... < bytes.len()` is the caller's contract (the positions of
/// consecutive delimiters in `bytes`), which keeps every slice below in bounds.
#[inline(always)]
fn emit_counted<'a, const N: usize>(
    bytes: &'a [u8],
    line_start: usize,
    ends: &[usize; N],
    quotes: u32,
    f: &mut impl FnMut([&'a [u8]; N]),
) {
    debug_assert!(line_start <= ends[0] && ends.windows(2).all(|w| w[0] < w[1]) && ends[N - 1] <= bytes.len());
    debug_assert!(!bytes.is_empty());
    let mut out = [&[][..]; N];
    let mut start = line_start;
    let mut wrapped = 0u32;
    if cfg!(feature = "branchy") {
        // The previous version, kept to A/B the two on x86.
        for k in 0..N {
            let value = unsafe { bytes.get_unchecked(start..ends[k]) };
            start = ends[k] + 1;
            out[k] = if quotes != 0 {
                if let [b'"', inner @ .., b'"'] = value {
                    wrapped += 1;
                    inner
                } else {
                    value
                }
            } else {
                value
            };
        }
        if quotes == 2 * wrapped {
            f(out);
        }
        return;
    }
    for k in 0..N {
        let end = ends[k];
        // Branch-free: whether a field is wrapped in quotes is unpredictable (about a fifth of
        // fields are), so select the trimmed bounds arithmetically. Both bytes read are inside
        // `bytes` (clamped), and only count when the field has at least two bytes.
        let last_index = bytes.len() - 1;
        // SAFETY: the indexes are clamped into `bytes`.
        let (first, last) = unsafe {
            (
                *bytes.get_unchecked(start.min(last_index)),
                *bytes.get_unchecked(end.saturating_sub(1).min(last_index)),
            )
        };
        let wrap = usize::from(end - start >= 2 && first == b'"' && last == b'"');
        wrapped += wrap as u32;
        // SAFETY: see the contract above; with `wrap` the bounds move inward by one byte each.
        out[k] = unsafe { bytes.get_unchecked(start + wrap..end - wrap) };
        start = end + 1;
    }
    if quotes == 2 * wrapped {
        f(out);
    }
}

#[inline(always)]
fn emit<'a, const N: usize>(
    bytes: &'a [u8],
    line_start: usize,
    ends: &[usize; N],
    quoted: bool,
    f: &mut impl FnMut([&'a [u8]; N]),
) {
    let mut out = [&[][..]; N];
    let mut start = line_start;
    for k in 0..N {
        let value = &bytes[start..ends[k]];
        start = ends[k] + 1;
        out[k] = if quoted {
            match field(value) {
                Some(value) => value,
                None => return,
            }
        } else {
            value
        };
    }
    f(out);
}

/// Hints that a chunk of a mapped file will be read soon, so a cold page cache starts reading
/// ahead instead of faulting page by page. Best effort: a failed hint changes nothing.
#[inline]
pub fn will_need(chunk: &[u8]) {
    const PAGE: usize = 4096;
    let start = chunk.as_ptr().addr();
    let first = (start + PAGE - 1) & !(PAGE - 1);
    let last = (start + chunk.len()) & !(PAGE - 1);
    if last > first {
        // SAFETY: MADV_WILLNEED only advises; it never changes or unmaps memory.
        unsafe { libc::madvise(first as *mut libc::c_void, last - first, libc::MADV_WILLNEED) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect<const N: usize>(run: impl FnOnce(&mut dyn FnMut([&[u8]; N]))) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        run(&mut |row| out.push(row.iter().map(|field| field.to_vec()).collect()));
        out
    }

    fn check<const N: usize>(bytes: &[u8]) {
        let expected = collect::<N>(|f| rows_scalar::<N>(bytes, f));
        let plain = collect::<N>(|f| unsafe { rows_masked::<N, Plain, _>(bytes, f) });
        assert_eq!(plain, expected, "plain masks, input {:?}", String::from_utf8_lossy(bytes));
        let fast = collect::<N>(|f| rows::<N>(bytes, f));
        assert_eq!(fast, expected, "rows, input {:?}", String::from_utf8_lossy(bytes));
    }

    #[test]
    fn masked_rows_match_scalar_on_random_text() {
        let mut state = 0x1234_5678_9abc_def1u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        // Few distinct bytes, so lines often hold exactly N - 1 commas and many quotes.
        let alphabet = *b",,,\n\"a\"ab\r,";
        for round in 0..4000 {
            let len = (next() % if round % 50 == 0 { 70_000 } else { 300 }) as usize;
            let bytes: Vec<u8> = (0..len).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
            check::<2>(&bytes);
            check::<4>(&bytes);
            check::<7>(&bytes);
        }
    }

    #[test]
    fn masked_rows_handle_edges() {
        for text in [
            "", "\n", "a,b\n", "a,b", "a,b\n\n,\n\"a\",\"b\"", "\"\",\"\"\n\"\"\",\"x\n", "a,b,c,d,e,f,g",
            "a,b,c,d,e,f,g\n", "a,b,c,d,e,f,g,h\n1,2,3,4,5,6,7\n",
        ] {
            check::<2>(text.as_bytes());
            check::<7>(text.as_bytes());
        }
        // Well formed lines straddling every block and segment boundary.
        let line = "\"e12345\",u1,2026-10-01T00:00:00Z,2026-10-01T00:00:01Z,view,p1,\n";
        for pad in 0..70 {
            let text = format!("{}{}", "x,\n".repeat(pad), line.repeat(700));
            check::<7>(text.as_bytes());
            check::<7>(&text.as_bytes()[..text.len() - 1]);
        }
    }
}
