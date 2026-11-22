//! Global allocator that backs big buffers with transparent huge pages (Linux).
//!
//! Faulting in the pipeline's multi-gigabyte buffers one 4 KiB page at a time was a large share of
//! its CPU time. Allocations of `BIG` bytes or more are mapped directly with `mmap`, aligned to
//! 2 MiB, and marked `MADV_HUGEPAGE`, so first touch costs one fault per 2 MiB. Everything smaller
//! goes to the system allocator.
//!
//! Which allocator owns a block is decided by its `Layout` alone, so `dealloc` and `realloc` always
//! agree with `alloc`. The only runtime switch (`SS_THP=0`) controls the `madvise` hint, nothing
//! else. If the kernel has huge pages disabled, the hint is ignored and the block is plain `mmap`
//! memory (which is what the system allocator hands out for blocks this big anyway). If `mmap`
//! fails the allocation fails with null, exactly as the system allocator would when out of memory.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};

const HUGE: usize = 2 << 20;
const PAGE: usize = 4096;
const BIG: usize = 4 * HUGE;

// experiment: threshold overridable with SS_BIG (bytes), read once in `init` before any big block exists
static THRESHOLD: AtomicUsize = AtomicUsize::new(BIG);

// Mid-size blocks (MID_MIN up to 8 MiB) come from huge-page regions and are recycled by size, so a
// 0.5 MB route shard costs neither a 2 MiB mapping of its own nor 4 KiB page faults. `SS_MID=<bytes>`
// moves the smallest size served this way (`SS_MID=99999999` turns the pool off).
const UNIT: usize = 16 << 10;
const MAX_UNITS: usize = 512;
const REGION: usize = 64 << 20;
static MID_MIN: AtomicUsize = AtomicUsize::new(64 << 10);
// Largest free block (in 1/8ths over the request) reused for a request with no free block of its own size.
static FIT_EIGHTHS: AtomicUsize = AtomicUsize::new(2);

struct Mid {
    /// Head of the free list for blocks of `n` units, 0 when empty; a free block's first word links to the next.
    free: [usize; MAX_UNITS + 1],
    cur: usize,
    end: usize,
}
static MID: Mutex<Mid> = Mutex::new(Mid { free: [0; MAX_UNITS + 1], cur: 0, end: 0 });

static ADVISE: AtomicBool = AtomicBool::new(true);

/// Call first thing in `main`: `SS_THP=0` turns the huge-page hint off.
pub fn init() {
    if std::env::var_os("SS_THP").is_some_and(|v| v == "0") {
        ADVISE.store(false, Relaxed);
    }
    if let Some(n) = std::env::var("SS_FIT").ok().and_then(|v| v.parse::<usize>().ok()) {
        FIT_EIGHTHS.store(n, Relaxed);
    }
    if let Some(n) = std::env::var("SS_MID").ok().and_then(|v| v.parse::<usize>().ok()) {
        MID_MIN.store(n, Relaxed);
    }
    if let Some(n) = std::env::var("SS_BIG").ok().and_then(|v| v.parse::<usize>().ok()) {
        THRESHOLD.store(n, Relaxed);
    }
}

pub struct Alloc;

#[inline]
fn is_big(layout: &Layout) -> bool {
    layout.size() >= THRESHOLD.load(Relaxed) && layout.align() <= PAGE
}

#[inline]
fn is_mid(layout: &Layout) -> bool {
    !is_big(layout)
        && layout.size() >= MID_MIN.load(Relaxed)
        && layout.size() <= MAX_UNITS * UNIT
        && layout.align() <= PAGE
}

/// A block of `size` bytes from the mid-size pool; zeroed when `zero`.
unsafe fn mid_alloc(size: usize, zero: bool) -> *mut u8 {
    let units = size.div_ceil(UNIT);
    let mut mid = MID.lock().unwrap_or_else(|e| e.into_inner());
    let mut class = units;
    // No block of this size: take the nearest bigger free one (within the allowed slack) rather
    // than touching fresh memory.
    let last = (units + units * FIT_EIGHTHS.load(Relaxed) / 8).min(MAX_UNITS);
    while mid.free[class] == 0 && class < last {
        class += 1;
    }
    let head = mid.free[class];
    if head != 0 {
        mid.free[class] = unsafe { *(head as *const usize) };
        drop(mid);
        if zero {
            unsafe { std::ptr::write_bytes(head as *mut u8, 0, size) };
        }
        return head as *mut u8;
    }
    let bytes = units * UNIT;
    if mid.end - mid.cur < bytes {
        // The rest of the old region (under 8 MiB, mostly never touched) is given up.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                REGION + HUGE,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return std::ptr::null_mut();
        }
        let start = (base as usize + HUGE - 1) & !(HUGE - 1);
        if ADVISE.load(Relaxed) {
            unsafe { libc::madvise(start as *mut _, REGION, libc::MADV_HUGEPAGE) };
        }
        mid.cur = start;
        mid.end = start + REGION;
    }
    let block = mid.cur;
    mid.cur += bytes;
    block as *mut u8 // fresh mapping: already zero
}

unsafe fn mid_free(ptr: *mut u8, size: usize) {
    let units = size.div_ceil(UNIT);
    let mut mid = MID.lock().unwrap_or_else(|e| e.into_inner());
    unsafe { *(ptr as *mut usize) = mid.free[units] };
    mid.free[units] = ptr as usize;
}

unsafe impl GlobalAlloc for Alloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if is_mid(&layout) {
            return unsafe { mid_alloc(layout.size(), false) };
        }
        if !is_big(&layout) {
            return unsafe { System.alloc(layout) };
        }
        // Room to align the start up to 2 MiB, and 16 bytes before it for the mapping's address
        // and length. `mmap` returns a page-aligned address, so the aligned start is at most
        // `HUGE` bytes past it and `size + 2 * HUGE` always covers the block.
        let len = (layout.size() + 2 * HUGE + PAGE - 1) & !(PAGE - 1);
        unsafe {
            let base = libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            );
            if base == libc::MAP_FAILED {
                return std::ptr::null_mut();
            }
            let start = (base as usize + 16 + HUGE - 1) & !(HUGE - 1);
            if ADVISE.load(Relaxed) {
                // A hint: failure (huge pages off, or not supported) changes nothing else.
                libc::madvise(start as *mut _, (layout.size() + HUGE - 1) & !(HUGE - 1), libc::MADV_HUGEPAGE);
            }
            let header = start as *mut usize;
            header.sub(1).write(base as usize);
            header.sub(2).write(len);
            start as *mut u8
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if is_mid(&layout) {
            unsafe { mid_alloc(layout.size(), true) }
        } else if is_big(&layout) {
            // A fresh anonymous mapping is already zero.
            unsafe { self.alloc(layout) }
        } else {
            unsafe { System.alloc_zeroed(layout) }
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if is_mid(&layout) {
            return unsafe { mid_free(ptr, layout.size()) };
        }
        if !is_big(&layout) {
            return unsafe { System.dealloc(ptr, layout) };
        }
        unsafe {
            let header = ptr as *const usize;
            libc::munmap(header.sub(1).read() as *mut _, header.sub(2).read());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn big_blocks_are_aligned_zeroed_writable_and_freed() {
        for size in [BIG, BIG + 1, 3 * BIG + 12_345] {
            let layout = Layout::from_size_align(size, 8).unwrap();
            unsafe {
                let p = Alloc.alloc_zeroed(layout);
                assert!(!p.is_null());
                assert_eq!(p as usize % HUGE, 0);
                assert!(std::slice::from_raw_parts(p, size).iter().all(|&b| b == 0));
                p.write(1);
                p.add(size - 1).write(2);
                Alloc.dealloc(p, layout);
            }
        }
    }

    #[test]
    fn small_and_page_aligned_blocks_use_the_system_allocator() {
        for (size, align) in [(1, 1), (BIG - 1, 8), (BIG, 2 * PAGE)] {
            let layout = Layout::from_size_align(size, align).unwrap();
            assert!(!is_big(&layout));
            unsafe {
                let p = Alloc.alloc(layout);
                assert!(!p.is_null());
                p.write(7);
                Alloc.dealloc(p, layout);
            }
        }
    }

    #[test]
    fn realloc_keeps_contents_across_every_size_class() {
        let mut layout = Layout::from_size_align(100, 8).unwrap();
        unsafe {
            let mut p = Alloc.alloc(layout);
            for i in 0..100 {
                p.add(i).write(i as u8);
            }
            // small -> big -> bigger -> small
            for new in [BIG + 5, 2 * BIG, 64] {
                let keep = layout.size().min(new);
                p = Alloc.realloc(p, layout, new);
                assert!(!p.is_null());
                layout = Layout::from_size_align(new, 8).unwrap();
                for i in 0..keep.min(100) {
                    assert_eq!(p.add(i).read(), i as u8);
                }
            }
            Alloc.dealloc(p, layout);
        }
    }

    #[test]
    fn mid_blocks_are_recycled_and_zeroed_on_request() {
        // Calls the pool directly: flipping MID_MIN would race with the other tests' allocations.
        let size = 3 * UNIT - 5;
        unsafe {
            let a = mid_alloc(size, true);
            assert!(!a.is_null());
            assert_eq!(a as usize % UNIT, 0);
            std::ptr::write_bytes(a, 0xAB, size);
            mid_free(a, size);
            let b = mid_alloc(size, true);
            assert!(std::slice::from_raw_parts(b, size).iter().all(|&x| x == 0));
            let c = mid_alloc(size, false);
            assert_ne!(b, c);
            mid_free(b, size);
            mid_free(c, size);
        }
    }

    #[test]
    fn works_with_the_hint_off() {
        ADVISE.store(false, Relaxed);
        let layout = Layout::from_size_align(BIG, 8).unwrap();
        unsafe {
            let p = Alloc.alloc(layout);
            assert!(!p.is_null());
            p.add(BIG - 1).write(1);
            Alloc.dealloc(p, layout);
        }
        ADVISE.store(true, Relaxed);
    }
}
