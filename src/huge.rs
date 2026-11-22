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
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

const HUGE: usize = 2 << 20;
const PAGE: usize = 4096;
const BIG: usize = 4 * HUGE;

static ADVISE: AtomicBool = AtomicBool::new(true);

/// Call first thing in `main`: `SS_THP=0` turns the huge-page hint off.
pub fn init() {
    if std::env::var_os("SS_THP").is_some_and(|v| v == "0") {
        ADVISE.store(false, Relaxed);
    }
}

pub struct Alloc;

#[inline]
fn is_big(layout: &Layout) -> bool {
    layout.size() >= BIG && layout.align() <= PAGE
}

unsafe impl GlobalAlloc for Alloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
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
                libc::madvise(start as *mut _, layout.size(), libc::MADV_HUGEPAGE);
            }
            let header = start as *mut usize;
            header.sub(1).write(base as usize);
            header.sub(2).write(len);
            start as *mut u8
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if is_big(&layout) {
            // A fresh anonymous mapping is already zero.
            unsafe { self.alloc(layout) }
        } else {
            unsafe { System.alloc_zeroed(layout) }
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
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
