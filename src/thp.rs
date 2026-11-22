//! Optional transparent-huge-page backing for big buffers (`SS_THP=1`, Linux only).
//!
//! Allocations of 8 MiB or more are mapped directly, aligned to 2 MiB and marked `MADV_HUGEPAGE`,
//! so first touch costs one fault per 2 MiB instead of one per 4 KiB. Smaller ones, and everything
//! when the switch is off, go to the system allocator. The switch is set once, first thing in `main`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

pub static ON: AtomicBool = AtomicBool::new(false);
const HUGE: usize = 2 << 20;
const BIG: usize = 4 * HUGE;

pub struct Alloc;

#[inline]
fn big(layout: &Layout) -> bool {
    layout.size() >= BIG && layout.align() <= 4096 && ON.load(Relaxed)
}

unsafe impl GlobalAlloc for Alloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if !big(&layout) {
            return unsafe { System.alloc(layout) };
        }
        // Room to align the start up to 2 MiB, with 16 bytes before it for the mapping's address and length.
        let len = (layout.size() + 2 * HUGE) & !(HUGE - 1);
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
            libc::madvise(start as *mut _, base as usize + len - start, libc::MADV_HUGEPAGE);
            let header = start as *mut usize;
            header.sub(1).write(base as usize);
            header.sub(2).write(len);
            start as *mut u8
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if big(&layout) {
            // Fresh anonymous memory is already zero.
            unsafe { self.alloc(layout) }
        } else {
            unsafe { System.alloc_zeroed(layout) }
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if !big(&layout) {
            return unsafe { System.dealloc(ptr, layout) };
        }
        unsafe {
            let header = ptr as *const usize;
            libc::munmap(header.sub(1).read() as *mut _, header.sub(2).read());
        }
    }
}
