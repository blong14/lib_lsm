extern crate libc;

use crossbeam_skiplist::{map::Entry, SkipMap};
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use std::{ptr, slice, str};

// https://nnethercote.github.io/perf-book/build-configuration.html#jemalloc
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[no_mangle]
pub extern "C" fn skiplist_init() -> *mut c_void {
    let skip_map: SkipMap<String, Vec<u8>> = SkipMap::new();
    Box::into_raw(Box::new(skip_map)) as *mut c_void
}

#[no_mangle]
pub extern "C" fn skiplist_free(skiplist: *mut c_void) {
    if !skiplist.is_null() {
        // Deallocate the memory for the skip map
        unsafe {
            let _ = Box::from_raw(skiplist as *mut SkipMap<String, Vec<u8>>);
        }
    }
}

#[no_mangle]
pub extern "C" fn skiplist_insert(
    skiplist: *mut c_void,
    key: *const c_char,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> c_int {
    if skiplist.is_null() || key.is_null() || value.is_null() {
        return -1; // Null pointer check
    }

    let skip_map = unsafe { &mut *(skiplist as *mut SkipMap<String, Vec<u8>>) };

    // SAFETY: The system ensures UTF-8 encoded keys, so unchecked conversion is safe.
    let key_str = unsafe {
        let key_bytes = slice::from_raw_parts(key as *const u8, key_len);
        str::from_utf8_unchecked(key_bytes)
    };

    let value_slice = unsafe { slice::from_raw_parts(value, value_len) };
    let value_vec = value_slice.to_vec();

    // Only insert if the key does not exist
    skip_map.compare_insert(key_str.to_string(), value_vec, |_| true);
    0 // Success
}

#[no_mangle]
pub extern "C" fn skiplist_get(
    skiplist: *mut c_void,
    key: *const c_char,
    key_len: usize,
    value: *mut u8,
    value_len: *mut usize,
) -> c_int {
    if skiplist.is_null() || key.is_null() || value.is_null() || value_len.is_null() {
        return -1; // Error: null pointer
    }

    let key_str = unsafe {
        let key_bytes = slice::from_raw_parts(key as *const u8, key_len);
        str::from_utf8_unchecked(key_bytes)
    };

    let skip_map = unsafe { &mut *(skiplist as *mut SkipMap<String, Vec<u8>>) };
    if let Some(entry) = skip_map.get(key_str) {
        let entry_value = entry.value();
        let len = entry_value.len();

        unsafe {
            if *value_len < len {
                return -2; // Error: buffer too small
            }
            ptr::copy_nonoverlapping(entry_value.as_ptr(), value, len);
            *value_len = len;
        }
        0 // Success
    } else {
        -1 // Not found
    }
}

#[no_mangle]
pub extern "C" fn skiplist_remove(skiplist: *mut c_void, key: *const c_char) -> c_int {
    if skiplist.is_null() || key.is_null() {
        return -1; // Error: null pointer
    }

    // SAFETY: The key is ensured to be a valid UTF-8 null-terminated string.
    let c_str = unsafe { CStr::from_ptr(key) };
    let key_str = match c_str.to_str() {
        Ok(s) => s,
        Err(_) => return -1, // Error: invalid UTF-8
    };

    let skip_map = unsafe { &mut *(skiplist as *mut SkipMap<String, Vec<u8>>) };
    match skip_map.remove(key_str) {
        Some(_) => 0, // Success
        None => -1,   // Not found
    }
}

#[no_mangle]
pub extern "C" fn skiplist_clear(skiplist: *mut c_void) {
    if skiplist.is_null() {
        return;
    }

    let skip_map = unsafe { &mut *(skiplist as *mut SkipMap<String, Vec<u8>>) };
    skip_map.clear();
}

#[no_mangle]
pub extern "C" fn skiplist_size(skiplist: *mut c_void) -> c_int {
    if skiplist.is_null() {
        return -1; // Error: null pointer
    }

    let skip_map = unsafe { &mut *(skiplist as *mut SkipMap<String, Vec<u8>>) };
    skip_map.len() as c_int
}

#[repr(C)]
pub struct SkipMapIterator {
    inner: Box<dyn Iterator<Item = Entry<'static, String, Vec<u8>>>>,
}

#[no_mangle]
pub extern "C" fn skiplist_iterator_create(skiplist: *mut c_void) -> *mut SkipMapIterator {
    if skiplist.is_null() {
        return ptr::null_mut();
    }

    let skip_map = unsafe { &mut *(skiplist as *mut SkipMap<String, Vec<u8>>) };

    // Create an iterator and wrap it in a Box
    let iterator = Box::new(SkipMapIterator {
        inner: Box::new(
            skip_map
                .iter()
                .map(|entry| unsafe { std::mem::transmute(entry) }),
        ),
    });

    Box::into_raw(iterator)
}

#[no_mangle]
pub extern "C" fn skiplist_iterator_free(iter_ptr: *mut SkipMapIterator) {
    if !iter_ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(iter_ptr);
        };
    }
}

#[repr(C)]
pub struct SkipMapEntry {
    key_ptr: *const u8,
    key_len: usize,
    value_ptr: *const u8,
    value_len: usize,
}

#[no_mangle]
pub extern "C" fn skiplist_iterator_xnext(
    iter_ptr: *mut SkipMapIterator,
    entry_out: *mut SkipMapEntry,
) -> c_int {
    if iter_ptr.is_null() || entry_out.is_null() {
        return -1; // Error: null pointer
    }

    let iterator = unsafe { &mut *iter_ptr };

    match iterator.inner.next() {
        Some(entry) => {
            let entry_key = entry.key();
            let entry_value = entry.value();

            let key_bytes = entry_key.as_bytes();
            let value_bytes = entry_value;

            unsafe {
                (*entry_out).key_ptr = key_bytes.as_ptr();
                (*entry_out).key_len = key_bytes.len();
                (*entry_out).value_ptr = value_bytes.as_ptr();
                (*entry_out).value_len = value_bytes.len();
            }

            0 // Success
        }
        None => -1, // No more elements
    }
}

#[no_mangle]
pub extern "C" fn skiplist_iterator_next(
    iter_ptr: *mut SkipMapIterator,
    key: *mut c_char,
    key_len: *mut usize,
    value: *mut u8,
    value_len: *mut usize,
) -> c_int {
    if iter_ptr.is_null()
        || key.is_null()
        || key_len.is_null()
        || value.is_null()
        || value_len.is_null()
    {
        return -1; // Error: null pointer
    }

    let iterator = unsafe { &mut *iter_ptr };

    match iterator.inner.next() {
        Some(entry) => {
            let entry_key = entry.key();
            let entry_value = entry.value();

            let key_bytes = entry_key.as_bytes();
            let value_bytes = entry_value;

            unsafe {
                if *key_len < key_bytes.len() || *value_len < value_bytes.len() {
                    return -2; // Error: buffer too small
                }

                ptr::copy_nonoverlapping(key_bytes.as_ptr(), key as *mut u8, key_bytes.len());
                *key_len = key_bytes.len();

                ptr::copy_nonoverlapping(value_bytes.as_ptr(), value, value_bytes.len());
                *value_len = value_bytes.len();
            }

            0 // Success
        }
        None => -1, // No more elements
    }
}
