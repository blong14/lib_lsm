extern crate libc;

use crossbeam_skiplist::SkipMap;
use std::os::raw::{c_char, c_int, c_void};
use std::{ptr, slice, str};

// https://nnethercote.github.io/perf-book/build-configuration.html#jemalloc
// cbindgen:ignore
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

type StringSkipMap = SkipMap<String, Vec<u8>>;

#[no_mangle]
pub extern "C" fn skiplist_init() -> *mut c_void {
    let skip_map: StringSkipMap = SkipMap::new();
    Box::into_raw(Box::new(skip_map)) as *mut c_void
}

#[no_mangle]
pub extern "C" fn skiplist_free(skiplist: *mut c_void) {
    if !skiplist.is_null() {
        // Deallocate the memory for the skip map
        unsafe {
            let _ = Box::from_raw(skiplist as *mut StringSkipMap);
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

    let skip_map = unsafe { &mut *(skiplist as *mut StringSkipMap) };

    // SAFETY: The system ensures UTF-8 encoded keys, so unchecked conversion is safe.
    let key_str = unsafe {
        let key_bytes = slice::from_raw_parts(key as *const u8, key_len);
        str::from_utf8_unchecked(key_bytes)
    };

    let value_slice = unsafe { slice::from_raw_parts(value, value_len) };
    
    // Pre-allocate with exact capacity to avoid reallocations
    let mut value_vec = Vec::with_capacity(value_len);
    value_vec.extend_from_slice(value_slice);

    // Use insert instead of compare_insert for better performance when we know key doesn't exist
    skip_map.insert(key_str.to_string(), value_vec);
    0 // Success
}

#[no_mangle]
pub extern "C" fn skiplist_get(
    skiplist: *mut c_void,
    key: *const c_char,
    key_len: usize,
    value_ptr: *mut *const u8,
    value_len: *mut usize,
) -> c_int {
    if skiplist.is_null() || key.is_null() || value_ptr.is_null() || value_len.is_null() {
        return -1; // Error: null pointer
    }

    let key_str = unsafe {
        let key_bytes = slice::from_raw_parts(key as *const u8, key_len);
        str::from_utf8_unchecked(key_bytes)
    };

    let skip_map = unsafe { &*(skiplist as *const StringSkipMap) };

    if let Some(entry) = skip_map.get(key_str) {
        let entry_value = entry.value();

        unsafe {
            *value_ptr = entry_value.as_ptr();
            *value_len = entry_value.len();
        }

        0 // Success
    } else {
        -1 // Not found
    }
}

/// Opaque iterator for traversing the skip map
pub struct SkipMapIterator {
    skiplist: *const StringSkipMap,
    current_key: Option<String>,
    current_entry_key: Vec<u8>,
    current_entry_value: Vec<u8>,
}

#[no_mangle]
pub extern "C" fn skiplist_iterator_create(skiplist: *mut c_void) -> *mut SkipMapIterator {
    if skiplist.is_null() {
        return ptr::null_mut();
    }

    let iterator = Box::new(SkipMapIterator {
        skiplist: skiplist as *const StringSkipMap,
        current_key: None,
        current_entry_key: Vec::new(),
        current_entry_value: Vec::new(),
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

/// Entry containing key-value pair data
#[repr(C)]
pub struct SkipMapEntry {
    key_ptr: *const u8,
    key_len: usize,
    value_ptr: *const u8,
    value_len: usize,
}

#[no_mangle]
pub extern "C" fn skiplist_iterator_next(
    iter_ptr: *mut SkipMapIterator,
    entry_out: *mut SkipMapEntry,
) -> c_int {
    if iter_ptr.is_null() || entry_out.is_null() {
        return -1; // Error: null pointer
    }

    let iterator = unsafe { &mut *iter_ptr };
    let skip_map = unsafe { &*iterator.skiplist };

    // Get the next entry based on current position
    let next_entry = match &iterator.current_key {
        None => {
            // First iteration - get the first entry
            skip_map.front()
        }
        Some(current_key) => {
            // Get the entry after the current key
            skip_map
                .range::<str, _>((
                    std::ops::Bound::Excluded(current_key.as_str()),
                    std::ops::Bound::Unbounded,
                ))
                .next()
        }
    };

    match next_entry {
        Some(entry) => {
            let entry_key = entry.key();
            let entry_value = entry.value();

            // Store the key and value in the iterator to keep them alive
            iterator.current_entry_key = entry_key.as_bytes().to_vec();
            iterator.current_entry_value = entry_value.clone();

            // Update iterator position for next call
            iterator.current_key = Some(entry_key.clone());

            unsafe {
                (*entry_out).key_ptr = iterator.current_entry_key.as_ptr();
                (*entry_out).key_len = iterator.current_entry_key.len();
                (*entry_out).value_ptr = iterator.current_entry_value.as_ptr();
                (*entry_out).value_len = iterator.current_entry_value.len();
            }

            0 // Success
        }
        None => -1, // No more elements
    }
}
