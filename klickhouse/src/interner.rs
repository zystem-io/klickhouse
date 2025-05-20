use crate::types::{maybe_string_hash, slice_hash};
use crate::MaybeString;
use hashbrown::HashTable;
use log::debug;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

pub trait Interner: Debug + Send + Sync {
    fn intern_slice(&self, slice: &[u8]) -> MaybeString;
    fn intern_owned(&self, owned: Vec<u8>) -> MaybeString;
    fn cleanup(&self) {}
}

#[derive(Debug)]
pub struct SimpleInterner {
    pub(crate) interned_strings: Arc<Mutex<HashTable<MaybeString>>>,
    n_strings: usize,
}

impl SimpleInterner {
    pub(crate) fn new(n_strings: usize) -> Self {
        Self {
            interned_strings: Arc::new(Mutex::new(HashTable::default())),
            n_strings,
        }
    }
}

impl Interner for SimpleInterner {
    fn intern_slice(&self, bytes: &[u8]) -> MaybeString {
        let key = slice_hash(bytes);
        let len = bytes.len();
        let eq = |val: &MaybeString| {
            let slice = val.as_ref();
            if slice.len() != len {
                return false;
            }
            slice == bytes
        };

        let mut strings = self.interned_strings.lock().unwrap();
        strings
            .entry(key, eq, maybe_string_hash)
            .or_insert_with(|| MaybeString::from(bytes.to_vec()))
            .get()
            .clone()
    }

    fn intern_owned(&self, owned: Vec<u8>) -> MaybeString {
        let key = slice_hash(&owned);
        let len = owned.len();
        let eq = |val: &MaybeString| {
            let slice = val.as_ref();
            if slice.len() != len {
                return false;
            }
            slice == owned
        };

        let mut strings = self.interned_strings.lock().unwrap();
        strings
            .entry(key, eq, maybe_string_hash)
            .or_insert_with(|| MaybeString::from(owned))
            .get()
            .clone()
    }

    fn cleanup(&self) {
        let mut strings = self.interned_strings.lock().unwrap();
        if strings.len() <= self.n_strings {
            return;
        }

        strings.retain(|s| matches!(s, MaybeString::String(_)));

        // todo: do something else
        if strings.len() > self.n_strings {
            debug!("Exceeded interned string limit, clearing all interned strings");
            strings.clear();
        }
    }
}
