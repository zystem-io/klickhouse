use crate::types::{maybe_string_hash, slice_hash};
use crate::MaybeString;
use hashbrown::HashTable;
use log::debug;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

pub trait Interner: Debug + Send + Sync {
    fn intern_slice(&self, slice: &[u8]) -> MaybeString;
    fn cleanup(&self) {}
}

#[derive(Debug)]
pub struct SimpleInterner {
    pub(crate) interned_strings: Arc<RwLock<HashTable<MaybeString>>>,
    n_strings: usize,
}

impl SimpleInterner {
    pub(crate) fn new(n_strings: usize) -> Self {
        Self {
            interned_strings: Arc::new(RwLock::new(HashTable::default())),
            n_strings,
        }
    }
}

impl Interner for SimpleInterner {
    fn intern_slice(&self, bytes: &[u8]) -> MaybeString {
        let key = slice_hash(bytes);
        let eq = |val: &MaybeString| val.as_ref() == bytes;

        let strings = self.interned_strings.read().unwrap();
        if let Some(maybe_string) = strings.find(key, eq) {
            return maybe_string.clone();
        }
        drop(strings);

        let mut strings = self.interned_strings.write().unwrap();
        strings
            .entry(key, eq, maybe_string_hash)
            .or_insert_with(|| MaybeString::from(bytes.to_vec()))
            .get()
            .clone()
    }

    fn cleanup(&self) {
        let strings = self.interned_strings.read().unwrap();
        if strings.len() <= self.n_strings {
            return;
        }
        drop(strings);

        let mut strings = self.interned_strings.write().unwrap();
        strings.retain(|s| matches!(s, MaybeString::String(_)));

        // todo: do something else
        if strings.len() > self.n_strings {
            debug!("Exceeded interned string limit, clearing all interned strings");
            strings.clear();
        }
    }
}
