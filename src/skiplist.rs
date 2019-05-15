use rand::{rngs::ThreadRng, thread_rng, Rng};
use std::cmp::PartialOrd;
use std::fmt::Debug;
use std::ptr::{drop_in_place, null_mut};

struct SkipNode<K, V> {
    key: K,
    val: V,
    next: Vec<*mut SkipNode<K, V>>,
}

impl<K, V> SkipNode<K, V> {
    fn new(level: usize) -> Self {
        Self {
            key: unsafe { std::mem::uninitialized() },
            val: unsafe { std::mem::uninitialized() },
            next: vec![null_mut(); level],
        }
    }
    fn new_with_kv(key: K, val: V, level: usize) -> Self {
        Self {
            key,
            val,
            next: vec![null_mut(); level],
        }
    }
}

pub struct SkipList<K, V> {
    max_level: usize,
    head: *mut SkipNode<K, V>,
    rng: ThreadRng,
}

impl<K, V> SkipList<K, V>
where
    K: PartialOrd,
{
    pub fn new(max_level: usize) -> Self {
        assert!(max_level > 0);
        let head_box = Box::new(SkipNode::<K, V>::new(max_level));
        Self {
            max_level,
            head: Box::into_raw(head_box),
            rng: thread_rng(),
        }
    }

    fn random_level(&mut self) -> usize {
        let mut level = 0;
        while self.rng.gen::<bool>() && level + 1 < self.max_level {
            level += 1;
        }
        level + 1 // [1, max_level]
    }

    fn get_prevs(&self, key: &K) -> Vec<*mut SkipNode<K, V>> {
        let mut prevs = vec![];
        let mut prev = self.head;
        for l in (0..self.max_level).rev() {
            unsafe {
                let mut curr = (*prev).next[l];
                while !curr.is_null() && &(*curr).key < key {
                    prev = curr;
                    curr = (*curr).next[l];
                }
                prevs.push(prev);
            }
        }
        prevs.reverse();
        prevs
    }

    pub fn insert(&mut self, key: K, val: V) {
        let mut prevs = self.get_prevs(&key);
        unsafe {
            let node = (*prevs[0]).next[0];
            if !node.is_null() && (*node).key == key {
                (*node).val = val;
                return;
            }
            let node = Box::into_raw(Box::new(SkipNode::new_with_kv(
                key,
                val,
                self.random_level(),
            )));
            for l in 0..(*node).next.len() {
                (*node).next[l] = (*prevs[l]).next[l];
                (*prevs[l]).next[l] = node;
            }
        }
    }

    pub fn delete(&mut self, key: &K) {
        let mut prevs = self.get_prevs(&key);
        unsafe {
            let node = (*prevs[0]).next[0];
            if node.is_null() || &(*node).key != key {
                return;
            }
            for l in 0..(*node).next.len() {
                (*prevs[l]).next[l] = (*node).next[l];
            }
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        let mut prev = self.head;
        for l in (0..self.max_level).rev() {
            unsafe {
                let mut curr = (*prev).next[l];
                while !curr.is_null() && &(*curr).key < key {
                    prev = curr;
                    curr = (*curr).next[l];
                }
                if !curr.is_null() && &(*curr).key == key {
                    return Some(&(*curr).val);
                }
            }
        }
        None
    }
}

impl<K, V> Drop for SkipList<K, V> {
    fn drop(&mut self) {
        unsafe {
            let mut ptr = self.head;
            while !ptr.is_null() {
                let next_ptr = (*ptr).next[0];
                drop_in_place(ptr);
                ptr = next_ptr;
            }
        }
    }
}

impl<K, V> Debug for SkipList<K, V>
where
    K: Debug + PartialOrd,
    V: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for l in (0..self.max_level).rev() {
            unsafe {
                let mut curr = (*self.head).next[l];
                if curr.is_null() {
                    continue;
                }
                write!(f, "{}:", l)?;
                while !curr.is_null() {
                    write!(f, " {:?}", (*curr).key)?;
                    curr = (*curr).next[l];
                }
                write!(f, "\n")?;
            }
        }
        Ok(())
    }
}
