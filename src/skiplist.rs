use rand::{rngs::ThreadRng, thread_rng, Rng};
use std::cmp::PartialOrd;

struct SkipNode<K, V> {
    key: K,
    val: V,
    next: Vec<*mut SkipNode<K, V>>,
}

impl<K, V> SkipNode<K, V> {
    fn new() -> Self {
        Self {
            key: unsafe { std::mem::uninitialized() },
            val: unsafe { std::mem::uninitialized() },
            next: vec![],
        }
    }
    fn new_with_kv(key: K, val: V) -> Self {
        Self {
            key,
            val,
            next: vec![],
        }
    }
}

struct SkipList<K, V> {
    max_level: usize,
    head: SkipNode<K, V>,
    rng: ThreadRng,
}

impl<K, V> SkipList<K, V>
where
    K: PartialOrd,
{
    fn new(max_level: usize) -> Self {
        assert!(max_level > 0);
        let mut head = SkipNode::new();
        let ptr: *mut SkipNode<K, V> = unsafe { std::mem::transmute(&head) };
        head.next.extend(vec![ptr; max_level]);
        Self {
            max_level,
            head,
            rng: thread_rng(),
        }
    }

    #[inline(always)]
    unsafe fn tail(&self) -> *mut SkipNode<K, V> {
        std::mem::transmute(&self.head)
    }

    fn random_level(&mut self) -> usize {
        let mut level = 0;
        while self.rng.gen::<bool>() && level + 1 < self.max_level {
            level += 1;
        }
        level + 1 // [1, max_level]
    }

    fn insert(&mut self, key: K, val: V) {
        let mut prev: Vec<*mut SkipNode<K, V>> = vec![];
        let mut curr: &SkipNode<K, V> = &self.head;
        for l in (0..self.max_level).rev() {
            unsafe {
                while curr.next[l] != self.tail() && (*curr.next[l]).key < key {
                    curr = &*curr.next[l];
                }
                prev.push(std::mem::transmute(curr));
            }
        }
        prev.reverse();
        let mut node = SkipNode::new_with_kv(key, val);
        for l in 0..self.random_level() {
            unsafe {
                node.next.push((*prev[l]).next[l]);
                (*prev[l]).next[l] = std::mem::transmute(&node);
            }
        }
        std::mem::forget(node);
    }
}

impl<K, V> Drop for SkipList<K, V> {
    fn drop(&mut self) {
        unsafe {
            let curr_ptr = std::mem::transmute(&mut self.head);
            let mut ptr = self.head.next[0];
            while ptr != curr_ptr {
                let next_ptr = (*ptr).next[0];
                std::ptr::drop_in_place(ptr);
                ptr = next_ptr;
            }
        }
    }
}
