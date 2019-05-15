mod skiplist;

fn main() {
    use rand::Rng;
    use skiplist::SkipList;

    let mut rng = rand::thread_rng();
    let mut l = SkipList::<u32, u32>::new(4);
    l.insert(50, 50);
    for _ in 0..20 {
        let k = rng.gen::<u32>() % 100;
        l.insert(k, k);
    }
    println!("{:?}", l);
    println!("{:?}", l.get(&50));
    l.delete(&50);
    println!("{:?}", l.get(&50));
    println!("{:?}", l);
}
