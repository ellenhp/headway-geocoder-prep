use std::{
    collections::HashSet,
    fs::{write, File},
    sync::mpsc::{self, Receiver, SyncSender},
    thread::spawn,
};

use fst::Map;
use highway::{HighwayHash, HighwayHasher};
use memmap::Mmap;
use xorf::Xor16;

use crate::common::{get_reader, process_dense_nodes, process_nodes_and_ways, process_relations};

fn process_name(name: String, vocab: &Map<Mmap>) -> Vec<u64> {
    let words: Vec<&str> = name.split_whitespace().into_iter().collect();
    let mut phrase = vec![];
    for word in words {
        if let Some(id) = vocab.get(word) {
            phrase.push(id);
        } else {
            panic!("Must have vocab for this!");
        }
    }
    phrase
}

fn get_phrase_list() -> Vec<Vec<u64>> {
    let reader = get_reader();
    let mmap = unsafe { Mmap::map(&File::open("tmp_vocab.fst").unwrap()).unwrap() };
    let map = Map::new(mmap).unwrap();
    let (sender, receiver): (SyncSender<Option<Vec<u64>>>, Receiver<Option<Vec<u64>>>) =
        mpsc::sync_channel(1024);
    let (list_sender, list_receiver) = mpsc::sync_channel(1024);
    let _handle = spawn(move || {
        let mut hashset = HashSet::new();
        loop {
            match receiver.recv().expect("recv failure") {
                Some(tokens) => {
                    hashset.insert(tokens);
                }
                None => {
                    let mut list: Vec<Vec<u64>> = hashset.into_iter().collect();
                    list.sort();
                    list_sender.send(list).unwrap();
                    break;
                }
            }
        }
    });
    reader
        .par_map_reduce(
            |element| {
                if let Some(name) = process_nodes_and_ways(element.clone()) {
                    sender
                        .clone()
                        .send(Some(process_name(name, &map)))
                        .expect("Send must succeed");
                }
                if let Some(name) = process_dense_nodes(element.clone()) {
                    sender
                        .clone()
                        .send(Some(process_name(name, &map)))
                        .expect("Send must succeed");
                }
                if let Some(name) = process_relations(element.clone()) {
                    sender
                        .clone()
                        .send(Some(process_name(name, &map)))
                        .expect("Send must succeed");
                }
            },
            || (),
            |_a, _b| (),
        )
        .expect("Need for_each to work!");
    sender.send(None).expect("need send to succeed");
    list_receiver.recv().expect("need list")
}

pub(crate) fn write_phrase_filter() {
    let list = get_phrase_list();
    let mut hash_set = HashSet::new();
    for seq in list {
        let seq_bytes: Vec<u8> = seq.iter().flat_map(|val| val.to_le_bytes()).collect();
        let hash = HighwayHasher::default().hash64(&seq_bytes);
        hash_set.insert(hash);
    }
    let hash_list: Vec<u64> = hash_set.into_iter().collect();
    write(
        "tmp_phrase.xor",
        &bincode::serialize(&Xor16::from(hash_list)).unwrap(),
    )
    .unwrap();
}
