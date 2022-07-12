use std::{
    collections::HashSet,
    fs::File,
    io::BufWriter,
    sync::mpsc::{self, Receiver, SyncSender},
    thread::spawn,
};

use fst::MapBuilder;

use crate::common::{get_reader, process_dense_nodes, process_nodes_and_ways, process_relations};

fn get_word_list() -> Vec<String> {
    let reader = get_reader();
    let (sender, receiver): (SyncSender<Option<String>>, Receiver<Option<String>>) =
        mpsc::sync_channel(1024);
    let (list_sender, list_receiver) = mpsc::sync_channel(1024);
    let _handle = spawn(move || {
        let mut hashset = HashSet::new();
        loop {
            match receiver.recv().expect("recv failure") {
                Some(name) => {
                    for str in name.split_whitespace() {
                        hashset.insert(str.to_lowercase());
                    }
                }
                None => {
                    let mut list: Vec<String> = hashset.into_iter().collect();
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
                    sender.clone().send(Some(name)).expect("Send must succeed");
                }
                if let Some(name) = process_dense_nodes(element.clone()) {
                    sender.clone().send(Some(name)).expect("Send must succeed");
                }
                if let Some(name) = process_relations(element.clone()) {
                    sender.clone().send(Some(name)).expect("Send must succeed");
                }
            },
            || (),
            |_a, _b| (),
        )
        .expect("Need for_each to work!");
    sender.send(None).expect("need send to succeed");
    list_receiver.recv().expect("need list")
}

pub(crate) fn write_vocab_fst() {
    let mut builder = MapBuilder::new(BufWriter::new(
        File::create("tmp_vocab.fst").expect("Need file"),
    ))
    .expect("Need SetBuilder");
    let list = get_word_list();
    for (index, name) in list.iter().enumerate() {
        builder
            .insert(name, (index + 1) as u64)
            .expect("need insert to succeed");
    }
    builder.finish().expect("need finish to succeed");
}
