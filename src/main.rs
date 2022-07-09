use std::{
    collections::HashSet,
    fs::File,
    io::{BufReader, BufWriter},
    sync::mpsc::{self, Receiver, SyncSender},
    thread::spawn,
};

use fst::SetBuilder;
use osmpbf::{Element, ElementReader};

const ZOOM: u32 = 10;
const TILES_PER_AXIS: u32 = u32::pow(2, ZOOM);

fn get_reader() -> ElementReader<BufReader<File>> {
    ElementReader::from_path("planet.osm.pbf").expect("Need an OSM extract")
}

fn get_tile(lat: f64, lon: f64) -> u32 {
    let lat_part = (((lat + 90f64) * TILES_PER_AXIS as f64) as u32) * TILES_PER_AXIS;
    let lon_part = ((lon + 180f64) * TILES_PER_AXIS as f64) as u32;
    lat_part + lon_part
}

fn process_nodes_and_ways(element: Element) -> Option<String> {
    let (tags, _tiles) = match element {
        Element::Node(node) => (node.tags(), vec![get_tile(node.lat(), node.lon())]),
        Element::DenseNode(_dense_node) => return None,
        Element::Way(way) => (
            way.tags(),
            way.node_locations()
                .map(|location| get_tile(location.lat(), location.lon()))
                .collect(),
        ),
        Element::Relation(_relation) => return None,
    };
    for (key, value) in tags {
        if key == "name" {
            return Some(value.to_string());
        }
    }
    None
}

fn process_dense_nodes(element: Element) -> Option<String> {
    let (tags, _tiles) = match element {
        Element::Node(_node) => return None,
        Element::DenseNode(dense_node) => (
            dense_node.tags(),
            vec![get_tile(dense_node.lat(), dense_node.lon())],
        ),
        Element::Way(_way) => return None,
        Element::Relation(_relation) => return None,
    };
    for (key, value) in tags {
        if key == "name" {
            return Some(value.to_string());
        }
    }
    None
}

fn process_relations(element: Element) -> Option<String> {
    let tags = match element {
        Element::Node(_node) => return None,
        Element::DenseNode(_dense_node) => return None,
        Element::Way(_way) => return None,
        Element::Relation(relation) => relation.tags(),
    };
    for (key, value) in tags {
        if key == "name" {
            return Some(value.to_string());
        }
    }
    None
}

fn get_list() -> Vec<String> {
    let reader = get_reader();
    let (sender, receiver): (SyncSender<Option<String>>, Receiver<Option<String>>) =
        mpsc::sync_channel(1024);
    let (list_sender, list_receiver) = mpsc::sync_channel(1024);
    let handle = spawn(move || {
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

fn main() {
    let mut builder = SetBuilder::new(BufWriter::new(
        File::create("set-planet.fst").expect("Need file"),
    ))
    .expect("Need SetBuilder");
    for name in get_list() {
        builder.insert(name).expect("need insert to succeed");
    }
    builder.finish().expect("need finish to succeed");
}
