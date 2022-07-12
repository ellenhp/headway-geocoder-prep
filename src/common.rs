use std::{fs::File, io::BufReader};

use osmpbf::{Element, ElementReader};

pub(crate) fn get_reader() -> ElementReader<BufReader<File>> {
    ElementReader::from_path("planet.osm.pbf").expect("Need an OSM extract")
}

pub(crate) fn process_nodes_and_ways(element: Element) -> Option<String> {
    let tags = match element {
        Element::Node(node) => node.tags(),
        Element::DenseNode(_dense_node) => return None,
        Element::Way(way) => way.tags(),
        Element::Relation(_relation) => return None,
    };
    for (key, value) in tags {
        if key == "name" {
            return Some(value.to_string().to_lowercase());
        }
    }
    None
}

pub(crate) fn process_dense_nodes(element: Element) -> Option<String> {
    let tags = match element {
        Element::Node(_node) => return None,
        Element::DenseNode(dense_node) => dense_node.tags(),
        Element::Way(_way) => return None,
        Element::Relation(_relation) => return None,
    };
    for (key, value) in tags {
        if key == "name" {
            return Some(value.to_string().to_lowercase());
        }
    }
    None
}

pub(crate) fn process_relations(element: Element) -> Option<String> {
    let tags = match element {
        Element::Node(_node) => return None,
        Element::DenseNode(_dense_node) => return None,
        Element::Way(_way) => return None,
        Element::Relation(relation) => relation.tags(),
    };
    for (key, value) in tags {
        if key == "name" {
            return Some(value.to_string().to_lowercase());
        }
    }
    None
}
