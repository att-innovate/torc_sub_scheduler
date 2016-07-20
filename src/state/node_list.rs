// The MIT License (MIT)
//
// Copyright (c) 2016 AT&T
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

use std::collections::HashMap;
use std::sync::Mutex;

pub struct NodeList {
    node_list: Mutex<HashMap<String, Node>>,
}

#[derive(Clone, Debug, RustcEncodable)]
pub struct Node {
    pub name: String,
    pub ip: String,
    pub external_ip: String,
    pub management_ip: String,
    pub node_type: String,
    pub node_function: String,
    pub active: bool,
    pub slave_id: String,
    pub port_id: i64,
    pub reachable: bool,
}

impl NodeList {
    pub fn new() -> NodeList {
        NodeList { node_list: Mutex::new(HashMap::new()) }
    }

    pub fn add_new_node(&self, node: &Node) {
        println!("insert new node {}", node.name);
        self.node_list.lock().unwrap().insert(node.name.to_string(), node.clone());
    }

    pub fn is_node_active(&self, node_name: String) -> bool {
        match self.node_list.lock().unwrap().get(&node_name) {
            Some(node) => node.active,
            None => false,
        }
    }

    pub fn update_node(&self, node_name: String, node_type: String, node_function: String, slave_id: String) {
        let exists;

        println!("upate node {}", node_name);

        match self.node_list.lock().unwrap().get_mut(&node_name) {
            Some(node) => {
                node.node_type = node_type.clone();
                node.node_function = node_function.clone();
                node.slave_id = slave_id.clone();
                node.active = true;
                exists = true
            }
            None => exists = false,
        }

        if exists == false {
            println!("no node entry found for {}", node_name);
        }
    }

    pub fn get_node(&self, node_name: String) -> Result<Node, &'static str> {
        match self.node_list.lock().unwrap().get(&node_name) {
            Some(node) => Ok(node.clone()),
            None => Err("Can't find node"),
        }
    }

    pub fn get_nodes(&self) -> Vec<Node> {
        let mut result: Vec<Node> = vec![];

        let map = self.node_list.lock().unwrap();
        for value in map.values().into_iter() {
            result.push(value.clone());
        }

        result
    }
}
