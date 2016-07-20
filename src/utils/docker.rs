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

use state::StateManager;
use rustc_serialize::json::Json;

pub fn handle_inspect_data(state_manager: &StateManager, task_name: &String, inspect_data: &String, slave_id: &String) {
    // println!("{}", inspect_data);

    let json = Json::from_str(&inspect_data).unwrap();

    let id = json.as_array().unwrap()[0].find_path(&["Id"]).unwrap().as_string().unwrap();
    let node_name = json.as_array().unwrap()[0].find_path(&["Config", "Hostname"]).unwrap().as_string().unwrap();
    let mut new_ip = match json.as_array().unwrap()[0].find_path(&["NetworkSettings", "Networks", "torc", "IPAddress"]) {
        None => "".to_string(),
        Some(ip) => ip.as_string().unwrap().to_string(),   
    };

    if new_ip.len() == 0 {
        match state_manager.request_node(node_name.to_string()) {
            Some(node) => new_ip = node.ip.clone(),
            None => {}
        }
    }

    state_manager.send_update_task_info(task_name.to_string(),
                                        id.to_string(),
                                        new_ip.clone(),
                                        slave_id.clone());
}