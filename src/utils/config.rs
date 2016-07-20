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

use yaml_rust::yaml::Yaml;
use state::{SLA, Volume};

#[derive(Clone, Debug)]
pub struct Task {
    pub name: String,
    pub image: String,
    pub node_name: String,
    pub node_type: String,
    pub node_function: String,
    pub number_of_instances: i64,
    pub dependent_service: String,
    pub arguments: String,
    pub parameters: String,
    pub memory: f64,
    pub cpu: f64,
    pub volumes: Vec<Volume>,
    pub privileged: bool,
    pub sla: SLA,
    pub is_metered: bool,
    pub is_job: bool,
    pub network_type: String,
}

pub fn read_task(service: &Yaml) -> Task {
    let new_task = Task {
        name: service["name"].as_str().unwrap().to_string(),
        image: service["image_name"].as_str().unwrap().to_string(),
        node_name: read_string(service, "node_name".to_string()),
        node_type: read_string(service, "node_type".to_string()),
        node_function: read_string(service, "node_function".to_string()),
        number_of_instances: read_int(service, "number_of_instances".to_string(), 1),
        dependent_service: read_string(service, "dependent_service".to_string()),
        arguments: read_string(service, "arguments".to_string()),
        parameters: read_string(service, "parameters".to_string()),
        memory: read_float(service, "memory".to_string(), super::DEFAULT_MEMORY),
        cpu: read_float(service, "cpu".to_string(), super::DEFAULT_CPU),
        volumes: read_volumes_for_service(service),
        privileged: read_bool(service, "privileged".to_string()),
        sla: read_sla(service),
        is_metered: read_bool(service, "is_metered".to_string()),
        is_job: read_bool(service, "is_job".to_string()),
        network_type: service["network_type"].as_str().unwrap().to_string(),
    };
    new_task.clone()
}

pub fn read_string(element: &Yaml, key: String) -> String {
    match element[key.as_ref()].is_badvalue() {
        true => "".to_string(),
        false => element[key.as_ref()].as_str().unwrap().to_string(),
    }
}

pub fn read_bool(element: &Yaml, key: String) -> bool {
    match element[key.as_ref()].is_badvalue() {
        true => false,
        false => element[key.as_ref()].as_bool().unwrap(),
    }
}

pub fn read_float(element: &Yaml, key: String, default: f64) -> f64 {
    match element[key.as_ref()].is_badvalue() {
        true => default,
        false => element[key.as_ref()].as_f64().unwrap(),
    }
}

pub fn read_int(element: &Yaml, key: String, default: i64) -> i64 {
    match element[key.as_ref()].is_badvalue() {
        true => default,
        false => element[key.as_ref()].as_i64().unwrap(),
    }
}

fn read_volumes_for_service(service: &Yaml) -> Vec<Volume> {
    let mut result = Vec::new();

    match service["volumes"].is_badvalue() {
        true => {}
        false => {
            let volumes = service["volumes"].as_vec().unwrap();
            for volume in volumes {
                let definition = Volume {
                    host_path: volume["host_path"].as_str().unwrap().to_string(),
                    container_path: volume["container_path"].as_str().unwrap().to_string(),
                    read_only_mode: volume["read_only_mode"].as_bool().unwrap(),
                };
                result.push(definition);
            }
        }
    }

    result.clone()
}

fn read_sla(service: &Yaml) -> SLA {
    let sla: SLA;
    sla = match service["sla"].is_badvalue() {
        true => SLA::None,
        false => {
            match &service["sla"].as_str().unwrap().to_string() as &str {
                "singleton_each_node" => SLA::SingletonEachNode,
                "singleton_each_slave" => SLA::SingletonEachSlave,
                _ => SLA::None,
            }
        }
    };

    sla.clone()
}